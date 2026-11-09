"""Bounded, read-only analysis of one saved Airflow snapshot for AI tool calls."""
import hashlib
import json
import re
import sqlite3
import time
from collections import defaultdict
from datetime import date,datetime

from lineage_app.data.model import CONTRACT,trace
from lineage_app.data.dag_tags import dag_tag_graph

SECRET_PATTERNS=[re.compile(r'\bsk-[A-Za-z0-9_\-]{12,}'),re.compile(r'(?i)\bBearer\s+[A-Za-z0-9_.\-]+'),
    re.compile(r'\beyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+')]

def clean(value):
    if isinstance(value,dict):
        return {str(k):('[redacted]' if re.search(r'(?i)(password|secret|access_token|api_key|authorization)',str(k)) else clean(v)) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [clean(v) for v in value]
    if isinstance(value,(datetime,date)):return value.isoformat()
    if isinstance(value,str):
        for pattern in SECRET_PATTERNS:value=pattern.sub('[redacted]',value)
        if value.lstrip().startswith(('{','[')):
            try:value=json.dumps(clean(json.loads(value)),ensure_ascii=False)
            except (ValueError,TypeError,RecursionError):pass
    return value

def snapshot_id(meta):
    return hashlib.sha256(json.dumps([meta.get(k) for k in ('stage','base_url','snapshot_date','collected_at')]).encode()).hexdigest()

def identifier(name):return '"'+name.replace('"','""')+'"'

FUNCTIONS=set('count sum total avg min max coalesce ifnull nullif round abs lower upper length substr substring trim ltrim rtrim replace instr date time datetime strftime julianday unixepoch json_extract json_array_length json_type json_valid json json_array json_object json_group_array json_group_object group_concat printf format iif like glob typeof cast row_number rank dense_rank lag lead first_value last_value nth_value ntile cume_dist percent_rank sqrt pow mod'.split())

class SnapshotData:
    def __init__(self,store,expected=None):
        # Authorizer evidence must be collected on every execution, including
        # repeated questions with identical SQL.
        self.db=sqlite3.connect(':memory:',cached_statements=0);self.db.row_factory=sqlite3.Row
        try:self._load(store,expected)
        except BaseException:self.db.close();raise

    def _load(self,store,expected):
        self.records={};self.schemas={};self.evidence=[]
        with store.lock:
            if store.incomplete():raise ValueError('The saved snapshot is incomplete. Reconnect before asking AI.')
            self.meta=store.metadata()
            if not self.meta:raise ValueError('Load a saved snapshot first.')
            self.identity=snapshot_id(self.meta)
            if expected and expected!=self.identity:raise ValueError('This conversation uses an earlier snapshot. Start a new chat for current data.')
            self.day=date.fromisoformat(self.meta['snapshot_date'])
            for table in store.raw_catalog(self.day):
                name=table['name'];rows=store.raw_rows(name,self.day)
                self.records[name]=rows
                self.schemas[name]=dict(columns=table['fields'],rows=len(rows),present=table['present'])
                self._table(name,table['fields'],rows)
        relationships=dag_tag_graph(self.records.get('airflow_dag_inventory',[]))
        self.dag_link_coverage=relationships['coverage']
        links=[dict(stage=self.meta['stage'],upstream_dag_id=a,downstream_dag_id=b,source='airflow_upstream_tags') for a,b in relationships['edges']]
        fields=[dict(name=k,type='string',nullable=False) for k in ('stage','upstream_dag_id','downstream_dag_id','source')]
        self.records['declared_dag_links']=links;self.schemas['declared_dag_links']=dict(columns=fields,rows=len(links),present=self.schemas['airflow_dag_inventory']['present'])
        self._table('declared_dag_links',fields,links)
        self.db.commit();self.db.enable_load_extension(False);self.db.execute('PRAGMA query_only=ON')
        if hasattr(self.db,'setlimit'):
            # Full task logs can exceed 1 MB. Permit the stored cells while
            # keeping query result previews bounded independently below.
            largest=max((len(row.get('raw_json','').encode()) for row in self.records.get('airflow_task_diagnostics',[])),default=0)
            self.db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,max(1000000,largest+65536))
            self.db.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH,12000)
        self.read_tables=set();self.db.set_authorizer(self._authorize)

    def _table(self,name,fields,rows):
        columns=[f['name'] for f in fields]
        def sql_type(t):
            if t.startswith(('int','uint')) or t=='bool':return 'INTEGER'
            return 'REAL' if t.startswith(('float','double','decimal')) else 'TEXT'
        self.db.execute('CREATE TABLE '+identifier(name)+' ('+','.join(identifier(f['name'])+' '+sql_type(f['type']) for f in fields)+')')
        def scalar(value):
            if isinstance(value,(dict,list)):return json.dumps(value,default=str)
            if isinstance(value,(date,datetime)):return value.isoformat()
            return value
        if rows:self.db.executemany('INSERT INTO '+identifier(name)+' VALUES ('+','.join('?' for _ in columns)+')',([scalar(row.get(k)) for k in columns] for row in rows))

    def _authorize(self,action,arg1,arg2,database,source):
        if action in (sqlite3.SQLITE_SELECT,sqlite3.SQLITE_RECURSIVE):return sqlite3.SQLITE_OK
        if action==sqlite3.SQLITE_READ and arg1 in self.schemas:
            self.read_tables.add(arg1);return sqlite3.SQLITE_OK
        # SQLite labels reads from a query-local CTE with no database name.
        # Main/temp metadata and every non-allowlisted physical table stay denied.
        if action==sqlite3.SQLITE_READ and database is None:return sqlite3.SQLITE_OK
        if action==sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in FUNCTIONS:return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    def close(self):self.db.close()

    def manifest(self):
        return dict(stage=self.meta['stage'],snapshot_date=self.meta['snapshot_date'],collected_at=self.meta['collected_at'],
            tables={k:{field:v[field] for field in ('rows','present')} for k,v in self.schemas.items()},
            core_warnings=self.meta.get('warnings',[]),optional_coverage=self.meta.get('operational_coverage',{}),dag_link_coverage=self.dag_link_coverage)

    def column_index(self):
        """Names only: enough to write SQL without a separate discovery call."""
        return {name:','.join(field['name'] for field in schema['columns']) for name,schema in self.schemas.items()}

    def describe(self,names):
        if not isinstance(names,list) or not 1<=len(names)<=10:raise ValueError('Choose 1–10 tables.')
        if any(n not in self.schemas for n in names):raise ValueError('Unknown table.')
        result={n:self.schemas[n] for n in names}
        if 'airflow_monitoring_logs' in names:result['airflow_monitoring_logs']={**result['airflow_monitoring_logs'],'model_types':CONTRACT['columns']}
        return result

    def _evidence(self,kind,columns,rows,tables,**extra):
        clipped=False;safe=[]
        used=len(json.dumps(columns))+len(json.dumps(extra))+1500
        cell_limit=max(30,min(4000,(32000-used)//max(1,len(columns))))
        for row in rows:
            values=[]
            for value in row:
                value=clean(value)
                if isinstance(value,str) and len(value)>cell_limit:value=value[:cell_limit]+'…';clipped=True
                values.append(value)
            row_size=len(json.dumps(values,ensure_ascii=False))
            if safe and used+row_size>32000:extra['truncated']=True;break
            safe.append(values)
            used+=row_size
        result=clean(dict(id=f'E{len(self.evidence)+1}',kind=kind,stage=self.meta['stage'],snapshot_date=str(self.day),
            collected_at=self.meta['collected_at'],tables=sorted(tables),columns=columns,rows=safe,returned_rows=len(safe),clipped=clipped,**extra))
        self.evidence.append(result);return result

    def query(self,sql,max_rows=100):
        if not isinstance(sql,str) or len(sql)>12000 or not re.match(r'^\s*(SELECT|WITH)\b',sql,re.I):raise ValueError('Use one read-only SELECT or WITH query.')
        if type(max_rows) is not int or not 1<=max_rows<=200:raise ValueError('Return between 1 and 200 rows.')
        deadline=time.monotonic()+2;self.read_tables.clear()
        self.db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        cursor=None
        try:
            cursor=self.db.execute(sql)
            if not cursor.description:raise ValueError('A query must return rows.')
            columns=[c[0] for c in cursor.description];rows=cursor.fetchmany(max_rows+1)
            if not self.read_tables:raise ValueError('Query at least one saved table.')
            return self._evidence('query',columns,[list(r) for r in rows[:max_rows]],self.read_tables,
                sql=sql,truncated=len(rows)>max_rows,row_limit=max_rows)
        except sqlite3.DatabaseError as exc:raise ValueError('Query rejected: '+str(exc)[:240]) from None
        finally:
            if cursor:cursor.close()
            self.db.set_progress_handler(None,0)

    def lineage(self,scope,dag_id,task_id,direction,depth,run_id):
        if scope not in ('tasks','dags') or direction not in ('upstream','downstream','both') or depth not in ('direct','all'):raise ValueError('Invalid trace scope, direction or depth.')
        up=defaultdict(set);down=defaultdict(set)
        if scope=='tasks':
            if not task_id:raise ValueError('Choose a task_id for a task trace.')
            known={r['task_id'] for r in self.records['airflow_task_definitions'] if r['dag_id']==dag_id}
            if not known or task_id not in known:raise ValueError('This task has no current definition in the snapshot; dependency counts are unknown.')
            for edge in self.records['airflow_dag_dependencies']:
                if edge['dag_id']==dag_id and edge['upstream_task_id'] and edge['downstream_task_id']:
                    a,b=edge['upstream_task_id'],edge['downstream_task_id'];up[b].add(a);down[a].add(b)
            runs=[r for r in self.records['airflow_dag_runs'] if r['dag_id']==dag_id]
            if run_id and not any(r['run_id']==run_id for r in runs):raise ValueError('Requested run is outside the saved snapshot.')
            if not run_id and runs:run_id=max(runs,key=lambda r:str(r.get('started_at') or r.get('queued_at') or ''))['run_id']
            start=task_id;tables=['airflow_dag_dependencies','airflow_task_runs']
        else:
            if not any(r['dag_id']==dag_id for r in self.records['airflow_dag_inventory']):raise ValueError('DAG not found in saved inventory.')
            for edge in self.records['declared_dag_links']:
                a,b=edge['upstream_dag_id'],edge['downstream_dag_id'];up[b].add(a);down[a].add(b)
            start=dag_id;tables=['airflow_dag_inventory','declared_dag_links','airflow_dag_runs']
        rows=[];counts={}
        for side,adj in [('upstream',up),('downstream',down)]:
            if direction not in (side,'both'):continue
            nodes=sorted(trace(start,adj,depth=='all'));counts[side]=len(nodes)
            for node in nodes:
                if scope=='tasks':
                    instances=[r for r in self.records['airflow_task_runs'] if r['dag_id']==dag_id and r['run_id']==run_id and r['task_id']==node]
                    state=', '.join(sorted({r.get('state') or 'no state' for r in instances})) or 'No collected instance'
                else:
                    runs=[r for r in self.records['airflow_dag_runs'] if r['dag_id']==node]
                    state=max(runs,key=lambda r:str(r.get('started_at') or r.get('queued_at') or '')).get('state') if runs else 'No observed run today'
                rows.append([side,node,state])
        return self._evidence('lineage',['direction','task_id' if scope=='tasks' else 'dag_id','saved_state'],rows[:200],tables,
            scope=scope,dag_id=dag_id,task_id=task_id,run_id=run_id,depth=depth,counts=counts,truncated=len(rows)>200,
            dependency_coverage=None if scope=='tasks' else self.dag_link_coverage,
            source_note='Current task definition; relationship does not prove a sole cause.' if scope=='tasks' else 'Saved Airflow upstream tags; downstream is derived by reversing links. Known links only: missing/unknown tags cannot establish absence. Tags do not prove scheduling enforcement or run-to-run pairing.')
