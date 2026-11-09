"""Local collector storage: monthly partitions, one atomic daily Parquet file."""
import json
import hashlib
import os
import tempfile
import threading
from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lineage_app.data.model import CONTRACT, DAG_FIELDS, DEFINITION_FIELDS, INSTANCE_FIELDS, monitoring_rows
from lineage_app.data.operations import TABLES as OP_TABLES

UTC = pa.timestamp('us', tz='UTC')
WALL = pa.timestamp('us')
def diagnostic_key(row):
    identity=[row.get(k) for k in ('dag_id','run_id','task_id')]
    return 'task:'+hashlib.sha256(json.dumps(identity,ensure_ascii=False).encode()).hexdigest()+':'+str(row.get('map_index',-1))

def schema(fields):
    return pa.schema(fields)

SCHEMAS = {
    'airflow_dag_runs': schema([
        ('stage',pa.string()),('dag_id',pa.string()),('run_id',pa.string()),('run_date',pa.date32()),
        ('state',pa.string()),('run_type',pa.string()),('logical_date',UTC),('data_interval_start',UTC),
        ('data_interval_end',UTC),('run_after',UTC),('queued_at',UTC),('started_at',UTC),('completed_at',UTC),
        ('duration_seconds',pa.float64()),('dag_run_url',pa.string()),('collected_at',UTC),
        *[(k,pa.bool_() if k in ('dag_is_paused','dag_catchup','dag_has_import_errors') else
           pa.int64() if k in ('dag_max_active_runs','dag_max_active_tasks') else pa.string()) for k in DAG_FIELDS.values()],
        ('dag_owners_json',pa.string()),('dag_tags_json',pa.string()),('dag_run_triggered_by',pa.string()),
        ('dag_details_collected_at_utc',UTC),
    ]),
    'airflow_task_runs': schema([
        ('stage',pa.string()),('dag_id',pa.string()),('run_id',pa.string()),('run_date',pa.date32()),
        ('task_id',pa.string()),('map_index',pa.int64()),('state',pa.string()),('operator',pa.string()),
        ('try_number',pa.int64()),('queued_at',UTC),('started_at',UTC),('completed_at',UTC),
        ('duration_seconds',pa.float64()),('collected_at',UTC),
        *[(k, pa.bool_() if k in ('task_definition_depends_on_past','task_definition_wait_for_downstream')
           else pa.float64() if k=='task_definition_retries' else pa.string()) for k in DEFINITION_FIELDS.values()],
        *[(k,pa.int64() if k in ('task_max_tries','task_pool_slots','task_priority_weight') else pa.string()) for k in INSTANCE_FIELDS.values()],
        ('task_details_collected_at_utc',UTC),
    ]),
    'airflow_dag_dependencies': schema([
        ('stage',pa.string()),('dag_id',pa.string()),('snapshot_date',pa.date32()),
        ('upstream_task_id',pa.string()),('downstream_task_id',pa.string()),('collected_at',UTC),
    ]),
    'airflow_dag_inventory': schema([
        ('stage',pa.string()),('dag_id',pa.string()),('snapshot_date',pa.date32()),('raw_json',pa.string()),('collected_at',UTC),
    ]),
    'airflow_task_definitions': schema([
        ('stage',pa.string()),('dag_id',pa.string()),('task_id',pa.string()),('snapshot_date',pa.date32()),
        ('raw_json',pa.string()),('collected_at',UTC),
    ]),
    # Cross-DAG links (tags, triggers, sensors, S3 markers, assets) from dp_dag_lineage_export.
    # Live API collection has no link evidence, so it writes this day's file empty.
    'airflow_dag_links': schema([
        ('stage',pa.string()),('snapshot_date',pa.date32()),
        *[(k,pa.string()) for k in ('link_kind','evidence','upstream_dag_id','downstream_dag_id','upstream_source_id',
            'downstream_source_id','resolution','declared_by_dag_id','task_id','detail')],
        ('collected_at',UTC),
    ]),
}
TYPES = {'STRING':pa.string(),'DATE':pa.date32(),'TIMESTAMP_NTZ':WALL,'DOUBLE':pa.float64(),
         'BIGINT':pa.int64(),'INT':pa.int32(),'BOOLEAN':pa.bool_()}
SCHEMAS['airflow_monitoring_logs'] = schema([(k,TYPES[v]) for k,v in CONTRACT['columns'].items()])
for table,_ in OP_TABLES.values():
    SCHEMAS[table]=schema([('stage',pa.string()),('snapshot_date',pa.date32()),
        ('record_key',pa.string()),('raw_json',pa.string()),('collected_at',UTC)])
IDENTITIES = {
    'airflow_dag_runs': ('stage','dag_id','run_id'),
    'airflow_task_runs': ('stage','dag_id','run_id','task_id','map_index'),
}

class ParquetStore:
    """One source's Parquet. Each saved date keeps snapshots/<date>.json; metadata.json is the newest.

    With day=..., metadata()/load() read that date's snapshot instead of the newest one.
    Day views share the source store's lock.
    """
    def __init__(self, root, day=None, lock=None):
        self.root = Path(root)
        self.day = day
        self.lock = lock or threading.RLock()

    def incomplete(self):
        return (self.root/'.collection-incomplete').exists()

    def at(self, day):
        """A read view of one saved date (YYYY-MM-DD), sharing this store's lock."""
        return ParquetStore(self.root, day=day, lock=self.lock)

    def snapshot_path(self, day):
        return self.root/'snapshots'/f'{day}.json'

    def dates(self):
        """Saved snapshot dates, newest first."""
        found={p.stem for p in (self.root/'snapshots').glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].json')}
        latest=self.root/'metadata.json'
        if latest.exists():found.add(json.loads(latest.read_text())['snapshot_date'])
        return sorted(found,reverse=True)

    def _write_json(self, path, value):
        path.parent.mkdir(parents=True,exist_ok=True)
        tmp=path.with_name('.'+path.name+'.tmp')
        tmp.write_text(json.dumps(value,indent=2))
        os.replace(tmp,path)

    def path(self, dataset, day):
        return self.root / dataset / f'prti_dttm={day:%Y%m}' / f'{day.isoformat()}.parquet'

    def raw_catalog(self, day):
        """Inspect only the requested snapshot partition, using stored schemas."""
        with self.lock:
            result=[]
            for name,declared in SCHEMAS.items():
                path=self.path(name,day);present=path.is_file()
                parquet=pq.ParquetFile(path) if present else None
                actual=parquet.schema_arrow if parquet else declared
                result.append(dict(name=name,path=path.relative_to(self.root).as_posix(),present=present,
                    rows=parquet.metadata.num_rows if parquet else None,
                    bytes=path.stat().st_size if present else None,
                    fields=[dict(name=f.name,type=str(f.type),nullable=f.nullable) for f in actual]))
            return result

    def raw_rows(self, dataset, day):
        if dataset not in SCHEMAS:raise ValueError('Unknown raw table.')
        with self.lock:
            path=self.path(dataset,day)
            return self.read_file(path) if path.is_file() else []

    def raw_file(self, dataset, day):
        if dataset not in SCHEMAS:raise ValueError('Unknown raw table.')
        with self.lock:
            path=self.path(dataset,day)
            if not path.is_file():raise ValueError('No saved Parquet file for this table in the snapshot.')
            return path.read_bytes()

    def read_file(self, path):
        # ParquetFile prevents Hive partition inference from injecting prti_dttm into raw rows.
        return pq.ParquetFile(path).read().to_pylist()

    def read(self, dataset, start=None, end=None):
        result = []
        with self.lock:
            for p in sorted((self.root/dataset).glob('prti_dttm=*/*.parquet')):
                day = date.fromisoformat(p.stem)
                if (start and day < start) or (end and day > end): continue
                result.extend(self.read_file(p))
        return result

    def write_day(self, dataset, day, rows, merge=False):
        with self.lock:
            target = self.path(dataset, day)
            if merge and target.exists():
                fields = IDENTITIES[dataset]
                combined = {tuple(r[k] for k in fields):r for r in self.read_file(target)}
                for row in rows: combined[tuple(row[k] for k in fields)] = row
                rows = list(combined.values())
            table = pa.Table.from_pylist(rows, schema=SCHEMAS[dataset])
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix='.writing-', suffix='.tmp', dir=target.parent)
            os.close(fd)
            try:
                pq.write_table(table, name, compression='snappy')
                os.replace(name, target)
            finally:
                if os.path.exists(name): os.unlink(name)
            return len(rows)

    def save_collection(self, result, base, stage, source_info=None, extra_metadata=None):
        now, today = result['collected_at'], result['snapshot_date']
        with self.lock:
            self.root.mkdir(parents=True,exist_ok=True)
            marker=self.root/'.collection-incomplete'
            marker.write_text('A cache update started but has not committed its metadata.\n')
            for dataset,key in [('airflow_dag_runs','dag_runs'),('airflow_task_runs','task_runs')]:
                by_day = defaultdict(list)
                for row in result[key]: by_day[row['run_date']].append(row)
                for day,rows in by_day.items(): self.write_day(dataset,day,rows,merge=True)
            # A complete day's current definition is replaced, not merged with obsolete edges.
            self.write_day('airflow_dag_dependencies',today,result['dependencies'])
            self.write_day('airflow_dag_inventory',today,[dict(stage=stage,dag_id=d['dag_id'],
                snapshot_date=today,raw_json=json.dumps(d,default=str),collected_at=now) for d in result['inventory']])
            self.write_day('airflow_task_definitions',today,[dict(stage=stage,dag_id=d['dag_id'],task_id=d['task_id'],
                snapshot_date=today,raw_json=json.dumps(d,default=str),collected_at=now) for d in result['definitions']])
            self.write_day('airflow_dag_links',today,result.get('dag_links',[]))
            if 'operations' in result:
                for resource,(dataset,_) in OP_TABLES.items():
                    self.write_day(dataset,today,[dict(stage=stage,snapshot_date=today,record_key=diagnostic_key(r) if resource=='diagnostics' else str(i),
                        raw_json=json.dumps(r,default=str),collected_at=now) for i,r in enumerate(result['operations'].get(resource,[]))])
            # The model is deliberately rebuilt from disk, never the in-memory API response.
            dags = self.read('airflow_dag_runs',today,today)
            tasks = self.read('airflow_task_runs',today,today)
            by_day = defaultdict(list)
            for row in monitoring_rows(dags,tasks,now): by_day[row['run_date']].append(row)
            by_day.setdefault(today, [])
            for day,rows in by_day.items(): self.write_day('airflow_monitoring_logs',day,rows)
            self.root.mkdir(parents=True,exist_ok=True)
            metadata = dict(base_url=base,stage=stage,collected_at=now.isoformat(),
                snapshot_date=today.isoformat(),start=today.isoformat(),end=today.isoformat(),
                warnings=result['warnings'],coverage=result['coverage'],
                operational_coverage={k:v for k,v in result.get('operational_coverage',{}).items() if k in OP_TABLES})
            if source_info:
                metadata.update({k:source_info[k] for k in ('source_id','source_name','dependency_mode') if k in source_info})
            if extra_metadata:metadata.update(extra_metadata)
            self._write_json(self.snapshot_path(today.isoformat()),metadata)
            latest=self.root/'metadata.json'
            # An older imported date is selectable but never replaces the newest snapshot.
            if not latest.exists() or json.loads(latest.read_text())['snapshot_date']<=today.isoformat():
                self._write_json(latest,metadata)
            marker.unlink()

    def metadata(self):
        path = self.root/'metadata.json'
        if self.day:
            dated=self.snapshot_path(self.day)
            if dated.exists():path=dated
            elif not path.exists() or json.loads(path.read_text())['snapshot_date']!=self.day:return None
        if not path.exists():return None
        metadata=json.loads(path.read_text())
        metadata['operational_coverage']={k:v for k,v in metadata.get('operational_coverage',{}).items() if k in OP_TABLES}
        if (self.root/'.collection-incomplete').exists():
            metadata['warnings']=[*metadata.get('warnings',[]),
                'A local cache update was interrupted. Recollect this window before relying on its coverage or freshness.']
        return metadata

    def task_diagnostics(self,dag,run,task):
        """Read one task's saved logs; use Parquet predicates for new snapshots."""
        with self.lock:
            meta=self.metadata()
            if not meta:return []
            day=date.fromisoformat(meta['snapshot_date'])
            path=self.path('airflow_task_diagnostics',day)
            if not path.is_file():return []
            runs=[r for r in self.raw_rows('airflow_task_runs',day) if (r['dag_id'],r['run_id'],r['task_id'])==(dag,run,task)]
            if not runs:return []
            keys=[diagnostic_key(r) for r in runs]
            rows=pq.read_table(path,filters=[('record_key','in',keys)],partitioning=None).to_pylist()
            if not rows:
                # Earlier snapshots used numeric record keys.
                legacy=pq.ParquetFile(path).read(columns=['record_key']).column('record_key').to_pylist()
                if any(not k.startswith('task:') for k in legacy):rows=self.read_file(path)
            return [r for item in rows if (r:=json.loads(item['raw_json'])) and (r.get('dag_id'),r.get('run_id'),r.get('task_id'))==(dag,run,task)]

    def load(self,include_diagnostics=True):
        with self.lock:
            meta = self.metadata()
            if not meta: return None
            today = date.fromisoformat(meta['snapshot_date'])
            meta['start']=meta['end']=meta['snapshot_date']
            return dict(metadata=meta, rows=self.read('airflow_monitoring_logs',today,today),
                inventory=[json.loads(r['raw_json']) for r in self.read('airflow_dag_inventory',today,today)],
                definitions=[json.loads(r['raw_json']) for r in self.read('airflow_task_definitions',today,today)],
                dependencies=self.read('airflow_dag_dependencies',today,today),
                dag_links=self.read('airflow_dag_links',today,today),
                operations={key:[json.loads(r['raw_json']) for r in self.read(table,today,today)]
                    if key in meta.get('operational_coverage',{}) and (include_diagnostics or key!='diagnostics') else [] for key,(table,_) in OP_TABLES.items()})
