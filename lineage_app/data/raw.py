"""Read-only access and downloads for the current snapshot's physical tables."""
import csv
import io
import json
import zipfile
from datetime import date

from lineage_app.data.model import CONTRACT
from lineage_app.data.operations import TABLES as OP_TABLES

LABELS={
    'airflow_dag_runs':'DAG runs',
    'airflow_task_runs':'Task runs',
    'airflow_dag_dependencies':'Task dependencies',
    'airflow_dag_inventory':'DAG inventory',
    'airflow_task_definitions':'Task definitions',
    'airflow_dag_links':'Cross-DAG links',
    'airflow_monitoring_logs':'Monitoring logs',
}
LABELS.update({name:label for name,label in OP_TABLES.values()})
FORMATS={'parquet','csv','json','schema'}


def json_default(value):
    return value.isoformat() if hasattr(value,'isoformat') else str(value)


def json_bytes(value):
    return (json.dumps(value,default=json_default,ensure_ascii=False,indent=2)+'\n').encode('utf-8')


def cell_text(value):
    if value is None:return 'null'
    if isinstance(value,bool):return 'true' if value else 'false'
    if isinstance(value,(dict,list)):return json.dumps(value,default=json_default,ensure_ascii=False)
    return json_default(value)


def file_size(size):
    if size is None:return '—'
    if size<1024:return f'{size:,} B'
    if size<1024**2:return f'{size/1024:.1f} KB'
    return f'{size/1024**2:.1f} MB'


class RawData:
    def __init__(self,store,metadata):
        self.store=store;self.meta=metadata
        self.day=date.fromisoformat(metadata['snapshot_date'])
        self.stage=metadata['stage']
        self.catalog=store.raw_catalog(self.day)
        self.by_name={t['name']:t for t in self.catalog}
        for key,(name,_) in OP_TABLES.items():
            self.by_name[name]['collection']=metadata.get('operational_coverage',{}).get(key,{'state':'not_collected'})

    def table(self,name):
        if name not in self.by_name:raise ValueError('Unknown raw table.')
        return self.by_name[name]

    def schema(self,name):
        table=self.table(name)
        fields=[dict(field) for field in table['fields']]
        if name=='airflow_monitoring_logs':
            for field in fields:
                field['model_type']=CONTRACT['columns'].get(field['name'])
        result=dict(table=name,stage=self.stage,snapshot_date=str(self.day),
            stored_file=table['path'],present=table['present'],rows=table['rows'],fields=fields)
        if 'collection' in table:result['collection']=table['collection']
        return result

    def preview(self,name,query='',page=1,size=25):
        self.table(name)
        size=size if size in (25,50,100) else 25
        rows=self.store.raw_rows(name,self.day)
        query=query.strip()[:250]
        if query:
            needle=query.casefold()
            rows=[r for r in rows if any(needle in cell_text(v).casefold() for v in r.values())]
        count=len(rows);pages=max(1,(count+size-1)//size)
        page=min(max(1,page),pages);offset=(page-1)*size
        return dict(rows=rows[offset:offset+size],count=count,page=page,pages=pages,size=size,
            start=offset+1 if count else 0,end=min(offset+size,count),query=query)

    def download(self,name,format):
        table=self.table(name)
        if format not in FORMATS:raise ValueError('Unknown raw download format.')
        prefix=f'{name}-{self.stage}-{self.day}'
        if format=='schema':
            return json_bytes(self.schema(name)),prefix+'.schema.json','application/json; charset=utf-8'
        if not table['present']:raise ValueError('No saved Parquet file for this table in the snapshot.')
        if format=='parquet':
            return self.store.raw_file(name,self.day),prefix+'.parquet','application/octet-stream'
        rows=self.store.raw_rows(name,self.day)
        if format=='json':return json_bytes(rows),prefix+'.json','application/json; charset=utf-8'
        output=io.StringIO(newline='')
        fields=[field['name'] for field in table['fields']]
        writer=csv.writer(output);writer.writerow(fields)
        for row in rows:writer.writerow('' if row.get(k) is None else cell_text(row[k]) for k in fields)
        return output.getvalue().encode('utf-8'),prefix+'.csv','text/csv; charset=utf-8'

    def bundle(self):
        """Keep original Parquet bytes and partition paths; include every schema."""
        output=io.BytesIO()
        with self.store.lock:
            with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as archive:
                for table in self.catalog:
                    name=table['name']
                    if table['present']:archive.writestr(table['path'],self.store.raw_file(name,self.day))
                    archive.writestr('schemas/'+name+'.schema.json',json_bytes(self.schema(name)))
                archive.writestr('monitoring-schema.json',json_bytes(CONTRACT))
                archive.writestr('metadata.json',json_bytes(self.meta))
                archive.writestr('manifest.json',json_bytes(dict(stage=self.stage,snapshot_date=str(self.day),
                    tables=[dict(name=t['name'],file=t['path'] if t['present'] else None,
                        rows=t['rows'],columns=len(t['fields']),bytes=t['bytes'],collection=t.get('collection'),schema='schemas/'+t['name']+'.schema.json') for t in self.catalog])))
        return output.getvalue(),f'airflow-raws-{self.stage}-{self.day}.zip','application/zip'
