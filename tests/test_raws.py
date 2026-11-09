"""Physical snapshot browsing and faithful, scoped raw downloads."""
import csv
import io
import json
import tempfile
import unittest
import zipfile
from datetime import timedelta
from pathlib import Path

from sample_data import sample_collection
from lineage_app.data.parquet import ParquetStore,SCHEMAS
from lineage_app.data.raw import RawData
from lineage_app.views.render import Workspace

class RawTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=ParquetStore(Path(self.tmp.name)/'uat')
        self.result=sample_collection(history_days=1)
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        self.meta=self.store.metadata();self.source=RawData(self.store,self.meta)
    def test_catalog_uses_all_stored_schemas_and_snapshot_partition_only(self):
        day=self.result['snapshot_date'];past=day-timedelta(days=1)
        self.assertTrue(self.store.path('airflow_dag_runs',past).exists())
        self.assertEqual(set(self.source.by_name),set(SCHEMAS))
        for table in self.source.catalog:
            name=table['name']
            self.assertTrue(table['present'])
            self.assertEqual(table['rows'],len(self.store.read_file(self.store.path(name,day))))
            self.assertEqual([f['name'] for f in table['fields']],SCHEMAS[name].names)
            self.assertTrue(table['path'].endswith(f'{day}.parquet'))
            self.assertNotIn(str(past),table['path'])
        schema=self.source.schema('airflow_monitoring_logs')
        self.assertEqual(len(schema['fields']),85)
        self.assertTrue(all(f.get('model_type') for f in schema['fields']))
        schema=self.source.schema('airflow_dag_runs')
        collected=next(f for f in schema['fields'] if f['name']=='collected_at')
        self.assertEqual(collected['type'],'timestamp[us, tz=UTC]')
    def test_parquet_is_original_bytes_and_csv_json_preserve_raw_values(self):
        day=self.result['snapshot_date']
        for name in SCHEMAS:
            expected=self.store.read_file(self.store.path(name,day))
            parquet,filename,mime=self.source.download(name,'parquet')
            self.assertEqual(parquet,self.store.path(name,day).read_bytes())
            self.assertTrue(filename.endswith('.parquet'))
            payload,filename,mime=self.source.download(name,'json')
            parsed=json.loads(payload)
            self.assertEqual(len(parsed),len(expected))
            if name=='airflow_dag_inventory':
                self.assertIsInstance(parsed[0]['raw_json'],str)
                self.assertEqual(parsed[0]['raw_json'],expected[0]['raw_json'])
                self.assertIn('+00:00',parsed[0]['collected_at'])
            payload,filename,mime=self.source.download(name,'csv')
            reader=csv.DictReader(io.StringIO(payload.decode()))
            rows=list(reader)
            self.assertEqual(reader.fieldnames,SCHEMAS[name].names)
            self.assertEqual(len(rows),len(expected))
            if name=='airflow_dag_inventory':self.assertEqual(rows[0]['raw_json'],expected[0]['raw_json'])
    def test_raw_model_does_not_rewrite_saved_fields_for_the_workspace(self):
        day=self.result['snapshot_date'];name='airflow_monitoring_logs'
        rows=self.store.raw_rows(name,day)
        rows[0]['dag_avg_duration_7d_seconds']=123
        self.store.write_day(name,day,rows)
        w=Workspace(self.store.load(),{'view':'raws'},'c','cached',raw_store=self.store)
        self.assertIsNone(w.rows[0]['dag_avg_duration_7d_seconds'])
        downloaded=json.loads(self.source.download(name,'json')[0])
        self.assertEqual(downloaded[0]['dag_avg_duration_7d_seconds'],123)
    def test_bundle_contains_original_partition_files_all_schemas_and_no_history(self):
        content,name,mime=self.source.bundle();day=self.result['snapshot_date']
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            files=set(z.namelist())
            self.assertIn('metadata.json',files);self.assertIn('monitoring-schema.json',files)
            manifest=json.loads(z.read('manifest.json'))
            self.assertEqual(manifest['snapshot_date'],str(day))
            self.assertEqual(len(manifest['tables']),len(SCHEMAS))
            for table in self.source.catalog:
                self.assertEqual(z.read(table['path']),self.store.path(table['name'],day).read_bytes())
                self.assertIn('schemas/'+table['name']+'.schema.json',files)
            self.assertFalse(any(str(day-timedelta(days=1)) in path for path in files))
            self.assertFalse(any(path.startswith('/') or '..' in path.split('/') for path in files))
    def test_paged_search_clamps_requests_and_does_not_limit_downloads(self):
        page=self.source.preview('airflow_monitoring_logs',page=99999,size=25)
        self.assertEqual(page['page'],page['pages']);self.assertLessEqual(len(page['rows']),25)
        chosen=page['rows'][0]['dag_id']
        match=self.source.preview('airflow_monitoring_logs',query=chosen,page=-5,size=999)
        self.assertEqual(match['page'],1);self.assertEqual(match['size'],25)
        # Raw search includes metadata; another DAG's upstream tag may match too.
        self.assertTrue(all(chosen in json.dumps(row,default=str) for row in match['rows']))
        expected=self.source.by_name['airflow_monitoring_logs']['rows']
        self.assertEqual(len(json.loads(self.source.download('airflow_monitoring_logs','json')[0])),expected)
        missing=self.source.preview('airflow_monitoring_logs',query='no-such-row-xyz')
        self.assertEqual((missing['start'],missing['end'],missing['count']),(0,0,0))
    def test_absent_files_remain_distinct_from_saved_empty_tables(self):
        other=ParquetStore(Path(self.tmp.name)/'empty')
        result={**self.result,'dag_runs':[],'task_runs':[]}
        other.save_collection(result,'https://airflow.example.com','uat')
        source=RawData(other,other.metadata())
        self.assertFalse(source.by_name['airflow_dag_runs']['present'])
        self.assertIsNone(source.by_name['airflow_dag_runs']['rows'])
        self.assertEqual(source.by_name['airflow_monitoring_logs']['rows'],0)
        with self.assertRaises(ValueError):source.download('airflow_dag_runs','parquet')
        schema=json.loads(source.download('airflow_dag_runs','schema')[0])
        self.assertFalse(schema['present']);self.assertTrue(schema['fields'])
    def test_guards_reject_paths_and_unsupported_formats(self):
        for name in ('../metadata.json','/etc/passwd','metadata.json','not-a-table'):
            with self.assertRaises(ValueError):self.source.download(name,'json')
        with self.assertRaises(ValueError):self.source.download('airflow_dag_runs','exe')
        with self.assertRaises(ValueError):self.store.raw_file('../metadata.json',self.result['snapshot_date'])
    def test_raws_replaces_old_navigation_hides_dag_filters_and_escapes_values(self):
        day=self.result['snapshot_date'];name='airflow_dag_inventory'
        rows=self.store.raw_rows(name,day)
        record=json.loads(rows[0]['raw_json']);record['description']='<script>alert(1)</script>'
        rows[0]['raw_json']=json.dumps(record)
        self.store.write_day(name,day,rows)
        w=Workspace(self.store.load(),{'view':'raws','raw_table':name,'q':'missing-dag','state':'failed'},'c','cached',raw_store=self.store)
        html=w.render()
        self.assertIn('<h1>Raws</h1>',html)
        self.assertNotIn('view=analysis',html);self.assertNotIn('view=schema',html)
        self.assertNotIn('class="global-filters"',html)
        self.assertIn('/raws/download-all',html)
        self.assertIn('raw-mode-rows',html);self.assertIn('raw-mode-schema',html)
        self.assertNotIn('<script',html);self.assertNotIn('onclick=',html)
        self.assertIn('&lt;script&gt;',html)
        for table in SCHEMAS:self.assertIn(table,html)
        for old in ('analysis','schema'):
            prior=Workspace(self.store.load(),{'view':old},'c','cached',raw_store=self.store)
            self.assertEqual(prior.view,'raws')
        prior=Workspace(self.store.load(),{'view':'schema'},'c','cached',raw_store=self.store)
        self.assertEqual(prior.params['raw_table'],'airflow_monitoring_logs')
        self.assertEqual(prior.params['raw_mode'],'schema')

if __name__=='__main__':unittest.main()
