"""Task/run/map/attempt isolation, old caches, and safe full-log rendering."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from sample_data import sample_collection
from lineage_app.data.parquet import ParquetStore
from lineage_app.views.task_logs import task_log

class TaskLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=ParquetStore(Path(self.tmp.name)/'uat')
        self.result=sample_collection(history_days=1)
        row=next(r for r in self.result['task_runs'] if r['run_date']==self.result['snapshot_date'] and r['state']=='failed')
        self.original_entry=f'{row["map_index"]}:{row["try_number"]}'
        mapped={**row,'map_index':3,'try_number':2}
        self.result['task_runs'].append(mapped)
        self.params=dict(dag=row['dag_id'],run=row['run_id'],task=row['task_id'])
        self.diag={k:mapped[k] for k in ('dag_id','run_id','task_id','map_index','try_number','state')}
        self.diag.update(tries=[],tries_status={'state':'complete'},logs=[
            dict(try_number=1,log=dict(text='first attempt <script>bad</script>',truncated=False),status={'state':'complete'}),
            dict(try_number=2,log=dict(text='second attempt\n'+'all lines\n'*350+'END',truncated=False),status={'state':'complete'})])
        self.result['operations']['diagnostics']=[self.diag]
        self.save()
    def save(self):self.store.save_collection(self.result,'https://airflow.example.com','uat')
    def test_map_and_attempt_selection_read_only_and_full_download(self):
        self.assertEqual(task_log(self.store,dict(self.params,entry='3:1'),download=True),'first attempt <script>bad</script>')
        html=task_log(self.store,dict(self.params,entry='3:1'))
        self.assertIn('&lt;script&gt;bad&lt;/script&gt;',html);self.assertNotIn('<script>',html)
        self.assertIn('Attempt 2 · Map 3',html)
        self.assertNotIn('second attempt',html)
        self.assertTrue(task_log(self.store,dict(self.params,entry='3:2'),download=True).endswith('END'))
        self.assertIn('END',task_log(self.store,dict(self.params,entry='3:2',page='2')))
        self.assertNotIn('first attempt',task_log(self.store,dict(self.params,entry=self.original_entry)))
        with self.assertRaisesRegex(ValueError,'No task instance'):task_log(self.store,dict(self.params,run='another-run'))
    def test_old_numeric_keys_and_excerpt_logs_are_readable(self):
        old=copy.deepcopy(self.diag);old.pop('logs')
        old['log']=dict(text='legacy saved excerpt',try_number=2,truncated=True);old['log_status']={'state':'complete'}
        table='airflow_task_diagnostics';day=self.result['snapshot_date']
        rows=self.store.raw_rows(table,day);rows[0]['record_key']='0';rows[0]['raw_json']=json.dumps(old)
        self.store.write_day(table,day,rows)
        html=task_log(self.store,dict(self.params,entry='3:2'))
        self.assertIn('legacy saved excerpt',html);self.assertIn('Saved log is incomplete',html)
    def test_missing_denied_and_not_started_are_distinct(self):
        self.assertIn('Logs were not collected',task_log(self.store,dict(self.params,entry=self.original_entry)))
        self.diag['logs'][1].update(log=None,status={'state':'unavailable','message':'Permission denied'})
        self.save()
        self.assertIn('Permission denied',task_log(self.store,dict(self.params,entry='3:2')))
        row=next(r for r in self.result['task_runs'] if r['map_index']==-1 and r['task_id']==self.params['task'] and r['run_id']==self.params['run'])
        row['try_number']=0;row['state']='upstream_failed';self.save()
        with self.assertRaisesRegex(ValueError,'failed task instance'):task_log(self.store,dict(self.params,entry='-1:0'))
    def test_successful_map_is_hidden_even_if_old_snapshot_contains_its_log(self):
        row=next(r for r in self.result['task_runs'] if r['map_index']==3 and r['run_id']==self.params['run'] and r['task_id']==self.params['task'])
        row['state']='success';self.save()
        html=task_log(self.store,self.params)
        self.assertNotIn('Map 3',html);self.assertNotIn('second attempt',html)
        with self.assertRaisesRegex(ValueError,'failed task instance'):task_log(self.store,dict(self.params,entry='3:1'),download=True)
    def test_regular_workspace_load_skips_log_payloads_and_keeps_parquet_schema(self):
        self.assertEqual(self.store.load(include_diagnostics=False)['operations']['diagnostics'],[])
        self.assertEqual(self.store.load()['operations']['diagnostics'][0]['logs'][1]['log']['text'],self.diag['logs'][1]['log']['text'])
        self.assertEqual(self.store.task_diagnostics(self.params['dag'],self.params['run'],self.params['task']),[self.diag])
    def test_ai_can_query_full_log_larger_than_one_megabyte_without_dumping_it(self):
        from lineage_app.ai.data import SnapshotData
        text='full saved log\n'*85000
        self.diag['logs'][1]['log']['text']=text;self.save()
        engine=SnapshotData(self.store);self.addCleanup(engine.close)
        result=engine.query("SELECT length(json_extract(raw_json, '$.logs[1].log.text')) FROM airflow_task_diagnostics")
        self.assertEqual(result['rows'],[[len(text)]])
        self.assertLess(len(json.dumps(result)),2000)

if __name__=='__main__':unittest.main()
