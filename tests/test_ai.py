"""Grounded AI querying, history boundaries and safe rendering; no live API calls."""
import copy
import json
import os
import secrets
import tempfile
import time
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import patch

from lineage_app.ai.data import SnapshotData,clean,snapshot_id
from lineage_app.ai.chat import AISettings,ChatService,DataAgent,settings
from lineage_app.ai.store import ChatStore
from lineage_app.views.ai import answer_html,chat_view
from sample_data import sample_collection
from lineage_app.data.parquet import ParquetStore
from lineage_app.views.render import Workspace

class FakeOpenAI:
    def __init__(self):self.responses=self;self.calls=[]
    def create(self,**kwargs):
        self.calls.append(copy.deepcopy(kwargs))
        inputs=kwargs['input'];last=inputs[-1]
        if isinstance(last,dict) and last.get('type')=='function_call_output':
            evidence=json.loads(last['output']);count=evidence['rows'][0][0]
            return Obj(status='completed',output=[Obj(type='message')],output_text=f'There are **{count}** matching tasks. [E1]',usage=Obj(input_tokens=10,output_tokens=10,total_tokens=20))
        question=next(item['content'] for item in reversed(inputs) if isinstance(item,dict) and item.get('role')=='user')
        state='success' if 'succeed' in question else 'failed'
        call=Obj(type='function_call',name='query_data',arguments=json.dumps(dict(sql=f"SELECT COUNT(*) AS task_count FROM airflow_monitoring_logs WHERE record_level='task' AND task_state='{state}'",max_rows=5)),call_id='call_'+str(len(self.calls)))
        return Obj(status='completed',output=[call],output_text='',usage=Obj(input_tokens=10,output_tokens=10,total_tokens=20))

class AITests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.store=ParquetStore(self.root/'uat')
        self.result=sample_collection(history_days=1);self.store.save_collection(self.result,'https://airflow.example.com','uat')
        self.owner='a'*64;self.config=AISettings('test-secret-key','gpt-5-mini')
    def engine(self):
        engine=SnapshotData(self.store);self.addCleanup(engine.close);return engine
    def wait(self,service,chat):
        until=time.monotonic()+5
        while time.monotonic()<until:
            page=service.page(self.owner,self.store,chat)
            if not page['pending']:return page
            time.sleep(.01)
        self.fail('AI fixture did not finish')
    def test_snapshot_counts_are_exact_and_exclude_old_partitions(self):
        data=self.engine()
        result=data.query("SELECT COUNT(*) FROM airflow_monitoring_logs WHERE record_level='task'")
        expected=sum(t['run_date']==self.result['snapshot_date'] for t in self.result['task_runs'])
        self.assertEqual(result['rows'],[[expected]])
        self.assertEqual(result['tables'],['airflow_monitoring_logs'])
        self.assertEqual(len(data.schemas),11)  # 10 collected tables + cross-DAG links
        self.assertEqual(len(data.describe(['airflow_monitoring_logs'])['airflow_monitoring_logs']['columns']),85)
    def test_queries_are_read_only_and_cannot_read_files_or_metadata(self):
        data=self.engine()
        for sql in ('DROP TABLE airflow_task_runs','PRAGMA database_list',"ATTACH DATABASE '/tmp/private' AS secret",'SELECT * FROM sqlite_master',"SELECT load_extension('/tmp/private') FROM airflow_dag_runs",'SELECT 1; SELECT 2',"WITH x AS (SELECT 1) DELETE FROM airflow_dag_runs"):
            with self.assertRaises(ValueError,msg=sql):data.query(sql)
        self.assertTrue(data.query('SELECT COUNT(*) FROM airflow_dag_runs')['rows'][0][0])
        repeated=data.query('SELECT COUNT(*) FROM airflow_dag_runs')
        self.assertTrue(repeated['rows'][0][0]);self.assertEqual(repeated['tables'],['airflow_dag_runs'])
    def test_limits_schema_validation_json_and_redaction(self):
        data=self.engine();result=data.query('SELECT dag_id FROM airflow_monitoring_logs',max_rows=2)
        self.assertTrue(result['truncated']);self.assertEqual(result['returned_rows'],2)
        with self.assertRaises(ValueError):data.query('SELECT * FROM airflow_dag_runs',201)
        with self.assertRaises(ValueError):data.describe(['../../.env'])
        result=data.query("SELECT json_extract(raw_json,'$.dag_id') FROM airflow_dag_inventory",1)
        self.assertTrue(result['rows'][0][0])
        sensitive='{"password":"private-value","description":"Bearer abc.def.ghi"}'
        self.assertNotIn('private-value',clean(sensitive));self.assertNotIn('abc.def.ghi',clean(sensitive))
        self.assertNotIn('sk-proj-abcdefghijklmnop',clean('sk-proj-abcdefghijklmnop'))
    def test_trace_matches_direct_and_all_levels_and_rejects_unknown_run(self):
        data=self.engine();dag=self.result['inventory'][0]['dag_id']
        direct=data.lineage('tasks',dag,'start_pipeline','downstream','direct',None)
        full=data.lineage('tasks',dag,'start_pipeline','downstream','all',None)
        self.assertEqual(direct['counts']['downstream'],1);self.assertEqual(full['counts']['downstream'],8)
        with self.assertRaises(ValueError):data.lineage('tasks',dag,'start_pipeline','both','all','not-today')
        with self.assertRaises(ValueError):data.lineage('tasks',dag,'missing','both','all',None)
        dag_sql=dag.replace("'","''")
        closure=data.query(f"""WITH RECURSIVE descendants(task_id) AS (
            SELECT downstream_task_id FROM airflow_dag_dependencies
            WHERE dag_id='{dag_sql}' AND upstream_task_id='start_pipeline'
            UNION SELECT e.downstream_task_id FROM airflow_dag_dependencies e
            JOIN descendants d ON e.upstream_task_id=d.task_id
            WHERE e.dag_id='{dag_sql}' AND e.downstream_task_id IS NOT NULL
        ) SELECT COUNT(*) FROM descendants""")
        self.assertEqual(closure['rows'],[[8]])
    def test_snapshot_identity_and_interrupted_update_are_enforced(self):
        with self.assertRaises(ValueError):SnapshotData(self.store,'wrong-identity')
        (self.store.root/'.collection-incomplete').write_text('incomplete')
        with self.assertRaises(ValueError):SnapshotData(self.store)

    def test_dag_links_and_ai_traces_are_derived_from_saved_airflow_tags(self):
        data=self.engine()
        links=data.query('SELECT upstream_dag_id, downstream_dag_id, source FROM declared_dag_links')['rows']
        self.assertTrue(links)
        self.assertEqual({row[2] for row in links},{'airflow_upstream_tags'})
        direct=data.lineage('dags','dp_ingest_core_banking',None,'downstream','direct',None)
        full=data.lineage('dags','dp_ingest_core_banking',None,'downstream','all',None)
        self.assertEqual(direct['counts']['downstream'],2)
        self.assertEqual(full['counts']['downstream'],6)
        self.assertIn('airflow_dag_inventory',direct['tables'])
        self.assertIn('dp_ingest_core_banking',data.manifest()['dag_link_coverage'])
    def test_conversation_is_local_owner_scoped_durable_and_idempotent(self):
        chats=ChatStore(self.root/'chats.sqlite3');meta=self.store.metadata();request=secrets.token_hex(16)
        chat,answer,created=chats.enqueue(self.owner,meta,'','What failed?',request)
        self.assertTrue(created)
        again=chats.enqueue(self.owner,meta,chat,'What failed?',request)
        self.assertEqual(again,(chat,answer,False))
        self.assertFalse(chats.wait(self.owner,chat,meta['stage'],timeout=0))
        with self.assertRaises(ValueError):chats.wait('b'*64,chat,meta['stage'],timeout=0)
        with self.assertRaises(ValueError):chats.wait(self.owner,chat,'different-stage',timeout=0)
        with self.assertRaises(ValueError):chats.export('b'*64,chat)
        chats.finish(answer,'One task. [E1]',[],{},'fixture')
        self.assertTrue(chats.wait(self.owner,chat,meta['stage'],timeout=0))
        restarted=ChatStore(self.root/'chats.sqlite3')
        self.assertEqual(len(restarted.export(self.owner,chat)['messages']),2)
        changed={**meta,'collected_at':'2099-01-01T00:00:00Z'}
        with self.assertRaises(ValueError):restarted.enqueue(self.owner,changed,chat,'Now?',secrets.token_hex(16))
        self.assertFalse(restarted.page(self.owner,changed,chat)['current'])
    def test_recent_context_compacts_but_full_history_is_searchable(self):
        chats=ChatStore(self.root/'chats.sqlite3');chat=''
        for i in range(10):
            chat,answer,_=chats.enqueue(self.owner,self.store.metadata(),chat,f'Question {i} about lineage',secrets.token_hex(16))
            chats.finish(answer,f'Answer {i}',[],{},'fixture')
        chat,answer,_=chats.enqueue(self.owner,self.store.metadata(),chat,'What did I ask first?',secrets.token_hex(16))
        record,user,recent,notes=chats.turn(self.owner,chat,answer)
        self.assertEqual(len(recent),12);self.assertIn('Question 0',notes)
        self.assertIn('Question 0',chats.search(self.owner,chat,user['id'],'Question 0')['messages'][0]['content'])
        self.assertIn('Question 0',chats.search(self.owner,chat,user['id'],'','oldest')['messages'][0]['content'])
    def test_agent_uses_store_false_and_fresh_queries_for_follow_ups(self):
        client=FakeOpenAI();service=ChatService(self.root/'chats.sqlite3',self.config,client)
        request=secrets.token_hex(16);chat=service.ask(self.owner,self.store,'','What failed?',request)
        first=self.wait(service,chat);self.assertEqual(first['messages'][-1]['state'],'complete')
        self.assertTrue(first['messages'][-1]['evidence'])
        service.ask(self.owner,self.store,chat,'What failed?',request)
        self.assertEqual(len(client.calls),2)
        service.ask(self.owner,self.store,chat,'And succeeded?',secrets.token_hex(16));second=self.wait(service,chat)
        self.assertEqual(len(client.calls),4);self.assertEqual(len(second['messages']),4)
        self.assertIn('What failed?',json.dumps(client.calls[2]['input'],default=str))
        self.assertTrue(all(call['store'] is False for call in client.calls))
        self.assertEqual(client.calls[0]['tools'][0]['strict'],True)
        self.assertTrue(client.calls[0]['parallel_tool_calls'])
        self.assertIn('COLUMN INDEX',client.calls[0]['instructions'])
        self.assertEqual(first['messages'][-1]['usage']['requests'],2)
        self.assertIn('snapshot_load_ms',first['messages'][-1]['usage'])
        self.assertIn('finished_at',first['messages'][-1]['usage'])
        with service.store.db() as db:
            events=[json.loads(r['event']) for r in db.execute('SELECT event FROM ai_events')]
        self.assertTrue(any(event['kind']=='openai' for event in events))
        self.assertNotIn(self.config.api_key,json.dumps(events))
        self.assertNotIn(self.config.api_key,json.dumps(service.store.export(self.owner,chat)))
    def test_independent_queries_share_one_model_round_trip(self):
        calls=[]
        def create(**kwargs):
            calls.append(kwargs)
            if len(calls)==1:
                output=[Obj(type='function_call',name='query_data',arguments=json.dumps(dict(sql=f"SELECT COUNT(*) FROM airflow_task_runs WHERE state='{state}'",max_rows=1)),call_id=state) for state in ('failed','success')]
                return Obj(status='completed',output=output,output_text='',usage=None)
            return Obj(status='completed',output=[],output_text='Saved counts are available. [E1] [E2]',usage=None)
        answer,evidence,usage=DataAgent(self.config,Obj(responses=Obj(create=create))).answer(self.engine(),'Compare failed and succeeded tasks',[],'',lambda **kw:{},lambda _:None)
        self.assertEqual(len(calls),2);self.assertEqual(len(evidence),2)
        self.assertEqual(usage['tool_calls'],2);self.assertEqual(len(usage['api_calls']),2)
    def test_ungrounded_answers_do_not_retry_seven_model_calls(self):
        calls=[]
        def create(**kwargs):
            calls.append(kwargs);return Obj(status='completed',output=[],output_text='An unverified count.',usage=None)
        with self.assertRaisesRegex(RuntimeError,'No snapshot evidence'):
            DataAgent(self.config,Obj(responses=Obj(create=create))).answer(self.engine(),'Count tasks',[],'',lambda **kw:{},lambda _:None)
        self.assertEqual(len(calls),2)
    def test_query_errors_are_logged_and_can_be_corrected(self):
        calls=[];events=[]
        def create(**kwargs):
            calls.append(kwargs)
            if len(calls)<3:
                sql='SELECT missing_column FROM airflow_dag_runs' if len(calls)==1 else 'SELECT COUNT(*) FROM airflow_dag_runs'
                call=Obj(type='function_call',name='query_data',arguments=json.dumps(dict(sql=sql,max_rows=1)),call_id=str(len(calls)))
                return Obj(status='completed',output=[call],output_text='',usage=None)
            return Obj(status='completed',output=[],output_text='The saved count is verified. [E1]',usage=None)
        answer,evidence,usage=DataAgent(self.config,Obj(responses=Obj(create=create))).answer(self.engine(),'Count runs',[],'',lambda **kw:{},lambda _:None,lambda event,usage:events.append(event))
        errors=[event for event in events if event.get('kind')=='tool' and event.get('status')=='error']
        self.assertEqual(len(errors),1);self.assertIn('missing_column',errors[0]['error'])
        self.assertEqual(len(evidence),1);self.assertEqual(usage['requests'],3)
    def test_history_question_uses_local_search_without_irrelevant_data_queries(self):
        calls=[]
        def create(**kwargs):
            calls.append(kwargs)
            if len(calls)==1:
                call=Obj(type='function_call',name='search_chat_history',arguments='{"query":"","order":"oldest"}',call_id='history1')
                return Obj(status='completed',output=[call],output_text='',usage=None)
            return Obj(status='completed',output=[Obj(type='message')],output_text='You first asked which DAGs failed.',usage=None)
        client=Obj(responses=Obj(create=create));searched=[]
        def search(**kwargs):
            searched.append(kwargs);return {'messages':[dict(role='user',content='Which DAGs failed?')]}
        answer,evidence,usage=DataAgent(self.config,client).answer(self.engine(),'What did I ask first?',[],'',search,lambda _:None)
        self.assertEqual(searched,[dict(query='',order='oldest')]);self.assertEqual(len(calls),2)
        self.assertEqual(evidence,[]);self.assertIn('first asked',answer)
    def test_missing_key_never_calls_provider_and_errors_do_not_expose_secrets(self):
        service=ChatService(self.root/'chats.sqlite3',AISettings())
        with self.assertRaises(ValueError):service.ask(self.owner,self.store,'','Question',secrets.token_hex(16))
        class Broken(FakeOpenAI):
            def create(self,**kwargs):raise RuntimeError('test-secret-key')
        broken=ChatService(self.root/'other.sqlite3',self.config,Broken())
        chat=broken.ask(self.owner,self.store,'','Question',secrets.token_hex(16));page=self.wait(broken,chat)
        self.assertEqual(page['messages'][-1]['state'],'failed')
        self.assertNotIn('test-secret-key',page['messages'][-1]['content'])
        usage=page['messages'][-1]['usage']
        self.assertEqual(usage['requests'],1);self.assertEqual(usage['api_calls'][0]['error_type'],'RuntimeError')
        self.assertNotIn('test-secret-key',json.dumps(usage))
    def test_markdown_and_evidence_cannot_inject_html_or_external_images(self):
        html=answer_html('<script>alert(1)</script>\n\n[x](javascript:alert(1))\n\n![x](https://example.com/track)\n\nCount [E1]',3,[{'id':'E1'}])
        self.assertNotIn('<script',html);self.assertNotIn('href="javascript:',html);self.assertNotIn('<img',html)
        self.assertIn('#evidence-3-E1',html)
        service=ChatService(self.root/'chats.sqlite3',self.config,FakeOpenAI())
        chat=service.ask(self.owner,self.store,'','What failed?',secrets.token_hex(16));page=self.wait(service,chat)
        page['current']=False
        w=Workspace(self.store.load(),{'view':'ai'},'csrf','cached',raw_store=self.store,ai_state=page)
        rendered=w.render();self.assertIn('Earlier snapshot',rendered);self.assertIn('name="question"',rendered)
        self.assertNotIn('raw_table=',chat_view(w));self.assertNotIn('<script',rendered)
    def test_dotenv_uses_standard_variable_without_displaying_key(self):
        (self.root/'.env').write_text('OPENAI_API_KEY=fixture-key\nOPENAI_MODEL=fixture-model\n')
        with patch.dict(os.environ,{},clear=True):
            config=settings(self.root)
            self.assertEqual(config.api_key,'fixture-key');self.assertEqual(config.model,'fixture-model')
            self.assertNotIn('fixture-key',repr(config))

if __name__=='__main__':unittest.main()
