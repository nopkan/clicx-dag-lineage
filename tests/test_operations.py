"""Operational calculations, optional API failures, daily scope and safe rendering."""
import copy
import json
import tempfile
import threading
import unittest
from datetime import datetime,timedelta,timezone
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import parse_qs,urlsplit

from lineage_app.clients.airflow import AirflowClient,APIError
from sample_data import sample_collection
from lineage_app.data.model import BKK,CONTRACT
from lineage_app.data.operations import TABLES,collect_operations
from lineage_app.views.operations import performance_stats,performance_rankings,performance_run_key,performance,queue_wait,related_tasks,task_key
from lineage_app.data.parquet import ParquetStore,SCHEMAS
from lineage_app.views.render import Workspace

class OperationalViews(unittest.TestCase):
    def test_health_keeps_each_airflows_status_separate_and_missing_status_unknown(self):
        from lineage_app.views.operations import health
        w=self.workspace('health')
        w.meta['sources']=[{'name':'Data Platform'},{'name':'Risk Team'}]
        w.data['operations']['health']=[
            dict(airflow_source='Data Platform',metadatabase={'status':'healthy'},scheduler={'status':'healthy'}),
            dict(airflow_source='Risk Team',metadatabase={'status':'healthy'},scheduler={'status':'unhealthy'})]
        html=health(w)
        platform,risk=html.split('aria-label="Risk Team"',1)
        self.assertIn('airflow-team-badge data-platform',platform)
        self.assertNotIn('class="unhealthy"',platform)
        self.assertIn('airflow-team-badge risk-team',risk)
        self.assertIn('class="unhealthy"',risk)
        w.data['operations']['health'].pop()
        risk=health(w).split('aria-label="Risk Team"',1)[1].split('</section>',1)[0]
        self.assertNotIn('class="healthy"',risk)
        self.assertEqual(risk.count('Not reported'),4)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=ParquetStore(Path(self.tmp.name)/'uat');self.result=sample_collection(history_days=1)
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        self.data=self.store.load()
    def workspace(self,view='performance',**params):
        return Workspace(copy.deepcopy(self.data),dict(view=view,**params),'csrf','cached',raw_store=self.store)
    def test_completed_runtime_and_queue_samples_exclude_blocked_tasks_and_bad_waits(self):
        w=self.workspace();t=w.tasks[0];at=datetime.combine(w.day,datetime.min.time())
        w.tasks=[{**t,'task_state':'success','task_duration_seconds':120,'task_queued_at_bkk':at,'task_started_at_bkk':at+timedelta(seconds=30)},
            {**t,'task_state':'upstream_failed','task_duration_seconds':9999,'task_queued_at_bkk':at,'task_started_at_bkk':at},
            {**t,'task_state':'running','task_duration_seconds':9999,'task_queued_at_bkk':None,'task_started_at_bkk':at},
            {**t,'task_state':'failed','task_duration_seconds':60,'task_queued_at_bkk':at+timedelta(seconds=30),'task_started_at_bkk':at}]
        stats=performance_stats(w)
        self.assertEqual(stats['durations'],[120,60]);self.assertEqual(stats['waits'],[30])
        self.assertEqual(stats['starts'][0],3)
        self.assertIsNone(queue_wait(w.tasks[1]));self.assertIsNone(queue_wait(w.tasks[-1]))
        self.assertTrue(all(r['run_date']==w.day for r in stats['dag_runtimes']))
    def test_performance_dag_selection_filters_tasks_to_exact_run_before_ranking(self):
        w=self.workspace();stats=performance_stats(w);row=stats['dag_runtimes'][0]
        other={**row,'run_id':'other-run','dag_duration_seconds':99999}
        w.runs.append(other)
        task=next(t for t in stats['task_runtimes'] if performance_run_key(t)==performance_run_key(row))
        w.tasks.append({**task,'run_id':'other-run','map_index':99,'task_duration_seconds':99999})
        w.params['perf_run']=performance_run_key(row)
        stats=performance_stats(w);selected=performance_rankings(w,stats)
        self.assertEqual(selected['dag_rows'],stats['dag_runtimes'])
        self.assertTrue(selected['task_rows'])
        self.assertTrue(all((t['dag_id'],t['run_id'])==(row['dag_id'],row['run_id']) for t in selected['task_rows']))
        self.assertNotIn(99,[r['map_index'] for r in selected['task_rows']])
        self.assertEqual(selected['selected_run'],row)
    def test_performance_task_selection_shows_its_running_parent_and_exact_map(self):
        w=self.workspace();task=performance_stats(w)['task_runtimes'][0]
        mapped={**task,'map_index':42};w.tasks.append(mapped)
        parent=next(r for r in w.runs if performance_run_key(r)==performance_run_key(task))
        parent['dag_state']='running';parent['dag_duration_seconds']=None
        w.params['perf_task']=task_key(mapped)
        stats=performance_stats(w);selected=performance_rankings(w,stats)
        self.assertEqual(selected['selected_task']['map_index'],42)
        self.assertEqual(selected['dag_rows'],[parent]);self.assertNotIn(parent,stats['dag_runtimes'])
        self.assertEqual(selected['task_rows'],stats['task_runtimes'])
        self.assertIn('Running',performance(w))
    def test_performance_links_toggle_locally_and_clear_both_selections(self):
        class Links(HTMLParser):
            def __init__(self,html):super().__init__();self.items=[];self.feed(html)
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='a' and attrs.get('data-rank'):self.items.append(attrs)
        w=self.workspace();run=performance_stats(w)['dag_runtimes'][0]
        w.params['perf_run']=performance_run_key(run)
        links=Links(performance(w)).items
        self.assertTrue(all(parse_qs(urlsplit(a['href']).query)['view']==['performance'] for a in links))
        task_link=next(a for a in links if a['data-rank']=='task')
        w.params.update({k:v[-1] for k,v in parse_qs(urlsplit(task_link['href']).query).items()})
        links=Links(performance(w)).items
        dag_links=[a for a in links if a['data-rank']=='dag']
        self.assertEqual(len(dag_links),1);self.assertEqual(dag_links[0]['data-run'],run['run_id'])
        selected=next(a for a in links if a['data-rank']=='task' and a.get('aria-current')=='true')
        toggle=parse_qs(urlsplit(selected['href']).query)
        self.assertNotIn('perf_task',toggle);self.assertEqual(toggle['perf_run'],[performance_run_key(run)])
        clear=parse_qs(urlsplit(dag_links[0]['href']).query)
        self.assertNotIn('perf_run',clear);self.assertNotIn('perf_task',clear)
        self.assertTrue(all(urlsplit(a['href']).fragment=='performance-rankings' for a in links))
        self.assertNotIn('perf_run=',w.url(view='lineage'));self.assertNotIn('perf_task=',w.url(view='overview'))
    def test_failure_relationships_are_run_scoped_and_keep_mapped_identities(self):
        w=self.workspace('troubleshooting');selected=next(t for t in w.tasks if t['task_state']=='failed')
        blocked=next(t for t in related_tasks(w,selected,'down') if t['task_state']=='upstream_failed')
        other={**blocked,'run_id':'another-run','map_index':9};mapped={**blocked,'map_index':4}
        w.tasks.extend([other,mapped])
        related=related_tasks(w,selected,'down')
        self.assertNotIn(other,related);self.assertIn(mapped,related)
        self.assertNotEqual(task_key(blocked),task_key(mapped))
        self.assertIn(selected,related_tasks(w,blocked,'up'))
    def test_every_view_renders_saved_values_and_escapes_untrusted_content(self):
        bad='<script>alert(1)</script>'
        self.data['operations']['diagnostics'][0]['log']['text']=bad
        self.data['operations']['import_errors']=[dict(filename=bad,stack_trace=bad,timestamp='2026-10-29T01:00:00Z')]
        for view in ('performance','troubleshooting','health'):
            html=self.workspace(view).render()
            self.assertIn('<h1>'+view.capitalize()+'</h1>',html)
            self.assertNotIn('<script',html);self.assertNotIn('onclick=',html)
            if view=='health':self.assertIn('&lt;script&gt;',html)
            if view=='troubleshooting':
                self.assertIn('task-log-frame',html);self.assertNotIn(bad,html)
        self.assertIn('Task log',self.workspace('troubleshooting').render())
    def test_old_cache_is_unknown_and_does_not_claim_empty_or_healthy(self):
        self.data.pop('operations');self.data['metadata'].pop('operational_coverage')
        for view in ('health',):
            html=self.workspace(view).render()
            self.assertIn('Not collected.',html)
            self.assertNotIn('No current import errors.',html)
            self.assertNotIn('class="healthy"',html)
        self.assertIn('Longest DAG runs',self.workspace().render())
        self.assertIn('Task issues',self.workspace('troubleshooting').render())
    def test_global_filters_do_not_silently_limit_system_views(self):
        for view in ('health',):
            w=self.workspace(view,q='no-matching-dag',tag='none',state='failed')
            self.assertEqual(len(w.tasks),len(self.workspace().tasks))
            self.assertNotIn('class="global-filters"',w.render())
    def test_removed_views_tables_and_old_metadata_are_excluded(self):
        html=self.workspace().render()
        self.assertNotIn('view=capacity',html);self.assertNotIn('view=assets',html)
        for name in ('airflow_pools','airflow_assets','airflow_asset_events'):
            self.assertNotIn(name,SCHEMAS)
            with self.assertRaises(ValueError):self.store.raw_rows(name,self.result['snapshot_date'])
        path=self.store.root/'metadata.json';meta=json.loads(path.read_text())
        meta['operational_coverage']['assets']={'state':'complete','rows':1}
        path.write_text(json.dumps(meta))
        self.assertNotIn('assets',self.store.metadata()['operational_coverage'])
        self.assertNotIn('assets',self.store.load()['operations'])
    def test_optional_snapshots_replace_day_and_do_not_load_history(self):
        day=self.result['snapshot_date'];table=TABLES['health'][0]
        row=self.store.raw_rows(table,day)[0];old=day-timedelta(days=1)
        self.store.write_day(table,old,[{**row,'snapshot_date':old,'raw_json':'{"scheduler":{"status":"old"}}'}])
        self.assertEqual(self.store.load()['operations']['health'][0]['scheduler']['status'],'healthy')
        result=copy.deepcopy(self.result);result['operations']['health']=[]
        self.store.save_collection(result,'https://airflow.example.com','uat')
        self.assertEqual(self.store.load()['operations']['health'],[])
        self.assertTrue(self.store.path(table,old).is_file())
        self.assertEqual(len(CONTRACT['columns']),85)

class OptionalCollection(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,10,28,18,tzinfo=timezone.utc)
        self.lower=datetime(2026,10,28,17,tzinfo=timezone.utc);self.upper=self.lower+timedelta(days=1)
        self.client=AirflowClient('https://airflow.example.com','secret-token','uat')
        self.calls=[]
    def get(self,path,**params):
        self.calls.append((path,params))
        if path=='/api/v2/monitor/health':return {'scheduler':{'status':'healthy'}}
        if path=='/api/v2/importErrors':raise APIError(404,path)
        if path.endswith('/tries'):
            return {'task_instances':[{'try_number':2,'map_index':3,'state':'failed','start_date':self.lower.isoformat()},
                {'try_number':1,'map_index':3,'state':'failed','start_date':(self.lower-timedelta(hours=1)).isoformat()}],'total_entries':2}
        if '/logs/' in path:
            if params.get('token'):return {'content':['last secret-token line'],'continuation_token':None}
            return {'content':['first line'],'continuation_token':'page-2'}
        if path.endswith('/dependencies'):return {'dependencies':[{'name':'Pool','reason':'No open slots'}]}
        raise AssertionError(path)
    def task(self,**changes):
        return dict(dict(dag_id='dag/a',run_id='run+1',task_id='group.task',map_index=3,try_number=2,state='failed'),**changes)
    def collect(self,instances):
        self.client.get=self.get
        return collect_operations(self.client,instances,self.now,self.lower,self.upper)
    def test_optional_denial_does_not_lose_other_resources_or_request_removed_apis(self):
        data,coverage=self.collect([])
        self.assertEqual(coverage['import_errors']['state'],'unavailable')
        self.assertEqual(coverage['import_errors']['http_status'],404)
        self.assertEqual(coverage['health']['state'],'complete')
        self.assertEqual(set(data),{'health','import_errors','diagnostics'})
        self.assertFalse(any('/pools' in path or '/assets' in path for path,p in self.calls))
    def test_mapped_attempts_log_pagination_redaction_and_daily_scope(self):
        data,coverage=self.collect([self.task()]);diag=data['diagnostics'][0]
        self.assertTrue(any('/dag%2Fa/' in path and '/run%2B1/' in path and '/group.task/3/tries' in path for path,p in self.calls))
        self.assertEqual(len(diag['tries']),2)
        self.assertEqual([r['try_number'] for r in diag['logs']],[1,2])
        self.assertTrue(all(r['log']['text']=='first line\nlast [redacted] line' for r in diag['logs']))
        self.assertTrue(all(not r['log']['truncated'] for r in diag['logs']))
        self.assertNotIn('secret-token',json.dumps(data))
        self.assertTrue(all(p['map_index']==3 for path,p in self.calls if '/logs/' in path))
    def test_only_failed_tasks_have_diagnostic_requests_without_a_100_task_cap(self):
        tasks=[self.task(map_index=i) for i in range(107)]
        tasks += [self.task(map_index=1000+i,state=state) for i,state in enumerate(('success','running','queued','skipped','upstream_failed','deferred','up_for_retry',None))]
        data,coverage=self.collect(tasks)
        self.assertEqual(coverage['diagnostics']['state'],'complete');self.assertEqual(coverage['diagnostics']['eligible'],107)
        self.assertEqual(len(data['diagnostics']),107)
        self.assertTrue(all(len(r['logs'])==2 for r in data['diagnostics']))
        self.assertTrue(all(r['state']=='failed' for r in data['diagnostics']))
        self.assertEqual(coverage['diagnostics']['log_attempts'],214)
        self.assertEqual(coverage['diagnostics']['scope'],'failed_tasks')
        self.assertTrue(all(p['map_index']<107 for path,p in self.calls if '/logs/' in path))
        self.assertTrue(all(int(path.split('/')[-2])<107 for path,p in self.calls if path.endswith('/tries')))
        self.assertFalse(any(path.endswith('/dependencies') for path,p in self.calls))
    def test_full_logs_keep_more_than_four_pages_and_64k_characters(self):
        original=self.get
        def huge(path,**params):
            if '/logs/' in path:
                self.assertEqual(params['full_content'],'true')
                page=int(params.get('token',0))
                return {'content':[str(page)+'x'*70000],'continuation_token':str(page+1) if page<5 else None}
            return original(path,**params)
        self.get=huge
        data,_=self.collect([self.task()]);log=data['diagnostics'][0]['logs'][0]['log']
        self.assertFalse(log['truncated']);self.assertGreater(len(log['text']),420000)
        self.assertTrue(log['text'].startswith('0x'));self.assertTrue(log['text'].endswith('x'*70000))
        self.assertTrue(all(p['full_content']=='true' for path,p in self.calls if '/logs/' in path))
    def test_live_cursor_ends_collection_and_exposes_incomplete_snapshot(self):
        original=self.get
        def growing(path,**params):
            if '/logs/' in path:return {'content':['first line'] if not params.get('token') else [],'continuation_token':'same'}
            return original(path,**params)
        self.get=growing
        data,coverage=self.collect([self.task()])
        self.assertTrue(data['diagnostics'][0]['logs'][0]['log']['truncated'])
        self.assertEqual(coverage['diagnostics']['state'],'partial')
    def test_attempt_denial_does_not_hide_other_failed_task_attempts(self):
        original=self.get
        def denied(path,**params):
            if path.endswith('/tries') or path.endswith('/logs/1'):raise APIError(403,path)
            return original(path,**params)
        self.get=denied
        data,coverage=self.collect([self.task()])
        logs=data['diagnostics'][0]['logs']
        self.assertEqual([r['try_number'] for r in logs],[1,2])
        self.assertEqual(logs[0]['status']['message'],'Permission denied')
        self.assertIn('first line',logs[1]['log']['text'])
        self.assertEqual(coverage['diagnostics']['unavailable_logs'],1)
        self.assertEqual(coverage['diagnostics']['state'],'partial')
    def test_task_with_no_attempt_has_no_log_requests(self):
        original=self.get
        def not_started(path,**params):
            if path.endswith('/tries'):return {'task_instances':[],'total_entries':0}
            if '/logs/' in path:self.fail('A task with no execution cannot have attempt logs')
            return original(path,**params)
        self.get=not_started
        data,_=self.collect([self.task(state='upstream_failed',try_number=0)])
        self.assertEqual(data['diagnostics'],[])
    def test_cancel_propagates_instead_of_saving_unknown_resources(self):
        self.client.cancel.set();self.client.get=lambda path,**params:(_ for _ in ()).throw(RuntimeError('Collection cancelled.'))
        with self.assertRaisesRegex(RuntimeError,'cancelled'):
            collect_operations(self.client,[],self.now,self.lower,self.upper)

if __name__=='__main__':unittest.main()
