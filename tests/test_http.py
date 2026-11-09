"""Real local HTTP integration with a fake read-only Airflow server."""
import http.cookiejar
import json
import re
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import io
import zipfile
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener

from lineage_app import server as app
from lineage_app.data.model import BKK
from lineage_app.data.parquet import SCHEMAS
from sample_data import sample_collection

class FakeAirflow(BaseHTTPRequestHandler):
    requests=[]
    rejected_path=None
    rejection_status=401
    task_state='success'
    extra_tags=[]
    def log_message(self,*args):pass
    def do_GET(self):
        self.requests.append((self.command,self.path))
        if urlsplit(self.path).path==self.rejected_path:
            self.send_response(self.rejection_status);self.end_headers();return
        if self.headers.get('Authorization')!='Bearer integration-test-token':
            self.send_response(401);self.end_headers();return
        parsed=urlsplit(self.path);params=parse_qs(parsed.query);today=datetime.now(BKK).replace(hour=0,minute=0,second=0,microsecond=0)
        dag=dict(dag_id='fixture_dag',dag_display_name='Fixture DAG',is_paused=False,is_stale=False,
            tags=[{'name':'Platform'},*self.extra_tags],owners=['test'],timetable_summary='0 8 * * *',timezone='Asia/Bangkok')
        run=dict(dag_id='fixture_dag',dag_run_id='manual__test',run_type='manual',state='success',
            start_date=today.isoformat(),end_date=(today+timedelta(minutes=3)).isoformat())
        path=parsed.path
        if path=='/api/v2/dags':data=dict(dags=[dag],total_entries=1)
        elif path.endswith('/details'):data=dag
        elif path.endswith('/tasks'):data=dict(tasks=[dict(task_id='only_task',operator_name='PythonOperator',downstream_task_ids=[],retries=1)],total_entries=1)
        elif path=='/api/v2/dags/~/dagRuns':data=dict(dag_runs=[] if params.get('state')==['queued'] else [run],total_entries=0 if params.get('state')==['queued'] else 1)
        elif path.endswith('/taskInstances'):data=dict(task_instances=[dict(dag_id='fixture_dag',dag_run_id='manual__test',task_id='only_task',
            map_index=-1,try_number=1,state=self.task_state,operator='PythonOperator',start_date=today.isoformat(),
            end_date=(today+timedelta(minutes=2)).isoformat())],total_entries=1)
        elif path=='/api/v2/monitor/health':data=dict(metadatabase={'status':'healthy'},scheduler={'status':'healthy','latest_scheduler_heartbeat':today.isoformat()})
        elif path=='/api/v2/importErrors':data=dict(import_errors=[],total_entries=0)
        elif path.endswith('/tries'):data=dict(task_instances=[dict(try_number=1,map_index=-1,state=self.task_state,start_date=today.isoformat())],total_entries=1)
        elif '/logs/' in path:data=dict(content=['first log line <script>unsafe()</script>']+[f'line {i}' for i in range(1,405)]+['last log line'],continuation_token=None)
        else:self.send_response(404);self.end_headers();return
        body=json.dumps(data).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(body)

class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();app.DATA_ROOT=Path(cls.tmp.name);app.STORES.clear();app.SESSIONS.clear()
        from lineage_app.data import aws_import
        def no_aws(*args,**kwargs):raise AssertionError('Tests must not call the AWS CLI.')
        cls.no_aws=patch.object(aws_import,'download',no_aws);cls.no_aws.start()
        cls.fake=ThreadingHTTPServer(('127.0.0.1',0),FakeAirflow)
        cls.web=ThreadingHTTPServer(('127.0.0.1',0),app.Handler)
        for server in (cls.fake,cls.web):threading.Thread(target=server.serve_forever,daemon=True).start()
        cls.base=f'http://127.0.0.1:{cls.web.server_port}'
    @classmethod
    def tearDownClass(cls):
        cls.fake.shutdown();cls.web.shutdown();cls.fake.server_close();cls.web.server_close();cls.tmp.cleanup();cls.no_aws.stop()
    def setUp(self):
        self.client=build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
        FakeAirflow.rejected_path=None;FakeAirflow.rejection_status=401;FakeAirflow.task_state='success';FakeAirflow.extra_tags=[]
    def live_key(self):return app.connections.cache_key('uat',f'http://127.0.0.1:{self.fake.server_port}')+'@'+datetime.now(BKK).date().isoformat()
    def live_store(self):return app.store_for('uat',f'http://127.0.0.1:{self.fake.server_port}')
    def open_sample(self,csrf):
        """Save the sample collection as a local source and open it through Local data."""
        base='https://airflow.example.com';result=sample_collection()
        app.store_for('uat',base).save_collection(result,base,'uat',source_info=dict(source_name='Sample Airflow'))
        key=app.connections.cache_key('uat',base)+'@'+result['snapshot_date'].isoformat()
        with self.post('/cached',dict(csrf=csrf,cache=key)) as response:
            self.assertTrue(response.url.endswith('/app'));response.read()
    def get(self,path):return self.client.open(self.base+path)
    def post(self,path,values):return self.client.open(Request(self.base+path,urlencode(values).encode()))
    def csrf(self):
        with self.get('/') as response:
            body=response.read().decode()
            self.assertIn("script-src 'none'",response.headers['Content-Security-Policy'])
            self.assertIn('HttpOnly',response.headers['Set-Cookie'])
        return re.search('name="csrf" value="([^"]+)"',body)[1]
    def collect_live(self,csrf):
        with self.post('/connect',dict(csrf=csrf,base=f'http://127.0.0.1:{self.fake.server_port}',token='integration-test-token',stage='uat')) as response:response.read()
        return self.wait_collection(csrf)
    def wait_collection(self,csrf):
        session=next(s for s in app.SESSIONS.values() if s['csrf']==csrf)
        deadline=time.monotonic()+10
        while not session['job']['done']:
            if time.monotonic()>deadline:self.fail('Collection did not finish')
            time.sleep(.02)
        with self.get('/progress') as response:return response.read().decode(),response.url
    def test_refresh_reuses_token_and_local_data_refresh_reimports_without_airflow(self):
        csrf=self.csrf();self.collect_live(csrf)
        session=next(s for s in app.SESSIONS.values() if s['csrf']==csrf)
        before=len(FakeAirflow.requests)
        with self.post('/refresh',dict(csrf=csrf,return_to='/app?view=inventory')) as response:response.read()
        body,url=self.wait_collection(csrf)
        self.assertIn('view=inventory',url);self.assertIn('<h1>DAGs</h1>',body)
        self.assertGreater(len(FakeAirflow.requests),before)
        self.assertEqual(session['token'],'integration-test-token')
        with self.post('/cached',dict(csrf=csrf,cache=self.live_key())) as response:response.read()
        self.assertNotIn('token',session)
        before=len(FakeAirflow.requests);downloads=[]
        from lineage_app.data import aws_import
        # Never call the real AWS CLI from tests.
        with patch.object(aws_import,'download',lambda root,cancel,progress,**chosen:downloads.append(root)):
            with self.post('/refresh',dict(csrf=csrf,return_to='/app?view=inventory')) as response:response.read()
            body,url=self.wait_collection(csrf)
        self.assertEqual(downloads,[app.DATA_ROOT])
        self.assertIn('view=inventory',url);self.assertIn('<h1>DAGs</h1>',body);self.assertNotIn('role="dialog"',body)
        self.assertEqual((session['mode'],session['cache'],session['airflow']),('cached','uat@'+datetime.now(BKK).date().isoformat(),self.live_key().partition('@')[0]))
        self.assertEqual(len(FakeAirflow.requests),before)

    def test_airflow_tags_survive_collection_and_drive_html_svg_and_ai_without_new_requests(self):
        FakeAirflow.extra_tags=[{'name':'layer:curate'},{'name':'upstream:source_outside_snapshot'}]
        csrf=self.csrf();self.collect_live(csrf)
        before=len(FakeAirflow.requests)
        with self.get('/app?view=dependency&focus=fixture_dag') as response:
            page=response.read().decode()
        self.assertIn('Curate Data Layer',page)
        self.assertIn('source_outside_snapshot',page)
        self.assertIn('Not in snapshot',page)
        self.assertNotIn('Import data after syncing Airflow tags',page)
        with self.get('/dependency.svg?view=dependency&focus=fixture_dag') as response:
            svg=response.read().decode()
        self.assertIn('airflow_upstream_tags',svg)
        from lineage_app.ai.data import SnapshotData
        with self.live_store().lock:
            data=SnapshotData(self.live_store())
            try:
                self.assertEqual(data.query('SELECT upstream_dag_id,downstream_dag_id FROM declared_dag_links')['rows'],
                                 [['source_outside_snapshot','fixture_dag']])
            finally:data.close()
        self.assertEqual(len(FakeAirflow.requests),before)

    def test_tags_popup_picks_one_value_per_key_and_search_matches_any_tag(self):
        FakeAirflow.extra_tags=[{'name':'layer:curate'},{'name':' frequency:daily'},{'name':'team:dc'},{'name':'cortex.team'}]
        csrf=self.csrf();self.collect_live(csrf)
        with self.get('/app?view=inventory') as response:page=response.read().decode()
        self.assertIn('<summary class="tag-picker-button"',page);self.assertIn('>Tags</summary>',page)
        self.assertIn('<select id="tag-pick-layer" name="tag_layer"><option value="" selected>Any</option><option value="curate">curate</option></select>',page)
        self.assertIn('<select id="tag-pick-frequency" name="tag_frequency"><option value="" selected>Any</option><option value="daily">daily</option></select>',page)  # stray space trimmed
        self.assertIn('name="tag_team"',page);self.assertNotIn('name="tag_project"',page)  # keys without values are left out
        self.assertIn('name="tag_q"',page);self.assertIn('<option value="cortex.team"></option>',page)
        def shown(query):
            with self.get('/app?'+urlencode(dict(view='inventory',**query))) as response:body=response.read().decode()
            main=body.split('<main',1)[1].split('<footer',1)[0]
            return 'fixture_dag' in main.replace('value="fixture_dag"',''),body
        for query,found in ((dict(tag_layer='curate',tag_frequency='daily'),True),(dict(tag_layer='curate',tag_team='other'),False),
                            (dict(tag_frequency='weekly'),False),(dict(tag_q='CORTEX'),True),(dict(tag_q='dail'),True),
                            (dict(tag_q='nothing-like-this'),False),(dict(tag='layer:curate'),True)):
            with self.subTest(**query):self.assertEqual(shown(query)[0],found)
        _,body=shown(dict(tag_layer='curate',tag_team='dc'))
        self.assertIn('Tags · layer: curate · team: dc</summary>',body)
        self.assertIn('<option value="curate" selected>curate</option>',body)
        self.assertIn('href="/app?view=inventory">Clear tags</a>',body)

    def test_planner_applies_and_exports_without_api_calls_or_parquet_writes(self):
        FakeAirflow.extra_tags=[{'name':'upstream:none'}]
        csrf=self.csrf();self.collect_live(csrf)
        before=len(FakeAirflow.requests)
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        query=urlencode(dict(view='planner',plan_dag='fixture_dag',plan_start='03:30',plan_buffer='20'))
        with self.get('/app?'+query) as response:
            body=response.read().decode()
            self.assertIn("script-src 'none'",response.headers['Content-Security-Policy'])
        self.assertIn('<h1>Schedule</h1>',body)
        self.assertIn('class="gantt-pick"',body);self.assertIn('data-planner-dag="fixture_dag" data-row="selected"',body)
        self.assertNotIn('<script',body)
        self.assertLess(body.index('aria-label="Dependency"'),body.index('aria-label="Schedule"'))
        with self.get('/planner/export?'+query) as response:result=json.load(response)
        self.assertTrue(result['dags']['fixture_dag']['proposed']['end'].endswith('03:33:36'))
        self.assertEqual(result['other_starts'],'fixed')
        with self.get('/planner.svg?'+query) as response:self.assertIn('<svg',response.read().decode())
        with self.get('/app?view=planner&plan_dag=fixture_dag&plan_start=invalid') as response:
            body=response.read().decode()  # A bad experiment time is explained, with a way out.
            self.assertIn('role="alert"',body);self.assertIn('Clear experiment',body)
        with self.get('/app?view=planner&plan_dag=fixture_dag&plan_start=05:00') as response:
            self.assertIn('class="gantt-exp-chip"',response.read().decode())
        with self.get('/app?view=overview') as response:response.read()
        with self.get('/app?view=planner') as response:  # The experiment is remembered with its DAG…
            self.assertIn('Experiment 05:00',response.read().decode())
        with self.get('/app?view=planner&plan_dag=fixture_dag') as response:  # …until the DAG is opened without one.
            self.assertNotIn('gantt-exp-chip',response.read().decode())
        with self.get('/app?view=overview') as response:response.read()
        with self.get('/app?view=planner') as response:  # Coming back remembers the last Planner DAG.
            self.assertIn('data-planner-dag="fixture_dag" data-row="selected"',response.read().decode())
        with self.get('/app?view=planner&plan_dag=not_a_dag') as response:
            body=response.read().decode()
            self.assertIn('<h1>Schedule</h1>',body);self.assertIn('role="alert"',body)
        self.assertEqual(len(FakeAirflow.requests),before)
        self.assertEqual(files,{str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')})
    def test_rejected_token_at_core_or_optional_endpoint_keeps_snapshot_and_can_be_replaced(self):
        csrf=self.csrf()
        for rejected_path in ('/api/v2/dags','/api/v2/importErrors'):
            with self.subTest(rejected_path=rejected_path):
                self.collect_live(csrf)
                session=next(s for s in app.SESSIONS.values() if s['csrf']==csrf)
                files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
                FakeAirflow.rejected_path=rejected_path
                with self.post('/refresh',dict(csrf=csrf,return_to='/app?view=health')) as response:response.read()
                body,url=self.wait_collection(csrf)
                self.assertIn('Your Airflow token has expired or is invalid.',body)
                self.assertIn('role="dialog"',body);self.assertIn('view=health',url)
                self.assertNotIn('token',session);self.assertNotIn('integration-test-token',body)
                self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files)
                FakeAirflow.rejected_path=None
                with self.post('/refresh',dict(csrf=csrf,token='integration-test-token',return_to='/app?view=health')) as response:response.read()
                body,url=self.wait_collection(csrf)
                self.assertIn('<h1>Health</h1>',body);self.assertNotIn('role="dialog"',body)
                self.assertNotIn('token_error',session)
    def test_permission_error_does_not_claim_token_expired_or_clear_it(self):
        csrf=self.csrf();self.collect_live(csrf)
        FakeAirflow.rejected_path='/api/v2/dags';FakeAirflow.rejection_status=403
        with self.post('/refresh',dict(csrf=csrf)) as response:response.read()
        body,url=self.wait_collection(csrf)
        self.assertIn('does not have access',body);self.assertNotIn('expired',body)
        session=next(s for s in app.SESSIONS.values() if s['csrf']==csrf)
        self.assertIn('token',session)
    def test_initial_rejected_token_shows_error_and_refresh_cannot_redirect_offsite(self):
        csrf=self.csrf();FakeAirflow.rejected_path='/api/v2/dags'
        body,url=self.collect_live(csrf)
        self.assertIn('Your Airflow token has expired or is invalid.',body)
        self.assertIn('action="/import"',body)
        self.assertEqual(app.refresh_destination('https://example.com/app'),'/app')
        self.assertEqual(app.refresh_destination('//example.com/app'),'/app')
        self.assertEqual(app.refresh_destination('/app?view=lineage&expanded=a&expanded=b'),'/app?view=lineage&expanded=a&expanded=b')
    def test_lineage_logs_read_saved_failed_log_with_paging_and_full_download(self):
        FakeAirflow.task_state='failed'
        csrf=self.csrf();self.collect_live(csrf)
        with self.post('/cached',dict(csrf=csrf,cache=self.live_key())) as response:response.read()
        before=len(FakeAirflow.requests)
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        with self.get('/app?view=lineage&dag=fixture_dag&task=only_task') as response:
            body=response.read().decode()
            self.assertIn('task-log-frame',body);self.assertIn('loading="lazy"',body)
            self.assertIn('frame-src \'self\'',response.headers['Content-Security-Policy'])
            self.assertNotIn('first log line',body)
        query=urlencode(dict(dag='fixture_dag',run='manual__test',task='only_task'))
        with self.get('/task/log?'+query) as response:
            body=response.read().decode()
            self.assertIn('frame-ancestors \'self\'',response.headers['Content-Security-Policy'])
            self.assertIn('Lines 1–200 of 406',body)
            self.assertIn('&lt;script&gt;unsafe()&lt;/script&gt;',body);self.assertNotIn('<script>',body)
            self.assertIn('Attempt 1',body);self.assertIn('Download log',body)
        with self.get('/task/log?'+query+'&page=3') as response:
            body=response.read().decode();self.assertIn('last log line',body);self.assertIn('Lines 401–406',body)
        with self.get('/task/log/download?'+query) as response:
            text=response.read().decode()
            self.assertTrue(text.startswith('first log line'));self.assertTrue(text.endswith('last log line'))
            self.assertEqual(response.headers.get_content_type(),'text/plain')
            self.assertIn('attachment;',response.headers['Content-Disposition'])
        with self.assertRaises(HTTPError) as error:self.get('/task/log?'+query.replace('manual__test','wrong-run'))
        self.assertEqual(error.exception.code,404);error.exception.close()
        self.assertEqual(len(FakeAirflow.requests),before)
        self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files)
    def test_csrf_rejects_invalid_submission(self):
        self.csrf()
        with self.assertRaises(HTTPError) as error:self.post('/import',{'csrf':'invalid'})
        self.assertEqual(error.exception.code,403)
        error.exception.close()
    def test_localhost_published_port_is_allowed_but_foreign_hosts_are_rejected(self):
        for host in ('127.0.0.1:8766','localhost:8766','localhost'):
            with self.client.open(Request(self.base+'/health',headers={'Host':host})) as response:
                self.assertEqual(response.status,200)
        for host in ('example.com:8766','localhost.example.com','localhost@evil.example','127.0.0.1:65536','localhost:0','localhost/path'):
            with self.assertRaises(HTTPError) as error:self.client.open(Request(self.base+'/health',headers={'Host':host}))
            self.assertEqual(error.exception.code,403);error.exception.close()
    def test_native_graph_form_exports_every_checked_group(self):
        csrf=self.csrf()
        self.open_sample(csrf)
        with self.get('/graph.svg?native=1&expanded=persist&expanded=reconcile&task=reconcile.trigger_glue_job') as response:
            svg=response.read().decode()
            self.assertIn('Collapse persist',svg)
            self.assertIn('Collapse reconcile',svg)
            self.assertIn('Deselect reconcile.trigger_glue_job',svg)
            self.assertIn('attachment;',response.headers['Content-Disposition'])
        with self.get('/dependency.svg?view=dependency&focus=dp_reconcile_mobile_financing_daily&depth=direct&zoom=0.85') as response:
            import xml.etree.ElementTree as ET
            root=ET.fromstring(response.read())
            self.assertEqual(response.headers.get_content_type(),'image/svg+xml')
            self.assertIn('dag-dependency.svg',response.headers['Content-Disposition'])
            meta=json.loads(root.find('{http://www.w3.org/2000/svg}metadata').text)
            self.assertEqual(meta['focus'],'dp_reconcile_mobile_financing_daily')
            self.assertEqual(meta['depth'],'direct')
    def test_dependency_find_selects_and_anchors_without_hiding_other_dags(self):
        csrf=self.csrf()
        self.open_sample(csrf)
        from lineage_app.views.dependency import dependency_anchor
        with self.get('/app?view=dependency') as response:baseline=response.read().decode()
        nodes=set(re.findall('data-dag="([^"]+)"',baseline));self.assertGreater(len(nodes),1)
        dag=sorted(nodes)[-1]
        before=len(FakeAirflow.requests)
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        query=urlencode(dict(dag_search=dag.upper(),zoom='0.85',depth='direct',dependency_fullscreen='1',state='failed',tag='no-matching-tag'))
        with self.get('/dependency/find?'+query) as response:
            page=response.read().decode();destination=urlsplit(response.url)
        params=parse_qs(destination.query)
        self.assertEqual(params['focus'],[dag]);self.assertEqual(params['depth'],['direct'])
        self.assertEqual(params['zoom'],['0.85']);self.assertEqual(params['dependency_fullscreen'],['1'])
        self.assertNotIn('q',params);self.assertNotIn('state',params);self.assertNotIn('tag',params)
        self.assertEqual(destination.fragment,dependency_anchor(dag))
        self.assertIn('id="'+destination.fragment+'"',page)
        self.assertEqual(set(re.findall('data-dag="([^"]+)"',page)),nodes)
        self.assertRegex(page,r'class="dependency-select"[^>]*value="'+re.escape(dag)+r'"[^>]*checked')
        self.assertNotIn('name="_dag_jump"',page)
        with self.get('/dependency/find?'+query) as response:
            self.assertNotEqual(parse_qs(urlsplit(response.url).query)['_dag_jump'],params['_dag_jump'])
            response.read()
        for search,message in [('dp_','Choose a DAG'),('no-such-dag','No matching DAG')]:
            with self.get('/dependency/find?'+urlencode(dict(dag_search=search))) as response:
                page=response.read().decode()
                self.assertIn(message,page)
                self.assertEqual(set(re.findall('data-dag="([^"]+)"',page)),nodes)
        with self.get('/app?'+urlencode(dict(view='dependency',q=dag))) as response:
            self.assertEqual(urlsplit(response.url).fragment,dependency_anchor(dag))
            self.assertEqual(set(re.findall('data-dag="([^"]+)"',response.read().decode())),nodes)
        self.assertEqual(len(FakeAirflow.requests),before)
        self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files)
    def test_performance_rankings_cross_filter_without_lineage_or_collection(self):
        from html.parser import HTMLParser
        class Rankings(HTMLParser):
            def __init__(self,html):super().__init__();self.links=[];self.feed(html)
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='a' and attrs.get('data-rank'):self.links.append(attrs)
        def rankings(path):
            with self.get(path) as response:
                self.assertEqual(parse_qs(urlsplit(response.url).query)['view'],['performance'])
                body=response.read().decode();self.assertIn('<h1>Performance</h1>',body)
                return Rankings(body).links
        csrf=self.csrf()
        self.open_sample(csrf)
        before=len(FakeAirflow.requests)
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        baseline=rankings('/app?view=performance')
        dag=next(a for a in baseline if a['data-rank']=='dag')
        scoped=rankings(dag['href'])
        tasks=[a for a in scoped if a['data-rank']=='task']
        self.assertTrue(tasks)
        self.assertTrue(all((a['data-dag'],a['data-run'])==(dag['data-dag'],dag['data-run']) for a in tasks))
        selected=rankings(tasks[0]['href'])
        parents=[a for a in selected if a['data-rank']=='dag']
        self.assertEqual(len(parents),1)
        self.assertEqual((parents[0]['data-dag'],parents[0]['data-run']),(tasks[0]['data-dag'],tasks[0]['data-run']))
        restored=rankings(parents[0]['href'])
        self.assertEqual([(a['data-rank'],a['data-dag'],a['data-run'],a['data-task']) for a in restored],
            [(a['data-rank'],a['data-dag'],a['data-run'],a['data-task']) for a in baseline])
        self.assertEqual(len(FakeAirflow.requests),before)
        self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files)
    def test_sidebar_preference_survives_navigation_reload_and_stateful_graph_requests(self):
        csrf=self.csrf()
        self.open_sample(csrf)
        before=len(FakeAirflow.requests)
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        state=dict(view='lineage',dag='dp_reconcile_mobile_financing_daily',task='reconcile.trigger_glue_job',
            run='scheduled__'+datetime.now(BKK).replace(hour=2,minute=0,second=0,microsecond=0).astimezone(timezone.utc).isoformat(),
            expanded=['persist','reconcile'],zoom='fit',depth='direct',native='1',sidebar='collapsed')
        with self.get('/app?'+urlencode(state,doseq=True)) as response:
            body=response.read().decode()
            self.assertNotIn('sidebar=',response.url)
            self.assertRegex(body,r'id="sidebar-collapsed"[^>]*checked')
            self.assertIn('aria-label="Expand sidebar"',body)
            self.assertIn('form="lineage-state" formaction="/app" name="sidebar"',body)
            self.assertIn('form="lineage-state" name="zoom" value="fit"',body)
            self.assertRegex(body,r'name="expanded"[^>]*value="persist"[^>]*checked')
            self.assertRegex(body,r'name="expanded"[^>]*value="reconcile"[^>]*checked')
            self.assertRegex(body,r'name="task"[^>]*value="reconcile.trigger_glue_job"[^>]*checked')
        for view in ('overview','troubleshooting','dependency','raws','health'):
            with self.get('/app?view='+view) as response:
                body=response.read().decode()
                self.assertRegex(body,r'id="sidebar-collapsed"[^>]*checked')
                self.assertNotIn('<script',body)
        with self.get('/app?view=raws&raw_mode=schema&sidebar=expanded') as response:
            body=response.read().decode()
            self.assertNotRegex(body,r'id="sidebar-collapsed"[^>]*checked')
            self.assertRegex(body,r'id="raw-mode-schema"[^>]*checked')
            self.assertIn('aria-label="Collapse sidebar"',body)
        with self.get('/app?view=overview') as response:
            self.assertNotRegex(response.read().decode(),r'id="sidebar-collapsed"[^>]*checked')
        self.assertEqual(len(FakeAirflow.requests),before)
        self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files)
    def test_api_to_parquet_to_html_and_cached_reopen(self):
        csrf=self.csrf();today=datetime.now(BKK).date().isoformat()
        # Legacy or hand-crafted date inputs cannot widen the collection window.
        fields=dict(csrf=csrf,base=f'http://127.0.0.1:{self.fake.server_port}',token='integration-test-token',stage='uat',start='2000-01-01',end='2099-12-31',baseline='1')
        with self.post('/connect',fields) as response:self.assertIn('Collecting Airflow',response.read().decode())
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            with self.get('/progress') as response:
                text=response.read().decode()
                if response.url.endswith('/app'):break
            time.sleep(.05)
        else:self.fail('Collection did not finish')
        self.assertIn('<h1>Overview</h1>',text)
        self.assertIn('aria-label="Change connection"',text)
        self.assertNotIn('aria-label="Connection"',text)
        self.assertNotIn('aria-label="Disconnect"',text)
        self.assertIn('No failed DAGs today.',text)
        with self.get('/app?view=inventory') as response:
            self.assertIn('Fixture DAG',response.read().decode())
        self.assertIn('Local Parquet',text)
        self.assertNotIn('integration-test-token',text)
        with self.get('/app?view=lineage&dag=fixture_dag&task=only_task') as response:
            graph=response.read().decode();self.assertIn('<svg',graph);self.assertIn('only_task',graph)
            self.assertNotIn('task-log-frame',graph)
        with self.assertRaises(HTTPError) as error:self.get('/task/log?dag=fixture_dag&run=manual__test&task=only_task')
        self.assertEqual(error.exception.code,404);error.exception.close()
        with self.get('/export') as response:
            exported=json.load(response);self.assertEqual(len(exported['monitoring_rows']),2)
        self.assertEqual(exported['metadata']['start'],today)
        self.assertEqual(exported['metadata']['end'],today)
        self.assertTrue(all(r['run_date']==today for r in exported['monitoring_rows']))
        self.assertTrue(list(Path(self.tmp.name).rglob('*.parquet')))
        self.assertNotIn('integration-test-token',''.join(p.read_text() for p in Path(self.tmp.name).rglob('*.json')))
        old_sid,old_session=next((sid,s) for sid,s in app.SESSIONS.items() if s['csrf']==csrf)
        self.assertIn('token',old_session)
        with self.post('/disconnect',{'csrf':csrf}) as response:
            self.assertIn('action="/import"',response.read().decode())
        self.assertNotIn('token',old_session)
        self.assertNotIn(old_sid,app.SESSIONS)
        with self.get('/app') as response:self.assertEqual(urlsplit(response.url).path,'/')
        csrf=self.csrf()
        calls_before=len(FakeAirflow.requests)
        files_before={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        with self.post('/cached',{'csrf':csrf,'cache':self.live_key()}) as response:
            self.assertIn('<h1>Overview</h1>',response.read().decode())
        # Raws ignores unrelated DAG filters and exposes only this snapshot.
        with self.get('/app?view=raws&q=missing-dag&tag=missing-tag') as response:
            raw_page=response.read().decode()
            self.assertIn('<h1>Raws</h1>',raw_page)
            self.assertIn('airflow_dag_inventory',raw_page)
            self.assertNotIn('view=analysis',raw_page)
            self.assertNotIn('view=schema',raw_page)
        for view,title in [('performance','Performance'),('troubleshooting','Troubleshooting'),('health','Health')]:
            with self.get('/app?view='+view) as response:
                body=response.read().decode()
                self.assertIn('<h1>'+title+'</h1>',body)
                self.assertNotIn('<script',body)
        with self.get('/app?view=health') as response:self.assertIn('Healthy',response.read().decode())
        with self.get('/raws/download?table=airflow_dag_runs&format=parquet&date=2000-01-01') as response:
            import pyarrow.parquet as pq
            downloaded=pq.read_table(io.BytesIO(response.read()))
            self.assertEqual(set(downloaded.column('airflow_source').to_pylist()),{f'http://127.0.0.1:{self.fake.server_port}'})
            self.assertEqual(downloaded.drop_columns(['airflow_source']).to_pylist(),
                             pq.read_table(io.BytesIO(self.live_store().raw_file('airflow_dag_runs',datetime.now(BKK).date()))).to_pylist())
            self.assertIn(today,response.headers['Content-Disposition'])
        with self.get('/raws/download?table=airflow_dag_runs&format=json&raw_q=no-match&raw_page=999') as response:
            downloaded=json.load(response)
            self.assertEqual(len(downloaded),1)
            self.assertEqual(downloaded[0]['dag_id'],'fixture_dag')
        with self.get('/raws/download?table=airflow_monitoring_logs&format=schema') as response:
            fields=json.load(response)['fields']  # 85 monitoring columns + airflow_source in the merged view.
            self.assertEqual((len(fields),fields[-1]['name']),(86,'airflow_source'))
        with self.get('/raws/download-all?start=2000-01-01&end=2099-12-31') as response:
            self.assertEqual(response.headers.get_content_type(),'application/zip')
            with zipfile.ZipFile(io.BytesIO(response.read())) as archive:
                manifest=json.loads(archive.read('manifest.json'))
                self.assertEqual(manifest['snapshot_date'],today)
                self.assertEqual(len(manifest['tables']),len(SCHEMAS))
                self.assertNotIn('integration-test-token',''.join(archive.read(p).decode() for p in archive.namelist() if p.endswith('.json')))
        for path in ('/raws/download?table=../metadata.json&format=json','/raws/download?table=airflow_dag_runs&format=exe'):
            with self.assertRaises(HTTPError) as error:self.get(path)
            self.assertEqual(error.exception.code,400);error.exception.close()
        self.assertEqual(len(FakeAirflow.requests),calls_before)
        self.assertEqual({str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')},files_before)
        self.assertTrue(all(method=='GET' for method,path in FakeAirflow.requests))
        self.assertFalse(any('/api/v2/pools' in path or '/api/v2/assets' in path for method,path in FakeAirflow.requests))

if __name__=='__main__':unittest.main()
