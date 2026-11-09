import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from lineage_app.clients.airflow import AirflowClient, APIError
from sample_data import sample_collection
from lineage_app.data.model import BKK, CONTRACT, graph_layout, job_groups, map_dag, map_task, monitoring_rows, trace, window
from lineage_app.data.parquet import ParquetStore,SCHEMAS
from lineage_app.views.render import Workspace, combined_state, connect_page

class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.result=sample_collection(history_days=10)

    def test_groups_require_complete_connected_pairs_and_safe_contraction(self):
        define='job.define_glue_job';trigger='job.trigger_glue_job'
        defs=[{'task_id':n} for n in (define,trigger,'external')]
        self.assertEqual(job_groups(defs,[(define,trigger)]),{'job':(define,trigger)})
        self.assertEqual(job_groups(defs,[]),{})
        self.assertEqual(job_groups(defs+ [{'task_id':'job.other'}],[(define,trigger)]),{})
        self.assertEqual(job_groups(defs,[(define,trigger),(define,'external'),('external',trigger)]),{})
        self.assertEqual(job_groups([{'task_id':'define_glue_job'},{'task_id':'trigger_glue_job'}],[]),{})

    def test_group_state_never_masks_failure_or_unfinished_children(self):
        self.assertEqual(combined_state(['success','failed']),'failed')
        self.assertEqual(combined_state(['success','running']),'running')
        self.assertIsNone(combined_state(['success',None]))
        self.assertEqual(combined_state(['success','success']),'success')

    def test_exact_target_contract(self):
        rows=monitoring_rows(self.result['dag_runs'],self.result['task_runs'],self.result['collected_at'])
        self.assertEqual(len(CONTRACT['columns']),85)
        self.assertTrue(all(list(r)==list(CONTRACT['columns']) for r in rows))
        self.assertEqual(len(rows),len({r['record_id'] for r in rows}))
        self.assertTrue(all(r['source_job_run_id'] is None and not r['has_job_log'] for r in rows))

    def test_bangkok_boundary_and_queued_fallback(self):
        raw=dict(dag_id='a',dag_run_id='r',state='queued',queued_at='2026-10-28T17:00:00Z')
        row=map_dag('uat',raw,{},datetime.now(timezone.utc),'https://airflow.example.com')
        self.assertEqual(str(row['run_date']),'2026-10-29')
        self.assertIsNone(row['started_at'])

    def test_duration_derived_from_real_api_timestamps(self):
        raw=dict(dag_id='a',dag_run_id='r',state='success',start_date='2026-10-29T00:00:00Z',end_date='2026-10-29T00:03:00Z')
        row=map_dag('uat',raw,{},datetime.now(timezone.utc),'https://airflow.example.com')
        self.assertEqual(row['duration_seconds'],180)

    def test_run_window_validation(self):
        with self.assertRaises(ValueError):window('2026-10-29','2026-10-28')
        with self.assertRaises(ValueError):window('not-a-date','2026-10-28')

    def test_baseline_excludes_same_day_and_requires_three_samples(self):
        result=self.result
        rows=monitoring_rows(result['dag_runs'],result['task_runs'],result['collected_at'])
        latest=[r for r in rows if r['record_level']=='dag' and r['run_date']==result['end']]
        self.assertTrue(all(r['dag_benchmark_samples']==7 for r in latest))
        earliest=[r for r in rows if r['record_level']=='dag' and r['run_date']==result['end']-timedelta(days=10)]
        self.assertTrue(all(r['dag_avg_duration_7d_seconds'] is None for r in earliest))
        self.assertTrue(all(r['dag_duration_seconds'] is None for r in latest if r['dag_state']=='running'))

    def test_mapped_task_identities_are_distinct(self):
        result=self.result
        run=result['dag_runs'][0];task=result['task_runs'][0]
        other={**task,'map_index':0}
        rows=monitoring_rows([run],[task,other],result['collected_at'])
        self.assertEqual(len(rows),3)
        self.assertEqual(len({r['record_id'] for r in rows}),3)

    def test_latest_state_across_dates(self):
        run=copy.deepcopy(self.result['dag_runs'][0])
        updated={**run,'collected_at':run['collected_at']+timedelta(seconds=1),'run_date':run['run_date']+timedelta(days=1),'state':'failed'}
        rows=monitoring_rows([run,updated],[],updated['collected_at'])
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['dag_state'],'failed')
        self.assertEqual(rows[0]['run_date'],updated['run_date'])

    def test_graph_preserves_isolated_tasks_and_traces_cycles(self):
        definitions=[dict(task_id=t) for t in ('a','b','c','isolated')]
        pos,up,down,cycles=graph_layout(definitions,[('a','b'),('b','c')])
        self.assertEqual(set(pos),{'a','b','c','isolated'})
        self.assertEqual(trace('a',down),{'b','c'})
        self.assertEqual(trace('a',down,False),{'b'})
        self.assertFalse(cycles)
        _,_,down,cycles=graph_layout(definitions,[('a','b'),('b','a')])
        self.assertEqual(cycles,{'a','b'})
        self.assertEqual(trace('a',down),{'b'})

class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=ParquetStore(self.tmp.name)
        self.result=sample_collection()
    def tearDown(self):self.tmp.cleanup()

    def test_daily_merge_and_monthly_partition(self):
        row=self.result['dag_runs'][0];day=row['run_date']
        self.store.write_day('airflow_dag_runs',day,[row],True)
        self.store.write_day('airflow_dag_runs',day,[{**row,'state':'failed'}],True)
        rows=self.store.read('airflow_dag_runs')
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['state'],'failed')
        self.assertIn(f'prti_dttm={day:%Y%m}',str(self.store.path('airflow_dag_runs',day)))
        self.assertNotIn('prti_dttm',rows[0])

    def test_snapshot_replaces_and_preserves_history(self):
        day=self.result['snapshot_date'];edges=self.result['dependencies'][:3]
        self.store.write_day('airflow_dag_dependencies',day,edges)
        prior=day-timedelta(days=1)
        self.store.write_day('airflow_dag_dependencies',prior,[{**r,'snapshot_date':prior} for r in edges])
        self.store.write_day('airflow_dag_dependencies',day,edges[:1])
        self.assertEqual(len(self.store.read('airflow_dag_dependencies',day,day)),1)
        self.assertEqual(len(self.store.read('airflow_dag_dependencies',prior,prior)),3)

    def test_atomic_failure_preserves_previous_file(self):
        day=self.result['snapshot_date'];edges=self.result['dependencies'][:3]
        self.store.write_day('airflow_dag_dependencies',day,edges)
        with patch('lineage_app.data.parquet.os.replace',side_effect=OSError('Simulated interrupted write')):
            with self.assertRaises(OSError):self.store.write_day('airflow_dag_dependencies',day,[])
        self.assertEqual(len(self.store.read('airflow_dag_dependencies')),3)
        self.assertFalse(list(Path(self.tmp.name).rglob('*.tmp')))

    def test_full_storage_round_trip_and_no_token(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        self.assertEqual(len(data['rows']),70)
        self.assertEqual(len(data['inventory']),8)
        self.assertEqual(len(list(Path(self.tmp.name).rglob('*.parquet'))),len(SCHEMAS))
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        self.assertEqual(len(self.store.load()['rows']),70)
        self.assertNotIn('token',json.dumps(self.store.metadata()))

    def test_html_views_read_parquet_and_escape_api_text(self):
        self.result['inventory'][0]['dag_display_name']='<script>alert(1)</script>'
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        for view in ('overview','lineage','timeline','inventory','analysis','schema'):
            result=Workspace(data,{'view':view},'csrf','cached').render()
            self.assertNotIn('<script',result)
            self.assertNotIn('onclick=',result)
            self.assertNotIn('javascript:',result)
            self.assertNotIn('class="source-badge"',result)
        graph=Workspace(data,{'view':'lineage','dag':self.result['inventory'][0]['dag_id'],'task':'reconcile.trigger_glue_job'},'csrf','cached').graph_svg()[0]
        self.assertIn('reconcile.trigger_glue_job',graph)
        import xml.etree.ElementTree as ET
        ET.fromstring(graph)

    def test_views_cannot_expand_beyond_snapshot_day(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load();day=self.result['snapshot_date']
        previous=day-timedelta(days=1)
        old={**data['rows'][0],'run_date':previous,'run_id':'old-run'}
        self.store.write_day('airflow_monitoring_logs',previous,[old])
        self.assertTrue(self.store.path('airflow_monitoring_logs',previous).exists())
        self.assertTrue(all(r['run_date']==day for r in self.store.load()['rows']))
        data['rows'].append(old)
        params={'start':'2000-01-01','end':'2099-12-31','run':'old-run','baseline':'1'}
        for view in ('overview','lineage','timeline','inventory','analysis','schema'):
            workspace=Workspace(data,{**params,'view':view},'csrf','cached')
            self.assertEqual({r['run_date'] for r in workspace.rows},{day})
            self.assertNotEqual(workspace.run['run_id'],'old-run')
            self.assertNotIn('start=',workspace.url())
            self.assertNotIn('end=',workspace.url())
            rendered=workspace.render()
            self.assertNotIn('type="date"',rendered)
            self.assertNotIn('7-day average',rendered)
        connection=connect_page('csrf')
        self.assertNotIn('Collection options',connection)
        self.assertNotIn('Token stays in memory',connection)
        self.assertNotIn('type="date"',connection)

    def test_timeline_is_one_bangkok_day(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        workspace=Workspace(self.store.load(),{'view':'timeline'},'csrf','cached')
        rendered=workspace.render()
        for hour in ('00:00','06:00','12:00','18:00','24:00'):
            self.assertIn(f'<span>{hour}</span>',rendered)

    def test_timetable_places_each_run_once_in_vertical_time_rows(self):
        from lineage_app.views.timeline import timetable
        from html.parser import HTMLParser
        from urllib.parse import parse_qs,urlsplit
        class Table(HTMLParser):
            def __init__(self):super().__init__();self.slot=None;self.cards=[];self.time_headers=[]
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='tr' and 'data-slot' in attrs:self.slot=int(attrs['data-slot'])
                if tag=='th' and attrs.get('scope')=='row':self.time_headers.append(self.slot)
                if tag=='a' and 'timetable-card ' in attrs.get('class',''):self.cards.append((self.slot,attrs))
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load();day=self.result['snapshot_date']
        w=Workspace(data,{'view':'timeline','fullscreen':'1'},'csrf','cached')
        first=w.runs[0]
        # A run that has not started still belongs in the queued-time row.
        queued={**first,'run_id':'queued-only','dag_state':'queued','dag_started_at_bkk':None,
            'dag_completed_at_bkk':None,'dag_duration_seconds':None,'dag_queued_at_bkk':datetime.combine(day,datetime.min.time()).replace(hour=6,minute=47)}
        w.runs.append(queued)
        # Guard both day boundaries even if a caller supplies an old/future row.
        w.runs.extend([{**first,'run_id':'out-of-day-'+str(offset),'dag_started_at_bkk':datetime.combine(day+timedelta(days=offset),datetime.min.time())} for offset in (-1,1)])
        table=Table();html=timetable(w);table.feed(html)
        self.assertEqual(table.time_headers,list(range(0,1441,30)))
        expected={(r['dag_id'],r['run_id']):r for r in w.runs if (r['dag_started_at_bkk'] or r['dag_queued_at_bkk']).date()==day}
        self.assertEqual(len(table.cards),len(expected))
        self.assertEqual({(a['data-dag'],a['data-run']) for slot,a in table.cards},set(expected))
        for slot,attrs in table.cards:
            row=expected[(attrs['data-dag'],attrs['data-run'])]
            when=row['dag_started_at_bkk'] or row['dag_queued_at_bkk']
            self.assertLessEqual(slot,when.hour*60+when.minute)
            self.assertLess(when.hour*60+when.minute,slot+30)
            params=parse_qs(urlsplit(attrs['href']).query)
            self.assertEqual(params['run'],[row['run_id']])
            self.assertEqual(params['dag'],[row['dag_id']])
            self.assertNotIn('fullscreen',params)
        self.assertIn('Queued 06:47',html)
        self.assertIn('Persist Data Layer',html)
        self.assertNotIn('timeline-bar',html)

    def test_timetable_task_mode_preserves_mapped_instances_and_selected_run(self):
        from html.parser import HTMLParser
        class Cards(HTMLParser):
            def __init__(self):super().__init__();self.cards=[]
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='a' and 'timetable-card ' in attrs.get('class',''):self.cards.append(attrs)
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        w=Workspace(self.store.load(),{'view':'timeline','scope':'tasks','dag':self.result['inventory'][0]['dag_id']},'csrf','cached')
        mapped={**w.instances[0],'map_index':9}
        w.instances.append(mapped)
        html=w.render_timeline();parsed=Cards();parsed.feed(html)
        self.assertEqual(len(parsed.cards),len([t for t in w.instances if t.get('task_started_at_bkk') or t.get('task_queued_at_bkk')]))
        self.assertTrue(all(a['data-run']==w.run['run_id'] for a in parsed.cards))
        self.assertIn('Map 9',html)
        self.assertIn('Success</strong>',html)

    def test_timetable_groups_use_only_layer_tags_and_never_dag_names(self):
        from lineage_app.views.timeline import display_labels
        self.assertEqual(display_labels({'dag_id':'dag_a','tags':['persist']})['group'],'Persist Data Layer')
        self.assertEqual(display_labels({'dag_id':'dag_a','tags':[{'name':' Layer: Curate '}]})['group'],'Curate Data Layer')
        self.assertEqual(display_labels({'dag_id':'dp_unknown_persist_daily','tags':['team:dp','project:persist','stakeholder:risk']})['group'],'Other')
        self.assertEqual(display_labels({'dag_id':'dag_a','tags':[]})['group'],'Other')
        self.assertEqual(display_labels({'dag_id':'dag_a','tags':['layer:bronze']})['group'],'bronze')
        self.assertEqual(display_labels({'dag_id':'dag_a','tags':['persist','layer:curate']})['group'],'Curate Data Layer')
        for tags in (['persist','curate'],['curate','persist'],['layer:curate','layer:persist']):
            self.assertEqual(display_labels({'dag_id':'dag_a','tags':tags})['group'],'Other')

    def test_clicking_selected_task_again_deselects_without_changing_context(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        dag_id=self.result['inventory'][0]['dag_id']
        from urllib.parse import parse_qs,urlsplit
        import xml.etree.ElementTree as ET
        def target(workspace,action,task):
            root=ET.fromstring(workspace.graph_svg()[0])
            anchor=next(a for a in root.findall('{http://www.w3.org/2000/svg}a')
                if a.get('aria-label')==action+' '+task)
            return {k:v[-1] for k,v in parse_qs(urlsplit(anchor.get('href')).query).items()}
        original=Workspace(data,{'view':'lineage','dag':dag_id,'depth':'direct',
            'zoom':'1.15','tag':'daily'},'csrf','cached')
        selected=Workspace(data,target(original,'Inspect','load_config'),'csrf','cached')
        self.assertEqual(selected.task_id,'load_config')
        self.assertIn('fill="#e9fbf9"',selected.graph_svg()[0])
        deselect_params=target(selected,'Deselect','load_config')
        self.assertNotIn('task',deselect_params)
        deselected=Workspace(data,deselect_params,'csrf','cached')
        self.assertEqual(deselected.task_id,'')
        self.assertEqual(deselected.run['run_id'],original.run['run_id'])
        self.assertEqual(deselected.dag_id,original.dag_id)
        for key in ('depth','zoom','tag'):
            self.assertEqual(deselected.params[key],original.params[key])
        self.assertNotIn('fill="#e9fbf9"',deselected.graph_svg()[0])
        self.assertNotIn('Clear</button>',deselected.render_lineage())
        other=Workspace(data,target(selected,'Inspect','validate_results'),'csrf','cached')
        self.assertEqual(other.task_id,'validate_results')

    def test_group_expand_select_deselect_and_collapse_preserve_context(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        from urllib.parse import parse_qs,urlsplit
        import xml.etree.ElementTree as ET
        def root(workspace):return ET.fromstring(workspace.graph_svg()[0])
        def follow(workspace,label):
            anchor=next(a for a in root(workspace).iter('{http://www.w3.org/2000/svg}a') if a.get('aria-label')==label)
            params={k:v[-1] for k,v in parse_qs(urlsplit(anchor.get('href')).query).items()}
            return Workspace(data,params,'csrf','cached')
        def labels(workspace):return {a.get('aria-label') for a in root(workspace).iter('{http://www.w3.org/2000/svg}a')}
        original=Workspace(data,{'view':'lineage','dag':self.result['inventory'][0]['dag_id'],
            'depth':'direct','zoom':'1.15','tag':'daily'},'csrf','cached')
        self.assertIn('Expand reconcile',labels(original))
        self.assertNotIn('Inspect reconcile.define_glue_job',labels(original))
        expanded=follow(original,'Expand reconcile')
        self.assertIn('Inspect reconcile.define_glue_job',labels(expanded))
        self.assertIn('Inspect reconcile.trigger_glue_job',labels(expanded))
        self.assertIn('Expand persist',labels(expanded))
        self.assertEqual(len(root(original).findall('{http://www.w3.org/2000/svg}a')),7)
        # Every original connection remains visible except the hidden internal pairs.
        edges=lambda w:len(root(w).findall('{http://www.w3.org/2000/svg}path'))
        self.assertEqual(edges(expanded),edges(original)+1)
        selected=follow(expanded,'Inspect reconcile.trigger_glue_job')
        deselected=follow(selected,'Deselect reconcile.trigger_glue_job')
        self.assertIn('Collapse reconcile',labels(deselected))
        collapsed=follow(selected,'Collapse reconcile')
        self.assertFalse(collapsed.task_id)
        self.assertIn('Expand reconcile',labels(collapsed))
        self.assertEqual(collapsed.run['run_id'],original.run['run_id'])
        for key in ('depth','zoom','tag'):self.assertEqual(collapsed.params[key],original.params[key])
        # Deep links to a task automatically open its group.
        linked=Workspace(data,{**original.params,'task':'persist.trigger_glue_job'},'csrf','cached')
        self.assertIn('Deselect persist.trigger_glue_job',labels(linked))
        self.assertEqual(linked.graph_svg()[1:3],original.graph_svg()[1:3])

    def test_live_graph_bounds_and_node_positions_stay_fixed_for_all_selection_states(self):
        from html.parser import HTMLParser
        from lineage_app.views.lineage import live_lineage
        class Graph(HTMLParser):
            def __init__(self):super().__init__();self.nodes={};self.bounds=None;self.inputs=[];self.links=[]
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='svg' and 'live-svg' in attrs.get('class',''):self.bounds=tuple(attrs.get(k) for k in ('width','height','viewbox'))
                if 'data-task' in attrs:self.nodes[attrs['data-task']]=attrs['style']
                if 'data-group' in attrs:self.nodes[attrs['data-group']]=attrs['style']
                if tag=='input':self.inputs.append(attrs)
                if tag=='a':self.links.append(attrs)
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load();base={'view':'lineage','dag':self.result['inventory'][0]['dag_id'],'zoom':'fit'}
        states=[{}, {'expanded':'persist'}, {'expanded':'persist,reconcile','fullscreen':'1'},
            {'expanded':'persist,reconcile','task':'reconcile.trigger_glue_job'}]
        graphs=[]
        for state in states:
            w=Workspace(data,{**base,**state},'csrf','cached')
            drawing,panels,form=live_lineage(w)
            graph=Graph();graph.feed(drawing);graphs.append(graph)
            self.assertEqual(graph.links,[])  # Native controls cannot navigate or jump to anchors.
            self.assertNotIn('<script',drawing+panels+form)
            self.assertEqual(len([i for i in graph.inputs if i.get('class')=='task-select']),9)
            self.assertTrue(all(i.get('form')=='lineage-state' for i in graph.inputs))
            self.assertIn('name="native" value="1"',form)
            self.assertNotIn('name="fullscreen"',form)  # Only the actual toggle submits this state.
        for graph in graphs[1:]:
            self.assertEqual(graph.bounds,graphs[0].bounds)
            self.assertEqual(graph.nodes,graphs[0].nodes)
        selected=[i for i in graphs[-1].inputs if 'checked' in i]
        self.assertEqual({i['value'] for i in selected},{'persist','reconcile','reconcile.trigger_glue_job'})
        full=Workspace(data,{**base,'fullscreen':'1'},'csrf','cached').render_lineage()
        self.assertIn('id="lineage-fullscreen" name="fullscreen" value="1" form="lineage-state" aria-label="Full screen lineage" autocomplete="off" checked',full)

    def test_detail_relationship_lists_follow_depth_and_exclude_unrelated_branches(self):
        from html.parser import HTMLParser
        from urllib.parse import parse_qs,urlsplit
        class Relations(HTMLParser):
            def __init__(self):super().__init__();self.related={};self.direct=set()
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='a' and 'lineage-relation' in attrs.get('class','').split():
                    task=parse_qs(urlsplit(attrs['href']).query)['task'][0]
                    self.related[task]=attrs.get('class','')
                    if 'lineage-relation-indirect' not in attrs.get('class','').split():self.direct.add(task)
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load();base={'view':'lineage','dag':self.result['inventory'][0]['dag_id'],'task':'reconcile.trigger_glue_job'}
        w=Workspace(data,{**base,'depth':'direct'},'csrf','cached')
        _,up,down,_=graph_layout(w.definitions,w.edges)
        direct=Relations();direct.feed(w.render_detail(up,down))
        self.assertEqual(set(direct.related),{'reconcile.define_glue_job','validate_results'})
        w.params['depth']='full'
        full=Relations();full.feed(w.render_detail(up,down))
        expected={'start_pipeline','load_config','reconcile.define_glue_job','validate_results','publish_dataset','end_pipeline'}
        self.assertEqual(set(full.related),expected)
        self.assertNotIn('persist.trigger_glue_job',full.related)
        # Native radio changes filter pre-rendered chips without replacing the graph.
        native=Relations();html=w.render_detail(up,down,native=True);native.feed(html)
        self.assertEqual(set(native.related),expected)
        self.assertEqual(native.direct,set(direct.related))
        self.assertIn('relation-count-all',html);self.assertIn('relation-count-direct',html)
        from lineage_app.views.lineage import live_lineage
        drawing,panels,form=live_lineage(w)
        self.assertIn('lineage-relation-indirect',panels)
        self.assertNotIn('<script',drawing+panels+form)

    def test_dag_rail_always_exits_fullscreen_and_resets_group_context(self):
        from html.parser import HTMLParser
        from urllib.parse import parse_qs,urlsplit
        class Rail(HTMLParser):
            def __init__(self):super().__init__();self.links=[]
            def handle_starttag(self,tag,attrs):
                attrs=dict(attrs)
                if tag=='a' and 'dag-rail-item' in attrs.get('class',''):self.links.append(attrs['href'])
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        w=Workspace(data,{'view':'lineage','dag':self.result['inventory'][0]['dag_id'],
            'fullscreen':'1','expanded':'persist','task':'persist.define_glue_job'},'csrf','cached')
        rail=Rail();rail.feed(w.render_lineage())
        self.assertEqual(len(rail.links),len(w.inventory))
        for link in rail.links:
            params={k:v[-1] for k,v in parse_qs(urlsplit(link).query).items()}
            self.assertNotIn('fullscreen',params)
            self.assertNotIn('expanded',params)
            self.assertNotIn('task',params)
            destination=Workspace(data,params,'csrf','cached').render_lineage()
            self.assertNotIn('autocomplete="off" checked',destination)

    def test_native_form_does_not_reopen_a_collapsed_selected_group(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        w=Workspace(self.store.load(),{'view':'lineage','dag':self.result['inventory'][0]['dag_id'],
            'native':'1','task':'reconcile.trigger_glue_job','expanded':'persist'},'csrf','cached')
        self.assertFalse(w.task_id)
        self.assertNotIn('task',w.params)

    def test_stale_notices_do_not_hide_real_collection_failures(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        data['inventory'][0]['is_stale']=True
        stale_id=data['inventory'][0]['dag_id']
        data['metadata']['warnings']=['Stale DAG: '+stale_id]
        rendered=Workspace(data,{},'csrf','live').render()
        self.assertNotIn('Snapshot incomplete',rendered)
        self.assertIn('<summary>1 stale DAG</summary>',rendered)
        self.assertIn('snapshot-notice',rendered)
        data['metadata']['warnings'].append('Task definitions unavailable: current_dag')
        rendered=Workspace(data,{},'csrf','live').render()
        self.assertIn('Snapshot incomplete · 1',rendered)
        self.assertIn('Task definitions unavailable: current_dag',rendered)
        self.assertIn('<summary>1 stale DAG</summary>',rendered)

    def test_overview_lists_all_latest_failed_dags_and_airflow_links(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        failed=next(r for r in data['rows'] if r['record_level']=='dag' and r['dag_state']=='failed')
        inventory=data['inventory'][0]
        data['rows']=[];data['inventory']=[]
        for i in range(14):
            dag_id=f'failed_dag_{i:02d}'
            data['rows'].append({**failed,'dag_id':dag_id,'dag_run_url':f'https://airflow.example.com/dags/{dag_id}/runs/scheduled__2026-10-28T18:00:00%2B00:00/'})
            data['inventory'].append({**inventory,'dag_id':dag_id,'dag_display_name':dag_id})
        data['inventory'].append({**inventory,'dag_id':'recovered','dag_display_name':'recovered'})
        data['rows'].extend([{**failed,'dag_id':'recovered'},
            {**failed,'dag_id':'recovered','run_id':'retry-success','dag_state':'success',
             'dag_started_at_bkk':failed['dag_started_at_bkk']+timedelta(hours=1)}])
        workspace=Workspace(data,{},'csrf','live')
        self.assertEqual(workspace.view,'overview')
        table=workspace.render_overview()
        self.assertEqual(table.count('<tr>'),15)
        self.assertIn('failed_dag_13',table)
        self.assertNotIn('recovered',table)
        self.assertIn('https://airflow.example.com/dags/failed_dag_13/runs/scheduled__2026-10-28T18:00:00%2B00:00/',table)
        self.assertIn('overview-table',table)
        self.assertNotIn('Attention',table)
        self.assertNotIn('Today’s runs',table)
        self.assertIn('<th>DAG run</th>',table)
        self.assertEqual(table.count('class="lineage-row-link"'),14)
        self.assertIn('view=lineage',table)
        self.assertIn('run=',table)
        self.assertIn('>→</a></td></tr>',table)
        for name in ('Started at','Ended at','Run type','Failed tasks'):
            self.assertIn('<th>'+name+'</th>',table)

    def test_overview_task_counts_match_latest_run_not_older_failures(self):
        self.store.save_collection(self.result,'https://airflow.example.com','uat')
        data=self.store.load()
        failed=next(r for r in data['rows'] if r['record_level']=='dag' and r['dag_state']=='failed')
        related=[r for r in data['rows'] if r['record_level']=='task' and r['dag_id']==failed['dag_id'] and r['run_id']==failed['run_id']]
        self.assertEqual(len(related),9)
        data['rows'].append({**related[0],'run_id':'older-run','task_state':'failed'})
        table=Workspace(data,{},'csrf','cached').render_overview()
        self.assertIn('<span class="failed-task-count">1</span><span class="muted"> / 9</span>',table)

class PaginationTests(unittest.TestCase):
    def client(self):return AirflowClient('https://airflow.example.com','test-token','uat')

    def test_collects_all_pages(self):
        c=self.client();rows=[{'dag_id':str(i)} for i in range(205)]
        c.get=lambda path,**p:{'dags':rows[p['offset']:p['offset']+p['limit']],'total_entries':len(rows)}
        self.assertEqual(len(c.pages('/api/v2/dags','dags',lambda r:r['dag_id'])),205)

    def test_repeated_or_incomplete_pages_fail(self):
        c=self.client();c.get=lambda path,**p:{'dags':[{'dag_id':'a'}],'total_entries':2}
        with self.assertRaises(ValueError):c.pages('/api/v2/dags','dags',lambda r:r['dag_id'])
        c.get=lambda path,**p:{'dags':[],'total_entries':2}
        with self.assertRaises(ValueError):c.pages('/api/v2/dags','dags',lambda r:r['dag_id'])

    def test_token_and_url_validation(self):
        for base in ('http://example.com','https://user:password@example.com','https://example.com/api/v2'):
            with self.assertRaises(ValueError):AirflowClient(base,'x','uat')
        with self.assertRaises(ValueError):AirflowClient('https://example.com','x\ny','uat')
        c=AirflowClient('https://example.com','Bearer abc','uat')
        self.assertEqual(c.token,'abc');c.clear();self.assertEqual(c.token,'')

    def test_collector_current_graph_and_map_instances(self):
        now=datetime.now(BKK)
        start=now.replace(hour=0,minute=10,second=0,microsecond=0)
        dag={'dag_id':'demo','dag_display_name':'Demo','tags':[],'owners':[],'timezone':'Asia/Bangkok'}
        run={'dag_id':'demo','dag_run_id':'run1','state':'success','run_type':'manual',
             'start_date':start.isoformat(),'end_date':(start+timedelta(minutes=10)).isoformat()}
        task={'dag_id':'demo','dag_run_id':'run1','task_id':'isolated','map_index':-1,'try_number':1,
            'state':'success','start_date':start.isoformat(),'end_date':(start+timedelta(minutes=1)).isoformat()}
        calls=[]
        def get(path,**params):
            calls.append((path,params))
            if path=='/api/v2/dags':return {'dags':[dag,dict(dag_id='old_dag',is_stale=True)],'total_entries':2}
            if path.endswith('/details'):return dag
            if path.endswith('/tasks'):return {'tasks':[dict(task_id='isolated',downstream_task_ids=[])],'total_entries':1}
            if path=='/api/v2/dags/~/dagRuns':return {'dag_runs':[] if params.get('state')=='queued' else [run],'total_entries':0 if params.get('state')=='queued' else 1}
            if path.endswith('/taskInstances'):return {'task_instances':[task,{**task,'map_index':0}],'total_entries':2}
            if path in ('/api/v2/monitor/health','/api/v2/importErrors'):raise APIError(403,path)
            if path.endswith('/tries') or '/logs/' in path:raise APIError(404,path)
            raise AssertionError(path)
        c=self.client();c.get=get
        result=c.collect(collected_at=now)
        self.assertEqual(len(result['dag_runs']),1)
        self.assertEqual(len(result['task_runs']),2)
        self.assertEqual(len(result['definitions']),1)
        self.assertEqual(result['warnings'],[])
        self.assertEqual(result['coverage']['stale_dags'],1)
        self.assertFalse(any('/old_dag/' in path for path,params in calls))
        self.assertIsNone(result['dependencies'][0]['upstream_task_id'])
        self.assertTrue(all(path.startswith('/api/v2/') for path,params in calls))
        date_params=next(params for path,params in calls if 'start_date_gte' in params)
        self.assertIn('start_date_lte',date_params)
        self.assertNotIn('start_date_lt',date_params)

    def test_bangkok_connect_day_boundaries_and_old_runs_excluded(self):
        clicked_at=datetime(2026,10,28,17,0,tzinfo=timezone.utc)
        c=self.client();calls=[]
        dag={'dag_id':'a','is_stale':False}
        def get(path,**params):
            calls.append((path,params))
            if path=='/api/v2/dags':return dict(dags=[dag],total_entries=1)
            if path.endswith('/details'):return dag
            if path.endswith('/tasks'):return dict(tasks=[],total_entries=0)
            if path=='/api/v2/dags/~/dagRuns':
                if params.get('state')=='queued':return dict(dag_runs=[],total_entries=0)
                rows=[dict(dag_id='a',dag_run_id=str(i),state='success',run_type='manual',start_date=t)
                    for i,t in enumerate(('2026-10-28T16:59:59Z','2026-10-28T17:00:00Z','2026-10-29T17:00:00Z'))]
                return dict(dag_runs=rows,total_entries=3)
            if path.endswith('/taskInstances'):return dict(task_instances=[],total_entries=0)
            if path in ('/api/v2/monitor/health','/api/v2/importErrors'):raise APIError(403,path)
            raise AssertionError(path)
        c.get=get
        result=c.collect(collected_at=clicked_at)
        self.assertEqual(str(result['start']),'2026-10-29')
        self.assertEqual(result['start'],result['end'])
        self.assertEqual(result['snapshot_date'],result['start'])
        self.assertEqual([r['run_id'] for r in result['dag_runs']],['1'])
        query=next(p for path,p in calls if 'start_date_gte' in p)
        self.assertEqual(query['start_date_gte'],'2026-10-28T17:00:00+00:00')
        self.assertEqual(query['start_date_lte'],'2026-10-29T17:00:00+00:00')

if __name__=='__main__':unittest.main()
