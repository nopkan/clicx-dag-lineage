"""Dependency topology, latest-run identity and native interaction contracts."""
import json
import re
import tempfile
import unittest
from datetime import timedelta
from html.parser import HTMLParser
from pathlib import Path
from xml.sax.saxutils import escape
from urllib.parse import parse_qs,urlsplit

from sample_data import sample_collection
from lineage_app.views.dependency import dependency_model,dependency_timetable,dependency_svg,find_dependency,dependency_anchor,CARD_HEIGHT,CARD_WIDTH,HEADER_HEIGHT
from lineage_app.data.parquet import ParquetStore
from lineage_app.views.render import Workspace

class Markup(HTMLParser):
    def __init__(self):super().__init__();self.cards=[];self.inputs=[];self.paths=[];self.links=[];self.row_slots=[];self.buttons=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='div' and 'dependency-card ' in attrs.get('class',''):self.cards.append(attrs)
        if tag=='input':self.inputs.append(attrs)
        if tag=='button':self.buttons.append(attrs)
        if tag=='path' and 'data-upstream' in attrs:self.paths.append(attrs)
        if tag=='a':self.links.append(attrs)
        if tag=='tr' and 'data-slot' in attrs:self.row_slots.append(int(attrs['data-slot']))

class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        store=ParquetStore(Path(self.tmp.name)/'uat')
        self.collection=sample_collection()
        store.save_collection(self.collection,'https://airflow.example.com','uat')
        self.data=store.load()
    def workspace(self,**params):return Workspace(self.data,{'view':'dependency',**params},'csrf','cached')
    def test_airflow_links_are_drawn_with_their_evidence_and_unusable_links_are_skipped(self):
        from lineage_app.views.dependency import dependency_model
        link=lambda kind,a,b,**k:dict(dict(link_kind=kind,evidence='dependency_graph',upstream_dag_id=a,downstream_dag_id=b,resolution='same_airflow'),**k)
        self.data['dag_links']=[link('trigger','dp_customer_consent_sync','dp_load_lending_daily'),
            link('trigger','dp_customer_consent_sync','dp_load_lending_daily',evidence='rendered_fields'),
            link('asset',None,'dp_load_lending_daily',resolution='unknown'),
            link('sensor','{{ params.x }}','dp_load_lending_daily',resolution='templated'),
            link('tag','dp_monthly_finance_report','dp_load_lending_daily',evidence='dag_tags')]
        model=dependency_model(self.workspace())
        self.assertEqual(model['kinds'][('dp_customer_consent_sync','dp_load_lending_daily')],['trigger'])
        self.assertEqual(model['up']['dp_load_lending_daily']-{'dp_ingest_core_banking'},{'dp_customer_consent_sync'})
        html=dependency_timetable(self.workspace(focus='dp_load_lending_daily'))
        self.assertRegex(html,r'class="dependency-edge [^"]* dependency-edge-trigger" data-upstream="dp_customer_consent_sync" data-downstream="dp_load_lending_daily" data-link="trigger"')
        self.assertIn('<small class="dependency-link-kind">Trigger</small>',html)
        self.assertIn('class="kind-asset">Asset</span>',html)

    def test_details_box_opens_away_from_the_card_and_can_swap_or_hide(self):
        import re
        from lineage_app.views.dependency import dependency_model
        model=dependency_model(self.workspace())
        right_half=[d for d in model['nodes'] if model['positions'][d][0]+170/2>model['width']/2]
        self.assertTrue(right_half)
        html=dependency_timetable(self.workspace())
        rules=[r for r in re.findall(r'\.dependency-panel:has\(([^)]*)\) \.dependency-sidebar\{inset:12px auto 12px 12px\}',html) if 'swap' not in r]
        self.assertEqual(len(rules),1)
        self.assertEqual(len(rules[0].split(',')),len(right_half))
        self.assertIn('.dependency-panel:has(#dependency-panel-swap:checked) .dependency-sidebar{inset:12px auto 12px 12px}',html)
        for control in ('dependency-panel-swap','dependency-panel-hide'):
            self.assertRegex(html,f'id="{control}" name="[a-z_]+" value="[a-z0-9]+" form="dependency-state"')
        kept=dependency_timetable(self.workspace(panel_side='swap',panel_hidden='1'))  # Survives zoom/Fit reloads.
        self.assertIn('id="dependency-panel-swap" name="panel_side" value="swap" form="dependency-state" aria-label="Move details to the other side" autocomplete="off" checked',kept)
        self.assertIn('id="dependency-panel-hide" name="panel_hidden" value="1" form="dependency-state" aria-label="Hide DAG details" autocomplete="off" checked',kept)

    def test_risk_team_dags_are_split_by_team_after_other(self):
        from lineage_app.views.dependency import dependency_model
        ran=sorted({r['dag_id'] for r in self.workspace().runs})
        risk_colon,risk_dot,risk_packed,risk_none=ran[:4]
        for d in self.data['inventory']:
            d['airflow_source']='Data Platform'
            if d['dag_id']==risk_colon:d.update(airflow_source='Risk Team',tags=['team:risk_de',' project:data_mart'])
            if d['dag_id']==risk_dot:d.update(airflow_source='Risk Team',tags=['cortex.team','data'])
            if d['dag_id']==risk_packed:d.update(airflow_source='Risk Team',tags=['team:risk_de, project:batch, frequency:adhoc'])
            if d['dag_id']==risk_none:d.update(airflow_source='Risk Team',tags=[' project:tableau'])
            if d['dag_id']=='dp_load_lending_daily':d['tags']=[]
        w=self.workspace();model=dependency_model(w)
        self.assertEqual(model['nodes'][risk_colon]['group'],'Risk · risk_de')
        self.assertEqual(model['nodes'][risk_dot]['group'],'Risk · cortex')
        self.assertEqual(model['nodes'][risk_packed]['group'],'Risk · risk_de')
        self.assertEqual(model['nodes'][risk_none]['group'],'Other')
        # One Other column for both Airflows, after the Risk teams.
        self.assertEqual(model['groups'][-3:],['Risk · cortex','Risk · risk_de','Other'])
        self.assertEqual(model['groups'].count('Other'),1)
        html=dependency_timetable(w)
        self.assertIn('title="Risk · risk_de"><strong>risk_de</strong><span>Risk · 2 DAGs</span>',html)
        self.assertIn('class="timetable-group group-6" title="Other"><strong>Other</strong>',html)
        self.assertNotIn('Risk · Other',html)
        # Each card carries a dot in its Airflow's Settings colour.
        self.assertEqual(html.count('class="airflow-dot risk-team" title="Risk Team"'),4)
        self.assertEqual(html.count('class="airflow-dot data-platform"'),len(model['nodes'])-4)

    def test_nodes_use_latest_observed_run_only_and_keep_all_todays_runs(self):
        w=self.workspace();first=w.runs[0]
        later={**first,'run_id':'later-run','dag_started_at_bkk':first['dag_started_at_bkk']+timedelta(minutes=5)}
        prior={**first,'run_id':'previous-day','dag_started_at_bkk':first['dag_started_at_bkk']-timedelta(days=1)}
        w.runs.extend([later,prior])
        m=dependency_model(w)
        node=m['nodes'][first['dag_id']]
        self.assertEqual(node['row']['run_id'],'later-run')
        self.assertEqual({r['run_id'] for r in node['runs']},{first['run_id'],'later-run'})
        self.assertEqual(len(m['positions']),len(m['nodes']))
        self.assertTrue(all(m['positions'][dag][1]>=HEADER_HEIGHT for dag in m['nodes']))
        self.assertTrue(all(m['positions'][dag][1]+CARD_HEIGHT<m['height'] for dag in m['nodes']))
        # Neither selection nor full-window mode can change scene coordinates.
        full=self.workspace(focus=first['dag_id'],dependency_fullscreen='1')
        before=dependency_model(self.workspace())
        after=dependency_model(full)
        for key in ('positions','width','height','row_heights'):self.assertEqual(before[key],after[key])
    def test_graph_and_layer_labels_need_no_external_mapping_file(self):
        from unittest.mock import patch
        from lineage_app.views.timeline import display_labels
        w=self.workspace()
        with patch('pathlib.Path.read_text',side_effect=AssertionError('Unexpected mapping read')):
            model=dependency_model(w)
            dependency_timetable(w)
            dependency_svg(w)
        self.assertTrue(all(node['group']==display_labels(next(d for d in w.inventory if d['dag_id']==dag))['group'] for dag,node in model['nodes'].items()))
        self.assertEqual(display_labels({'dag_id':'unknown_layer','tags':['layer:unknown','persist']})['group'],'Other')

    def test_native_controls_fullscreen_and_detail_run_links(self):
        w=self.workspace();html=dependency_timetable(w);parsed=Markup();parsed.feed(html)
        self.assertEqual(parsed.row_slots,list(range(0,1441,30)))
        from lineage_app.views.dependency import planned_time
        # Every DAG that ran, plus current DAGs with a clock schedule that did not run (dashed "planned" cards).
        planned={d['dag_id'] for d in w.inventory if not d.get('is_stale') and planned_time(d,w.day)}
        self.assertEqual({c['data-dag'] for c in parsed.cards},{r['dag_id'] for r in w.runs}|planned)
        self.assertGreater(len(planned-{r['dag_id'] for r in w.runs}),0)
        selectors=[i for i in parsed.inputs if i.get('class')=='dependency-select']
        self.assertEqual({i['value'] for i in selectors},{c['data-dag'] for c in parsed.cards})
        self.assertTrue(all('onclick' not in i for i in parsed.inputs))
        self.assertEqual(len([i for i in parsed.inputs if i.get('class')=='dependency-deselect']),len(selectors))
        fs=next(i for i in parsed.inputs if i.get('id')=='dependency-fullscreen')
        self.assertEqual(fs['type'],'checkbox');self.assertNotIn('checked',fs)
        self.assertEqual(fs['autocomplete'],'off')
        self.assertNotIn('<script',html)
        self.assertNotIn('Legacy dependencies',html)
        self.assertTrue(parsed.links)
        for link in parsed.links:
            query=parse_qs(urlsplit(link['href']).query)
            if 'move_dag' in query:  # "Move…" stays on Dependency with that DAG selected.
                self.assertEqual(query['view'],['dependency']);self.assertEqual(query['focus'],query['move_dag']);continue
            self.assertEqual(query['view'],['lineage'])
            self.assertIn('dag',query)
            if query['dag'][0] in {r['dag_id'] for r in w.runs}:self.assertIn('run',query)
            self.assertNotIn('fullscreen',query);self.assertNotIn('dependency_fullscreen',query)
        focused=self.workspace(focus=selectors[0]['value'],dependency_fullscreen='1')
        parsed=Markup();parsed.feed(dependency_timetable(focused))
        self.assertEqual([i['value'] for i in parsed.inputs if i.get('class')=='dependency-select' and 'checked' in i],[selectors[0]['value']])
        self.assertIn('checked',next(i for i in parsed.inputs if i.get('id')=='dependency-fullscreen'))
    def test_only_tagged_visible_edges_get_arrows_and_missing_nodes_are_explicit(self):
        w=self.workspace();ids=[r['dag_id'] for r in w.runs]
        a,b=ids[:2];missing='dag_without_a_run'
        for d in w.data['inventory']:
            d['tags']=[f'upstream:{a}',f'upstream:{missing}'] if d['dag_id']==b else ['upstream:none']
        html=dependency_timetable(w);parsed=Markup();parsed.feed(html)
        self.assertEqual([(p['data-upstream'],p['data-downstream']) for p in parsed.paths],[(a,b)])
        self.assertIn(missing,html)
        self.assertIn('Not in snapshot',html)
        self.assertIn('No recorded links',html)
        self.assertNotIn('data-dag="'+missing+'"',html)
        m=dependency_model(w)
        self.assertEqual(m['down'][a],{b})
        self.assertEqual(m['up'][b],{a,missing})

    def test_timeline_bookmark_opens_dependency_and_navigation_is_renamed(self):
        w=self.workspace();page=w.render()
        self.assertIn('<h1>Dependency</h1>',page)
        self.assertIn('view=dependency',page)
        self.assertNotIn('>Timeline<',page)
        old=Workspace(self.data,{'view':'timeline'},'csrf','cached')
        self.assertEqual(old.view,'dependency')
        self.assertEqual(old.params['view'],'dependency')

    def test_filters_do_not_erase_links_from_hidden_or_unrun_dags(self):
        a,b,c=[d['dag_id'] for d in self.data['inventory'][:3]]
        for d in self.data['inventory']:
            d['tags']=['upstream:none']+(['view-only-selected'] if d['dag_id']==a else [])
            if d['dag_id']==b:d['tags']=[f'upstream:{a}']
            if d['dag_id']==c:d['tags']=[f'upstream:{b}']
        w=self.workspace(tag='view-only-selected')
        model=dependency_model(w)
        self.assertEqual(set(model['nodes']),{a})
        self.assertEqual(model['down'][a],{b})
        self.assertEqual(model['down'][b],{c})
        html=dependency_timetable(w)
        self.assertIn('Filtered',html)
        self.assertNotIn('data-downstream="'+b+'"',html)

    def test_missing_tags_show_refresh_notice_without_loading_old_edges(self):
        for d in self.data['inventory']:d['tags']=['layer:persist']
        w=self.workspace()
        self.assertEqual(dependency_model(w)['edges'],[])
        self.assertIn('Import data after syncing Airflow tags',dependency_timetable(w))

    def test_find_dag_keeps_every_node_edge_and_position(self):
        baseline=self.workspace();before=dependency_model(baseline)
        dag=next(iter(before['nodes']))
        searched=self.workspace(q=dag,dag_search=dag,focus=dag)
        after=dependency_model(searched)
        for key in ('nodes','edges','positions','width','height'):self.assertEqual(before[key],after[key])
        self.assertEqual([d['dag_id'] for d in find_dependency(searched,dag.upper())],[dag])
        self.assertEqual(find_dependency(searched,'no-such-dag'),[])
        self.assertGreater(len(find_dependency(searched,'dp_')),1)
        html=searched.render();parsed=Markup();parsed.feed(html)
        self.assertEqual(len(parsed.cards),len(before['nodes']))
        target=next(card for card in parsed.cards if card['data-dag']==dag)
        self.assertEqual(target['id'],dependency_anchor(dag))
        self.assertEqual([i['value'] for i in parsed.inputs if i.get('class')=='dependency-select' and 'checked' in i],[dag])
        finder=next(i for i in parsed.inputs if i.get('name')=='dag_search')
        self.assertEqual(finder['form'],'dependency-state');self.assertIn('list',finder)
        self.assertIn('formaction="/dependency/find"',html)
        self.assertNotIn('name="q"',html)
        self.assertNotIn('dag_search=',searched.url(view='lineage'))
        self.assertNotIn('q=',searched.url(view='lineage'))
        no_run={'dag_id':'without_run','dag_display_name':'Without a run'}
        self.data['inventory'].append(no_run)
        searched.dependency_search={'matches':find_dependency(searched,'without_run')}
        parsed=Markup();html=dependency_timetable(searched);parsed.feed(html)
        self.assertIn('No run today',html)
        self.assertEqual({c['data-dag'] for c in parsed.cards},set(before['nodes']))

    def test_toolbar_preserves_native_selection_depth_and_fullscreen(self):
        w=self.workspace(zoom='0.85',tag='daily');parsed=Markup();parsed.feed(dependency_timetable(w))
        zooms=[b for b in parsed.buttons if b.get('name')=='zoom']
        self.assertEqual([b['value'] for b in zooms],['0.7','1.0','fit','1','0.85'])
        self.assertTrue(all(b.get('form')=='dependency-state' for b in zooms))
        self.assertEqual(zooms[-1]['formaction'],'/dependency.svg')
        self.assertTrue(all(i.get('form')=='dependency-state' for i in parsed.inputs if i.get('type') in ('radio','checkbox')))
        self.assertEqual({i['name'] for i in parsed.inputs if i.get('class')=='dependency-select'},{'focus'})
        fs=next(i for i in parsed.inputs if i.get('id')=='dependency-fullscreen')
        self.assertEqual(fs['name'],'dependency_fullscreen')
        dag=parsed.cards[0]['data-dag']
        next_view=self.workspace(focus=dag,zoom='fit',depth='direct',dependency_fullscreen='1')
        html=dependency_timetable(next_view)
        self.assertIn('zoom:min(1,calc((100cqw - 2px) / ',html)
        self.assertIn('calc((100cqh - 2px) / ',html)
        checked=Markup();checked.feed(html)
        self.assertEqual([i['value'] for i in checked.inputs if i.get('class')=='dependency-select' and 'checked' in i],[dag])
        self.assertIn('checked',next(i for i in checked.inputs if i.get('id')=='dependency-depth-direct'))
        self.assertIn('checked',next(i for i in checked.inputs if i.get('id')=='dependency-fullscreen'))

    def test_monitoring_layout_fits_both_dimensions_and_keeps_cards_and_arrows_aligned(self):
        w=self.workspace();m=dependency_model(w)
        self.assertEqual(w.params['zoom'],'1')
        default_html=dependency_timetable(w)
        self.assertIn('zoom:1.0',default_html)
        self.assertIn('>100%</span>',default_html)
        html=dependency_timetable(self.workspace(zoom='fit'))
        self.assertIn('100cqw',html);self.assertIn('100cqh',html)
        self.assertLess(CARD_HEIGHT,60)
        # Cards share the exact cell padding, gap and dimensions used by arrows.
        for (slot,group),items in m['buckets'].items():
            ordered=sorted(items,key=lambda dag:(m['nodes'][dag]['when'],dag))
            for i,dag in enumerate(ordered):
                x,y=m['positions'][dag]
                self.assertLessEqual(x+CARD_WIDTH,m['width'])
                self.assertLessEqual(y+CARD_HEIGHT,m['height'])
                for other in ordered[i+1:]:
                    ox,oy=m['positions'][other]
                    self.assertTrue(x+CARD_WIDTH<=ox or ox+CARD_WIDTH<=x or y+CARD_HEIGHT<=oy or oy+CARD_HEIGHT<=y)
        self.assertIn('class="dependency-slot-empty',html)
        self.assertIn('id="dependency-focus-none"',html)
        self.assertNotIn('checked',next(i for i in self.parse(html).inputs if i.get('id')=='dependency-fullscreen'))
        # Closing/selecting details must not change scene dimensions or positions.
        focused=self.workspace(focus=next(iter(m['nodes'])))
        after=dependency_model(focused)
        for key in ('width','height','positions','columns','row_heights'):self.assertEqual(m[key],after[key])

    def parse(self,html):
        parsed=Markup();parsed.feed(html);return parsed

    def test_svg_download_is_portable_and_matches_selected_dependency_depth(self):
        import xml.etree.ElementTree as ET
        from unittest.mock import patch
        w=self.workspace();m=dependency_model(w);a,b,c=list(m['nodes'])[:3]
        for d in w.data['inventory']:
            d['tags']=[f'upstream:{a}' if d['dag_id']==b else f'upstream:{b}' if d['dag_id']==c else 'upstream:none']
        ns={'s':'http://www.w3.org/2000/svg'}
        for depth,expected in [('direct',{(a,b)}),('all',{(a,b),(b,c)})]:
            selected=self.workspace(focus=a,depth=depth,zoom='0.85')
            svg=dependency_svg(selected)
            root=ET.fromstring(svg)
            edges={(p.get('data-upstream'),p.get('data-downstream')) for p in root.findall('.//s:path',ns) if p.get('data-upstream')}
            self.assertEqual(edges,expected)
            self.assertEqual(len(root.findall('.//s:g[@data-dag]',ns)),len(m['nodes']))
            meta=json.loads(root.find('s:metadata',ns).text)
            self.assertEqual(meta['focus'],a);self.assertFalse(meta['run_pairing_verified'])
            self.assertEqual(meta['source'],'airflow_upstream_tags+airflow_dependency_evidence')
            self.assertEqual(meta['snapshot_date'],str(w.day))
            self.assertNotIn('foreignObject',svg)
            self.assertNotIn('<script',svg)

    def test_move_pick_mode_turns_every_time_into_a_target(self):
        dag='dp_load_customer_profile'
        html=dependency_timetable(self.workspace(move_dag=dag,focus=dag))
        targets=re.findall(r'<th scope="row" class="timetable-time move-target[^"]*"><a href="([^"]+)"',html)
        self.assertEqual(len(targets),48)  # 00:00 ... 23:30
        first=parse_qs(urlsplit(targets[0].replace('&amp;','&')).query)
        self.assertEqual((first['move_dag'],first['move_to'],first['focus']),([dag],['00:00'],[dag]))
        self.assertIn(f'<strong>Moving {dag}</strong>',html);self.assertNotIn('move-verdict',html)
        self.assertNotIn('<script',html)

    def test_moving_earlier_flags_upstream_and_clears_todays_overlap(self):
        from lineage_app.views.dependency import dependency_model
        dag='dp_load_customer_profile'  # 01:03-02:06 today; upstream dp_ingest_core_banking ends 00:50.
        w=self.workspace(move_dag=dag,move_to='00:30',focus=dag);m=dependency_model(w)
        self.assertEqual(m['nodes'][dag]['slot'],30)
        result=m['move_result']
        self.assertEqual(list(result['conflicts']),['dp_ingest_core_banking'])
        c=result['conflicts']['dp_ingest_core_banking']
        self.assertEqual((c['relation'],c['level'],c['seconds'],c['existing']),('upstream',1,c['seconds'],False))
        self.assertGreater(c['seconds'],0)
        # Today it overlapped the downstream reconcile run (starts 02:00 < ends 02:06); at 00:30 it no longer does.
        self.assertEqual(result['cleared'],['dp_reconcile_mobile_financing_daily'])
        html=dependency_timetable(w)
        self.assertIn('✗ 1 conflict<',html)
        self.assertRegex(html,r'class="timetable-card dependency-card [^"]* moved"')
        self.assertRegex(html,r'data-dag="dp_ingest_core_banking"[^>]*Conflict \(upstream · direct\)'.replace('data-dag','class="[^"]*move-conflict" data-dag'))
        self.assertIn('class="timetable-time move-target current"',html)
        # Zoom/Fit keep the move: it is part of the state form.
        self.assertIn('<input type="hidden" name="move_to" value="00:30">',html)

    def test_moving_later_flags_downstream_and_no_conflict_says_so(self):
        from lineage_app.views.dependency import dependency_model
        dag='dp_ingest_core_banking'  # 00:06-00:50; direct downstream dp_load_customer_profile starts 01:03.
        late=dependency_model(self.workspace(move_dag=dag,move_to='01:00'))['move_result']['conflicts']
        self.assertEqual(list(late),['dp_load_customer_profile'])
        self.assertEqual((late['dp_load_customer_profile']['relation'],late['dp_load_customer_profile']['level']),('downstream',1))
        ok=dependency_timetable(self.workspace(move_dag=dag,move_to='00:00'))
        self.assertIn('✓ No conflicts',ok)
        # A run with no finished end today cannot be checked downstream, and says so.
        running=dependency_timetable(self.workspace(move_dag='dp_airflow_monitoring_logs',move_to='09:00'))
        self.assertIn('Downstream can’t be checked',running)

    def test_bad_move_values_are_ignored_and_leaving_the_page_drops_the_move(self):
        from lineage_app.views.dependency import dependency_model
        m=dependency_model(self.workspace(move_dag='dp_ingest_core_banking',move_to='25:99'))
        self.assertIsNone(m['move']['start']);self.assertNotIn('move_result',m)
        self.assertIsNone(dependency_model(self.workspace(move_dag='not_a_dag',move_to='01:00'))['move'])
        w=self.workspace(move_dag='dp_ingest_core_banking',move_to='01:00')
        self.assertNotIn('move_',w.url(view='inventory'))


    def test_schedule_analysis_draft_and_conflict_badges(self):
        from lineage_app.views.dependency import dependency_model
        # Today: dp_load_customer_profile 01:03-02:06; downstream reconcile starts 02:00 (also waits on lending, ends 03:42).
        m=dependency_model(self.workspace())
        self.assertEqual(list(m['analysis']['dp_load_customer_profile']),['dp_reconcile_mobile_financing_daily'])
        self.assertEqual(sorted(m['analysis']['dp_reconcile_mobile_financing_daily']),['dp_load_customer_profile','dp_load_lending_daily'])
        html=dependency_timetable(self.workspace())
        self.assertIn('dp_reconcile_mobile_financing_daily: end 02:06 &gt; start 02:00 (Δ6m)',html)
        self.assertIn('dp_load_lending_daily: end 03:42 &gt; start 02:00 (Δ1h 42m)',html)
        self.assertIn('✓ No conflicts with its upstream and downstream',html)  # e.g. dp_ingest_core_banking
        self.assertRegex(html,r'data-dag="dp_reconcile_mobile_financing_daily".*?<span class="conflict-badge"[^>]*>2</span>')
        self.assertIn('name="move_end" value="02:06"',html)  # draft starts as today's times
        # Apply a draft that ends at 01:50: no conflict left for it, and reconcile drops to one.
        w=self.workspace(move_dag='dp_load_customer_profile',move_to='01:03',move_end='01:50',focus='dp_load_customer_profile')
        m=dependency_model(w)
        self.assertEqual(m['move']['end'].strftime('%H:%M'),'01:50');self.assertTrue(m['move']['custom_end'])
        self.assertEqual(m['analysis']['dp_load_customer_profile'],{})
        self.assertEqual(list(m['analysis']['dp_reconcile_mobile_financing_daily']),['dp_load_lending_daily'])
        self.assertEqual(m['move_result']['cleared'],['dp_reconcile_mobile_financing_daily'])
        html=dependency_timetable(w)
        self.assertIn('Draft <b>01:03 → 01:50</b>',html);self.assertIn('<input type="hidden" name="move_end" value="01:50">',html)
        # The Move bar has an exact end as well as an exact start.
        bar=html.split('class="move-exact"',1)[1].split('</form>',1)[0]
        self.assertIn('name="move_to" value="01:03"',bar);self.assertIn('name="move_end" value="01:50"',bar)
        self.assertNotIn('type="hidden" name="move_end"',bar)
        # An end at or before the start is the next day.
        late=dependency_model(self.workspace(move_dag='dp_load_customer_profile',move_to='23:30',move_end='00:20'))['move']
        self.assertEqual((late['end']-late['start']).total_seconds(),50*60)


    def test_details_show_all_tags_then_inherited_projects(self):
        inv={d['dag_id']:d for d in self.data['inventory']}
        inv['dp_ingest_core_banking']['tags']=['team:dp','layer:persist']
        inv['dp_load_customer_profile']['tags']=['upstream:dp_ingest_core_banking','project:customer']
        html=dependency_timetable(self.workspace())
        box=html.split('data-dag-detail="dp_ingest_core_banking"',1)[1].split('data-dag-detail=',1)[0]
        tags=box.split('class="dependency-tags"',1)[1]
        self.assertLess(box.index('Today’s runs'),box.index('class="dependency-tags"'))  # after Today's runs
        self.assertIn('<h3>Tags <span>3</span></h3>',tags)
        self.assertIn('<span class="tag-pill">team:dp</span><span class="tag-pill">layer:persist</span>',tags)
        self.assertIn('<span class="tag-pill inherited-tag" title="Inherited from downstream: dp_load_customer_profile">project:customer</span>',tags)


    def test_dags_without_a_run_show_at_their_schedule_and_are_checked_as_60_minute_mock_runs(self):
        from lineage_app.views.dependency import dependency_model,planned_time
        inv={d['dag_id']:d for d in self.data['inventory']}
        ran={r['dag_id'] for r in self.workspace().runs}
        idle=next(k for k,d in inv.items() if k not in ran and not d.get('is_stale') and planned_time(d,self.workspace().day))
        # Make the idle DAG a direct downstream of dp_load_customer_profile (01:03-02:06), scheduled at 01:30.
        inv[idle].update(timetable_summary='30 1 * * *',tags=['upstream:dp_load_customer_profile'])
        unscheduled=next((k for k,d in inv.items() if k not in ran and k!=idle and not d.get('is_stale')),None)
        if unscheduled:inv[unscheduled]['timetable_summary']=None
        m=dependency_model(self.workspace())
        node=m['nodes'][idle]
        self.assertTrue(node['no_run']);self.assertEqual(node['when'].strftime('%H:%M'),'01:30');self.assertEqual(node['slot'],90)
        if unscheduled:self.assertNotIn(unscheduled,m['nodes'])  # no run and no schedule: not shown
        # Mock 01:30-02:30 starts before the upstream ends at 02:06: both sides report it.
        c=m['analysis'][idle]['dp_load_customer_profile']
        self.assertEqual((c['relation'],c['seconds']),('upstream',36*60))
        self.assertTrue(m['analysis']['dp_load_customer_profile'][idle]['mock'])
        html=dependency_timetable(self.workspace())
        card=html.split(f'data-dag="{idle}"',1)[0].rsplit('<div id=',1)[1]
        self.assertIn(' no-run"',card)
        self.assertIn('Planned 01:30 → 02:30',html.split(f'data-dag="{idle}"',1)[1].split('</div></div>',1)[0])
        self.assertIn('No run · planned 01:30 → 02:30 (mock 60m)',html)
        self.assertIn('has no run: planned time, mock 60m',html)
        for name,expect in (('0 16 5 1 *','16:00'),('@daily','00:00'),('0 0,6,8 * * *','00:00'),('*/15 2 * * *','02:00'),('None',None),('Partitioned Asset',None)):
            got=planned_time({'timetable_summary':name},self.workspace().day)
            self.assertEqual(got.strftime('%H:%M') if got else None,expect,name)


if __name__=='__main__':unittest.main()
