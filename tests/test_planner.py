"""Schedule arithmetic, dependency conflicts and incomplete-data boundaries."""
import copy
import re
from datetime import date,datetime,timedelta
from types import SimpleNamespace
import unittest
import xml.etree.ElementTree as ET

from lineage_app.data.planner import plan_model
from lineage_app.views.planner import planner_svg


class PlannerTests(unittest.TestCase):
    def workspace(self,**params):
        day=date(2026,11,4)
        def dt(t):return datetime.combine(day,datetime.strptime(t,'%H:%M').time())
        specs=[('A','02:00','02:45',[]),('F','03:00','03:30',['A']),
               ('B','04:00','04:30',['F','X']),('C','05:00','05:30',['B']),('X','02:30','03:45',[])]
        inv=[];runs={}
        for dag,start,end,parents in specs:
            inv.append(dict(dag_id=dag,tags=['layer:curate']+['upstream:'+v for v in parents or ['none']]))
            runs[dag]=[dict(dag_id=dag,run_id='run_'+dag,dag_started_at_bkk=dt(start),dag_completed_at_bkk=dt(end),dag_state='success')]
        return SimpleNamespace(day=day,meta=dict(stage='uat',snapshot_date=str(day),collected_at='2026-11-04T10:00:00+07:00'),
            params=dict(plan_dag='F',**params),dag_id='F',data={'inventory':inv},by_dag=runs)

    def test_unchanged_scenario_includes_all_descendants_and_their_other_prerequisites(self):
        m=plan_model(self.workspace())
        self.assertEqual(m['scope'],{'A','F','B','C','X'})
        self.assertEqual(m['ancestors'],{'A'})
        self.assertEqual(m['descendants'],{'B','C'})
        self.assertFalse(m['conflicts'])
        self.assertEqual(m['status'],'No timing conflicts')
        self.assertEqual(m['ready'].strftime('%H:%M'),'02:45')
        self.assertEqual(m['deadline'].strftime('%H:%M'),'04:00')

    def test_later_start_adds_conflict_and_other_starts_remain_fixed(self):
        w=self.workspace(plan_start='03:45');before=copy.deepcopy(w.by_dag)
        m=plan_model(w)
        self.assertEqual([(c['upstream'],c['downstream'],c['overlap'],c['change']) for c in m['conflicts']],[('F','B',900,'new')])
        self.assertEqual(m['scenario']['B']['start'],m['baseline']['B']['start'])
        self.assertEqual(w.by_dag,before)

    def test_earlier_start_conflicts_with_upstream_readiness(self):
        m=plan_model(self.workspace(plan_start='02:30'))
        self.assertEqual(m['conflicts'][0]['upstream'],'A')
        self.assertEqual(m['conflicts'][0]['downstream'],'F')
        self.assertEqual(m['conflicts'][0]['overlap'],900)

    def test_ten_and_twenty_percent_buffers_apply_to_all_measured_durations(self):
        for percent in (10,20):
            with self.subTest(percent=percent):
                m=plan_model(self.workspace(plan_buffer=str(percent)))
                self.assertAlmostEqual(m['scenario']['F']['duration'],1800*(1+percent/100))
                self.assertAlmostEqual(m['scenario']['X']['duration'],4500*(1+percent/100))
                self.assertEqual(m['scenario']['X']['start'],m['baseline']['X']['start'])

    def test_custom_end_overrides_selected_duration_only(self):
        m=plan_model(self.workspace(plan_start='03:30',plan_end='04:10',plan_buffer='20'))
        self.assertEqual(m['scenario']['F']['duration'],2400)
        self.assertEqual(m['scenario']['B']['duration'],2160)
        self.assertTrue(any(c['upstream']=='F' and c['overlap']==600 for c in m['conflicts']))

    def test_custom_end_and_buffer_cross_midnight_without_losing_the_day(self):
        w=self.workspace(plan_start='23:50',plan_end='00:20')
        m=plan_model(w)
        self.assertEqual(m['scenario']['F']['duration'],1800)
        self.assertEqual(m['scenario']['F']['end'].date(),w.day+timedelta(days=1))
        m=plan_model(self.workspace(plan_start='23:50',plan_buffer='20'))
        self.assertEqual(m['scenario']['F']['end'].strftime('%H:%M'),'00:26')

    def test_existing_overlaps_are_distinct_from_new_worse_and_resolved(self):
        w=self.workspace()
        w.by_dag['F'][0]['dag_completed_at_bkk']=w.by_dag['F'][0]['dag_completed_at_bkk'].replace(hour=4,minute=15)
        existing=plan_model(w)
        self.assertEqual(existing['changed'],0)
        self.assertEqual(existing['conflicts'][0]['change'],'existing')
        w.params['plan_start']='03:15'
        self.assertEqual(plan_model(w)['conflicts'][0]['change'],'worse')
        w.params['plan_start']='02:45'
        m=plan_model(w)
        self.assertFalse(m['conflicts'])
        self.assertEqual(len(m['resolved']),1)

    def test_missing_runtime_requires_custom_end_and_never_becomes_zero(self):
        w=self.workspace(plan_start='03:30')
        w.by_dag['F'][0].update(dag_completed_at_bkk=None,dag_state='running')
        m=plan_model(w)
        self.assertIsNone(m['scenario']['F']['end'])
        self.assertIn('F',m['gaps'])
        self.assertEqual(m['status'],'Needs review')
        w.params['plan_end']='04:10'
        m=plan_model(w)
        self.assertNotIn('F',m['gaps'])
        self.assertEqual(m['conflicts'][0]['change'],'uncompared')

    def test_missing_tag_coverage_failed_baselines_and_cycles_are_not_clear(self):
        w=self.workspace();w.data['inventory'][0]['tags']=['upstream:unknown']
        self.assertEqual(plan_model(w)['status'],'Needs review')
        w=self.workspace();w.by_dag['A'][0]['dag_state']='failed'
        self.assertEqual(plan_model(w)['failures'],['A'])
        self.assertEqual(plan_model(w)['status'],'Needs review')
        w=self.workspace();w.data['inventory'][0]['tags']=['upstream:F']
        self.assertTrue(plan_model(w)['cyclic'])

    def test_invalid_inputs_are_rejected(self):
        for params in (dict(plan_start='bad'),dict(plan_end='25:00'),dict(plan_buffer='nan'),dict(plan_buffer='-1'),dict(plan_buffer='201'),dict(plan_shift='500')):
            with self.subTest(params=params),self.assertRaises(ValueError):plan_model(self.workspace(**params))
        w=self.workspace();w.params['plan_dag']='not_collected'
        with self.assertRaises(ValueError):plan_model(w)

    def test_snapshot_change_resets_old_scenario_and_step_is_bounded(self):
        m=plan_model(self.workspace(plan_start='04:30',plan_buffer='20',plan_snapshot='old'))
        self.assertTrue(m['reset']);self.assertEqual(m['buffer'],0)
        self.assertEqual(m['scenario']['F']['start'],m['baseline']['F']['start'])
        m=plan_model(self.workspace(plan_start='03:00',plan_shift='15'))
        self.assertEqual(m['scenario']['F']['start'].strftime('%H:%M'),'03:15')
        with self.assertRaises(ValueError):plan_model(self.workspace(plan_start='00:05',plan_shift='-15'))

    def chart(self,w):
        m=plan_model(w);svg,width,height=planner_svg(m,interactive=False)
        root=ET.fromstring(svg);ns='{http://www.w3.org/2000/svg}'
        rows={}
        for group in root.iter(ns+'g'):
            if group.get('data-row'):
                bar=next((n for n in group if n.get('class')=='planner-run'),None)
                rows[group.get('data-planner-dag')]=dict(kind=group.get('data-row'),bar=bar,y=float(bar.get('y')) if bar is not None else None)
        return m,svg,root,rows

    def test_chart_shows_every_upstream_and_downstream_level_in_order(self):
        w=self.workspace();w.data['inventory'].append(dict(dag_id='Z',tags=['upstream:C']))
        w.by_dag['Z']=[dict(dag_id='Z',run_id='run_Z',dag_started_at_bkk=datetime(2026,11,4,6,0),dag_completed_at_bkk=datetime(2026,11,4,6,20),dag_state='success')]
        _,svg,_,rows=self.chart(w)
        self.assertEqual(set(rows),{'A','F','B','C','Z'})  # X is only B's other parent, not related to F
        order=sorted(rows,key=lambda d:rows[d]['y'])
        self.assertEqual(order,['A','F','B','C','Z'])
        ET.fromstring(svg)

    def test_selected_is_blue_interceptors_red_and_others_grey(self):
        from lineage_app.views.planner import BLUE,GREY,RED
        w=self.workspace()
        w.by_dag['B'][0]['dag_started_at_bkk']=w.by_dag['B'][0]['dag_started_at_bkk'].replace(hour=3,minute=15)  # starts before F ends
        w.by_dag['A'][0]['dag_completed_at_bkk']=w.by_dag['A'][0]['dag_completed_at_bkk'].replace(hour=3,minute=10)  # ends after F starts
        _,_,_,rows=self.chart(w)
        self.assertEqual(rows['F']['kind'],'selected');self.assertEqual(rows['F']['bar'].get('fill'),BLUE)
        self.assertEqual(rows['A']['kind'],'red');self.assertEqual(rows['A']['bar'].get('fill'),RED)
        self.assertEqual(rows['B']['kind'],'red')
        self.assertEqual(rows['C']['kind'],'grey');self.assertEqual(rows['C']['bar'].get('fill'),GREY)
        _,_,_,rows=self.chart(self.workspace())
        self.assertEqual({d:r['kind'] for d,r in rows.items()},{'A':'grey','F':'selected','B':'grey','C':'grey'})

    def test_bars_are_true_scale_and_only_names_and_times_are_text(self):
        m,svg,root,rows=self.chart(self.workspace())
        ppm=float(root.get('data-minute-px'))
        self.assertAlmostEqual(float(rows['F']['bar'].get('width')),30*ppm,delta=.2)
        self.assertAlmostEqual(float(rows['B']['bar'].get('x'))-float(rows['F']['bar'].get('x')),60*ppm,delta=.2)
        ns='{http://www.w3.org/2000/svg}'
        visible=[t.text for t in root.iter(ns+'text')]  # Tooltips (<title>) are not visible text.
        names={'A','F','B','C'}
        self.assertTrue(all(v in names or re.fullmatch(r'\d\d:\d\d( [+-]\d+d)?',v) for v in visible),visible)

    def test_labels_are_escaped_and_running_runs_are_hatched(self):
        w=self.workspace();w.data['inventory'][1]['dag_display_name']='<script>bad()</script>'
        w.by_dag['A'][0].update(dag_completed_at_bkk=None,dag_state='running')
        _,svg,_,rows=self.chart(w)
        self.assertNotIn('<script>',svg);self.assertNotIn('<a ',svg)
        self.assertEqual(rows['A']['bar'].get('fill'),'url(#run-red)')  # still running after F started

    def test_page_is_only_the_picker_and_the_chart(self):
        from lineage_app.views.planner import planner
        page=planner(self.workspace(plan_start='03:45',plan_buffer='20'))
        self.assertIn('class="gantt-pick"',page);self.assertIn('<div class="gantt" role="group"',page)
        for gone in ('New start','Overlaps','Check','More options','Download','Wider family','safe'):self.assertNotIn(gone,page)
        self.assertIn('data-planner-dag="F" data-row="selected"',page)

    def test_page_chart_fills_the_width_from_earliest_start_to_latest_finish(self):
        from lineage_app.views.planner import gantt_html
        html=gantt_html(plan_model(self.workspace()),interactive=False)
        bars={d:(float(x),float(w)) for d,x,w in re.findall(r'data-planner-dag="([A-Z])".*?<rect class="planner-run" x="([0-9.]+)%" y="8" width="([0-9.]+)%"',html)}
        self.assertEqual(list(bars),['A','F','B','C'])
        first=bars['A'][0];last=bars['C'][0]+bars['C'][1]
        self.assertAlmostEqual(first,100*0.02/1.04,delta=.05)   # 02:00 sits just inside the left edge…
        self.assertAlmostEqual(last,100*1.02/1.04,delta=.05)    # …and 05:30 just inside the right edge
        self.assertAlmostEqual(bars['F'][1],100*30/210/1.04,delta=.05)  # true scale in percent of the width
        self.assertIn('width="100%"',html)
        self.assertIn('>02:00<',html);self.assertIn('01:55 to 05:34 Bangkok time',html)  # 2% padding either side

    def test_ticks_adapt_to_the_range(self):
        from lineage_app.views.planner import ticks
        d=date(2026,11,4);at=lambda h,mi:datetime(2026,11,4,h,mi)
        self.assertEqual([t.strftime('%H:%M') for t in ticks(at(3,58),at(4,15),d)],['03:58','04:00','04:02','04:04','04:06','04:08','04:10','04:12','04:14'])
        self.assertLessEqual(len(ticks(at(0,0),at(23,0),d)),13)

    def test_clicking_a_row_opens_its_run_and_config_box(self):
        from lineage_app.views.planner import planner
        w=self.workspace();w.data['inventory'][2].update(timetable_summary='0 4 * * *',timetable_description='At 04:00',timezone='Asia/Bangkok',owners="['data-team']",is_paused='False',max_active_runs='1')
        w.by_dag['B'][0]['dag_started_at_bkk']=w.by_dag['B'][0]['dag_started_at_bkk'].replace(hour=3,minute=15)
        page=planner(w)
        self.assertIn('<label for="gsel-2" class="gantt-row red" data-planner-dag="B"',page)
        self.assertIn('<input type="radio" class="gantt-select" name="gantt_sel" id="gsel-2"',page)
        self.assertIn('.gantt-panel:has(#gsel-2:checked) .gsel-2-detail{display:block}',page)
        box=page[page.index('data-dag-detail="B"'):page.index('data-dag-detail="C"')]
        self.assertIn('Downstream · direct',box)
        self.assertIn('Might intercept: starts 15m before the selected DAG finishes.',box)
        self.assertIn('<dt>Run ID</dt><dd>run_B</dd>',box);self.assertIn('<dt>Duration</dt>',box)
        self.assertIn('<dt>Schedule</dt><dd>0 4 * * * · At 04:00</dd>',box);self.assertIn('<dt>Owners</dt><dd>data-team</dd>',box)
        self.assertIn('<dt>Paused</dt><dd>No</dd>',box)
        self.assertIn('href="/app?view=planner&amp;plan_dag=B">Plan this DAG</a>',box)
        for control in ('Close details','Move details to the other side','Hide details'):self.assertIn(f'aria-label="{control}"',page)
        selected=page[page.index('data-dag-detail="F"'):page.index('data-dag-detail="B"')]
        self.assertIn('Selected DAG',selected);self.assertNotIn('Plan this DAG',selected)

    def test_each_row_shows_its_run_status(self):
        from lineage_app.views.planner import planner
        w=self.workspace();w.by_dag['B'][0]['dag_state']='failed';w.by_dag['C']=[]
        page=planner(w)
        row=lambda d:page[page.index(f'data-planner-dag="{d}"'):page.index('</label>',page.index(f'data-planner-dag="{d}"'))]
        self.assertIn('<span class="state success">Success</span>',row('A'))
        self.assertIn('<span class="state failed">Failed</span>',row('B'))
        self.assertIn('<span class="state no-run">No run</span>',row('C'))

    def test_experiment_moves_the_selected_dag_and_rechecks_every_dependency(self):
        from lineage_app.views.planner import planner
        page=planner(self.workspace())
        self.assertIn('class="button gantt-exp-button"',page);self.assertIn('name="plan_start"',page)
        self.assertNotIn('gantt-exp-chip',page);self.assertIn('data-planner-dag="B" data-row="grey"',page)
        page=planner(self.workspace(plan_start='03:45'))  # 03:45 → 04:15 now runs past B's 04:00 start
        self.assertIn('Experiment 03:45 → 04:15',page)
        self.assertIn('data-planner-dag="B" data-row="red"',page);self.assertIn('data-planner-dag="A" data-row="grey"',page)
        self.assertIn('class="planner-actual"',page)        # today's real run stays as a ghost outline
        self.assertIn('F · experiment 03:45 → 04:15',page)
        box=page[page.index('data-dag-detail="B"'):page.index('data-dag-detail="C"')]
        self.assertIn('Might intercept: starts 15m before the selected DAG finishes.',box)
        page=planner(self.workspace(plan_start='02:30',plan_end='02:40'))  # starts before A finishes
        self.assertIn('data-planner-dag="A" data-row="red"',page);self.assertIn('Experiment 02:30 → 02:40',page)
        self.assertIn('value="02:40"',page)
        page=planner(self.workspace(plan_start='03:45',plan_buffer='20'))  # other old what-if fields are ignored
        self.assertIn('Experiment 03:45 → 04:15',page)

if __name__=='__main__':unittest.main()
