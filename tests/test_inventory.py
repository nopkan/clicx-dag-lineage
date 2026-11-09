"""Operational inventory counts, exact Airflow links, and missing-data semantics."""
import copy
import tempfile
import unittest
from datetime import timedelta
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs,quote,urlsplit

from sample_data import sample_collection
from lineage_app.views.inventory import COLUMNS,LOCKED
from lineage_app.data.parquet import ParquetStore
from lineage_app.views.render import Workspace

class Table(HTMLParser):
    def __init__(self):
        super().__init__();self.rows={};self.current=None;self.column=None;self.inputs=[];self.buttons=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='tr' and attrs.get('data-dag'):
            self.current={};self.rows[attrs['data-dag']]=self.current
        if tag=='td' and self.current is not None:
            self.column=attrs['class'].removeprefix('inventory-col-')
            self.current[self.column]={'text':'','links':[]}
        if tag=='a' and self.column:self.current[self.column]['links'].append(attrs)
        if tag=='input':self.inputs.append(attrs)
        if tag=='button':self.buttons.append(attrs)
    def handle_data(self,data):
        if self.column:self.current[self.column]['text']+=data
    def handle_endtag(self,tag):
        if tag=='td':self.column=None
        if tag=='tr':self.current=None

class InventoryTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        collection=sample_collection();store=ParquetStore(Path(tmp.name)/'uat')
        store.save_collection(collection,'https://airflow.example.com','uat')
        self.data=store.load()
    def render(self,data=None,**params):
        w=Workspace(data or self.data,{'view':'inventory',**params},'csrf','cached')
        html=w.render_inventory();parsed=Table();parsed.feed(html)
        return w,html,parsed
    def test_counts_and_failure_ratio_match_the_exact_latest_run(self):
        data=copy.deepcopy(self.data)
        original=next(r for r in data['rows'] if r['record_level']=='dag')
        dag=original['dag_id'];new_id='manual__latest/date+00:00'
        newer={**original,'run_id':new_id,'dag_state':'failed','dag_run_url':None,
            'dag_started_at_bkk':original['dag_started_at_bkk']+timedelta(minutes=5),
            'dag_completed_at_bkk':original['dag_started_at_bkk']+timedelta(minutes=8),
            'dag_duration_seconds':180}
        data['rows'].append(newer)
        task=next(r for r in data['rows'] if r['record_level']=='task' and r['dag_id']==dag)
        for i,state in enumerate(['failed','failed','upstream_failed']):
            data['rows'].append({**task,'run_id':new_id,'task_state':state,'map_index':i})
        data['rows'].append({**newer,'run_id':'yesterday','run_date':original['run_date']-timedelta(days=1)})
        w,html,table=self.render(data)
        row=table.rows[dag]
        self.assertEqual(row['run_id']['text'],new_id)
        self.assertEqual(row['runs']['text'],'2')
        self.assertEqual(row['failed_runs']['text'],str(sum(r['dag_state']=='failed' for r in [original,newer])))
        self.assertEqual(row['failed_tasks']['text'],'2 / 3')
        self.assertEqual(row['upstream_failed']['text'],'1')
        self.assertEqual(row['tasks']['text'],str(sum(t['dag_id']==dag for t in data['definitions'])))
        expected_edges={(r['upstream_task_id'],r['downstream_task_id']) for r in data['dependencies'] if r['dag_id']==dag and r['upstream_task_id'] and r['downstream_task_id']}
        self.assertEqual(row['dependencies']['text'],str(len(expected_edges)))
        self.assertEqual(row['dag_url']['links'][0]['href'],'https://airflow.example.com/dags/'+quote(dag,safe='')+'/')
        self.assertEqual(row['run_url']['links'][0]['href'],'https://airflow.example.com/dags/'+quote(dag,safe='')+'/runs/'+quote(new_id,safe='')+'/')
        query=parse_qs(urlsplit(row['lineage']['links'][0]['href']).query)
        self.assertEqual(query['dag'],[dag]);self.assertEqual(query['run'],[new_id])
        self.assertEqual(query['view'],['lineage'])
    def test_paused_stale_and_no_run_dags_remain_visible_without_fake_zero_definitions(self):
        data=copy.deepcopy(self.data);d=data['inventory'][0];dag=d['dag_id']
        d.update(is_stale=True,is_paused=True,has_import_errors=True)
        data['rows']=[r for r in data['rows'] if r['dag_id']!=dag]
        w,html,table=self.render(data)
        row=table.rows[dag]
        self.assertEqual(len(table.rows),len(data['inventory']))
        self.assertEqual(row['paused']['text'],'Yes')
        self.assertEqual(row['definition']['text'],'Stale')
        self.assertEqual(row['import_errors']['text'],'Yes')
        self.assertEqual(row['tasks']['text'],'—');self.assertEqual(row['dependencies']['text'],'—')
        self.assertEqual(row['run_url']['text'],'—');self.assertEqual(row['run_url']['links'],[])
        self.assertEqual(row['runs']['text'],'0')
        self.assertEqual(row['state']['text'],'No runs')
        self.assertTrue(row['dag_url']['links'])
        self.assertEqual(row['failed_tasks']['text'],'—')
        # A failed definition request also means unavailable, not zero tasks.
        d['is_stale']=False
        data['metadata']['warnings'].append('Task definitions unavailable: '+dag)
        row=self.render(data)[2].rows[dag]
        self.assertEqual(row['definition']['text'],'Unavailable')
        self.assertEqual(row['tasks']['text'],'—')
    def test_native_columns_all_metadata_and_bangkok_timestamp_conversion(self):
        data=copy.deepcopy(self.data);d=data['inventory'][0];dag=d['dag_id']
        d.update(last_parsed='2026-10-29T07:19:12Z',next_dagrun_run_after='2026-10-29T17:30:15Z',
            description='<script>alert(1)</script>',tags=[{'name':'tag'+str(i)} for i in range(6)])
        w,html,table=self.render(data)
        row=table.rows[dag]
        self.assertEqual(row['last_parsed']['text'],'29 Oct 2026 · 14:19:12')
        self.assertEqual(row['run_after']['text'],'30 Oct 2026 · 00:30:15')
        self.assertIn('tag5',row['tags']['text'])
        self.assertEqual(set(row),{c[0] for c in COLUMNS})
        checkboxes=[i for i in table.inputs if i.get('type')=='checkbox']
        self.assertEqual({i['id'].removeprefix('inventory-col-') for i in checkboxes},{key for key,title,tip,width in COLUMNS if key not in LOCKED})
        self.assertTrue(all('checked' in i for i in checkboxes))
        self.assertEqual(LOCKED,{'dag'})  # links can be hidden too
        self.assertTrue({'inventory-col-dag_url','inventory-col-run_url','inventory-col-lineage'}<={i['id'] for i in checkboxes})
        self.assertTrue(any(b.get('type')=='reset' for b in table.buttons))
        self.assertNotIn('<script',html);self.assertNotIn('onclick=',html)
        for key in ('dag_url','run_url'):
            for link in row[key]['links']:
                self.assertEqual(link['target'],'_blank')
                self.assertEqual(link['rel'],'noopener noreferrer')

if __name__=='__main__':unittest.main()
