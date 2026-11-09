"""Copied dp_dag_lineage_export folders: verification, import, Local data and newer-wins."""
import hashlib
import http.cookiejar
import io
import json
import re
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

import pyarrow as pa
import pyarrow.parquet as pq

from lineage_app import connections as c, server as app
from lineage_app.data import exports
from lineage_app.data.model import BKK
from lineage_app.data.parquet import ParquetStore

OUR = dict(name='Data Platform', base_url='https://d257oz8wesvwn0.cloudfront.net', stage='uat', dependency_mode='tags')
OTHER = dict(name='Risk Team', base_url='http://airflow-dpt.uat-ecs-dpf-dpt-thl-01-serverless-core:8080', stage='uat', dependency_mode='api')


def write_export(root, now, failed=(), stage='uat'):
    """A minimal folder in the dp_dag_lineage_export format (format version 1)."""
    day = now.astimezone(BKK).date()
    folder = Path(root) / stage / 'dag_lineage' / day.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    start = now.astimezone(BKK).replace(hour=1, minute=0, second=0, microsecond=0)
    tables = {name: [] for name in exports.COLUMNS}
    entries = []
    for source in (OUR, OTHER):
        sid = c.source_id(source['base_url'])
        entry = dict(source_id=sid, name=source['name'], base_url=source['base_url'], stage=stage,
                     dependency_mode=source['dependency_mode'])
        if source['name'] in failed:
            entries.append(dict(entry, status='failed', error='Cannot reach Airflow for authentication.'))
            continue
        common = dict(source_id=sid, source_name=source['name'], airflow_url=source['base_url'], stage=stage,
                      snapshot_date=day, collected_at=now)
        dag = 'our_dag' if source is OUR else 'other_dag'
        tables['airflow_dags'].append(dict(common, dag_id=dag, is_stale=False, raw_json=json.dumps(
            dict(dag_id=dag, is_stale=False, tags=[{'name': 'upstream:other_dag'}] if source is OUR else [],
                 owners=['x'], timetable_summary='0 1 * * *'))))
        tables['airflow_tasks'].extend(dict(common, dag_id=dag, task_id=t, operator_name='PythonOperator',
                                            raw_json=json.dumps(dict(task_id=t, operator_name='PythonOperator',
                                                                     downstream_task_ids=d)))
                                       for t, d in (('a', ['b']), ('b', [])))
        tables['airflow_dag_runs'].append(dict(common, dag_id=dag, run_id='r1', state='success', run_type='scheduled',
            raw_json=json.dumps(dict(dag_id=dag, dag_run_id='r1', state='success', run_type='scheduled',
                                     start_date=start.isoformat(), end_date=(start + timedelta(minutes=4)).isoformat()))))
        tables['airflow_task_instances'].extend(dict(common, dag_id=dag, run_id='r1', task_id=t, map_index=-1,
            try_number=1, state='success', raw_json=json.dumps(dict(dag_id=dag, dag_run_id='r1', task_id=t, map_index=-1,
                try_number=1, state='success', operator='PythonOperator', start_date=start.isoformat(),
                end_date=(start + timedelta(minutes=1)).isoformat()))) for t in ('a', 'b'))
        tables['airflow_operations'].append(dict(common, resource='health', record_key='0', raw_json='{"scheduler":{"status":"healthy"}}'))
        if source is OUR:
            tables['airflow_dag_links'].append(dict(common, link_kind='tag', evidence='dag_tags', upstream_dag_id='other_dag',
                downstream_dag_id='our_dag', upstream_source_id=c.source_id(OTHER['base_url']), downstream_source_id=sid,
                resolution='cross_airflow', declared_by_dag_id='our_dag', task_id=None, detail='upstream:other_dag', raw_json='{}'))
        entries.append(dict(entry, status='complete', warnings=[], coverage=dict(dags=1, stale_dags=0, definitions=2, dag_runs=1, task_instances=2),
                            operational_coverage=dict(health=dict(state='complete', rows=1)),
                            dependency_coverage=dict(dependency_graph=dict(state='unavailable', http_status=404))))
    files = []
    for name, columns in exports.COLUMNS.items():
        schema = pa.schema([(k, pa.date32() if k == 'snapshot_date' else pa.timestamp('us', tz='UTC') if k == 'collected_at'
                             else pa.bool_() if k == 'is_stale' else pa.int64() if k in ('map_index', 'try_number') else pa.string())
                            for k in columns])
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(tables[name], schema=schema), buffer)
        body = buffer.getvalue()
        (folder / f'{name}.parquet').write_bytes(body)
        files.append(dict(name=f'{name}.parquet', rows=len(tables[name]), bytes=len(body), sha256=hashlib.sha256(body).hexdigest()))
    manifest = dict(format='clicx-dag-lineage', version=1, snapshot_date=day.isoformat(), timezone='Asia/Bangkok',
                    collected_at=now.isoformat(), completed_at=now.isoformat(),
                    producer=dict(dag_id='dp_dag_lineage_export', run_id='manual__test'), sources=entries, files=files)
    (folder / 'manifest.json').write_text(json.dumps(manifest))
    return folder


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name); self.stores = {}
        self.now = datetime.now(timezone.utc).replace(microsecond=0)

    def tearDown(self):
        self.tmp.cleanup()

    def store_for(self, stage, base=None):
        key = c.cache_key(stage, base)
        return self.stores.setdefault(key, ParquetStore(self.root / stage / ('source_id=' + key.split(':')[1])))

    def test_imports_both_sources_into_their_url_partitions_once(self):
        write_export(self.root, self.now)
        notices = exports.sync(self.root, ('uat',), self.store_for)
        day = self.now.astimezone(BKK).date()
        self.assertEqual(notices, [f'Imported Data Platform from uat/dag_lineage/{day}.', f'Imported Risk Team from uat/dag_lineage/{day}.'])
        our = self.store_for('uat', OUR['base_url'])
        meta = our.metadata()
        self.assertEqual((meta['source_name'], meta['base_url'], meta['snapshot_date']), ('Data Platform', OUR['base_url'], day.isoformat()))
        self.assertEqual(meta['export']['folder'], f'dag_lineage/{day}')
        self.assertEqual(meta['dependency_coverage']['dependency_graph']['http_status'], 404)
        data = our.load()
        self.assertEqual([d['dag_id'] for d in data['inventory']], ['our_dag'])
        self.assertEqual({(r['upstream_task_id'], r['downstream_task_id']) for r in data['dependencies']}, {('a', 'b')})
        self.assertEqual(sorted(r['record_level'] for r in data['rows']), ['dag', 'task', 'task'])
        self.assertEqual(data['rows'][0]['dag_run_url'], OUR['base_url'] + '/dags/our_dag/runs/r1')
        links = our.raw_rows('airflow_dag_links', day)
        self.assertEqual([(l['upstream_dag_id'], l['downstream_dag_id'], l['resolution']) for l in links],
                         [('other_dag', 'our_dag', 'cross_airflow')])
        self.assertEqual(self.store_for('uat', OTHER['base_url']).raw_rows('airflow_dag_links', day), [])
        files = {p: p.stat().st_mtime_ns for p in self.root.rglob('*.parquet') if 'source_id=' in str(p)}
        self.assertEqual(exports.sync(self.root, ('uat',), self.store_for), [])
        self.assertEqual({p: p.stat().st_mtime_ns for p in self.root.rglob('*.parquet') if 'source_id=' in str(p)}, files)

    def test_exports_with_the_old_source_labels_are_renamed(self):
        folder = write_export(self.root, self.now)
        manifest = json.loads((folder / 'manifest.json').read_text())
        for source, old in zip(manifest['sources'], ('Our Airflow', 'Other Airflow')):source['name'] = old
        (folder / 'manifest.json').write_text(json.dumps(manifest))
        day = self.now.astimezone(BKK).date()
        self.assertEqual(exports.sync(self.root, ('uat',), self.store_for),
                         [f'Imported Data Platform from uat/dag_lineage/{day}.', f'Imported Risk Team from uat/dag_lineage/{day}.'])
        self.assertEqual([self.store_for('uat', s['base_url']).metadata()['source_name'] for s in (OUR, OTHER)], ['Data Platform', 'Risk Team'])

    def test_partial_copy_or_unknown_files_are_rejected_before_any_write(self):
        folder = write_export(self.root, self.now)
        path = folder / 'airflow_task_instances.parquet'; path.write_bytes(path.read_bytes()[:-1])
        notices = exports.sync(self.root, ('uat',), self.store_for)
        self.assertEqual(len(notices), 1); self.assertIn('does not match manifest.json', notices[0])
        self.assertEqual(list(self.root.rglob('metadata.json')), [])
        (folder / 'manifest.json').unlink()
        self.assertIn('manifest.json is missing', exports.sync(self.root, ('uat',), self.store_for)[0])

    def test_newer_snapshot_wins_and_failed_source_is_reported(self):
        write_export(self.root, self.now)
        exports.sync(self.root, ('uat',), self.store_for)
        shutil.rmtree(self.root / 'uat' / 'dag_lineage')
        write_export(self.root, self.now - timedelta(minutes=5), failed=('Risk Team',))
        notices = exports.sync(self.root, ('uat',), self.store_for)
        day = self.now.astimezone(BKK).date()
        self.assertEqual(notices, [f'uat/dag_lineage/{day}: Risk Team was not exported (Cannot reach Airflow for authentication.).'])
        self.assertEqual(self.store_for('uat', OUR['base_url']).metadata()['collected_at'], self.now.isoformat())

    def test_folder_in_the_wrong_environment_is_not_imported(self):
        folder = write_export(self.root, self.now)
        target = self.root / 'dev' / 'dag_lineage'; target.mkdir(parents=True); shutil.move(folder, target / folder.name)
        notices = exports.sync(self.root, ('uat', 'dev'), self.store_for)
        self.assertTrue(notices and all('belongs in data/uat/dag_lineage/' in n for n in notices[:2]))
        self.assertEqual(list(self.root.rglob('metadata.json')), [])

    def test_every_date_is_imported_and_older_dates_do_not_replace_the_newest(self):
        write_export(self.root, self.now)
        exports.sync(self.root, ('uat',), self.store_for)
        old = self.now - timedelta(days=2)
        write_export(self.root, old)  # Copied later, e.g. a backfilled folder.
        day, old_day = (t.astimezone(BKK).date().isoformat() for t in (self.now, old))
        self.assertEqual(exports.sync(self.root, ('uat',), self.store_for),
                         [f'Imported Data Platform from uat/dag_lineage/{old_day}.', f'Imported Risk Team from uat/dag_lineage/{old_day}.'])
        our = self.store_for('uat', OUR['base_url'])
        self.assertEqual(our.dates(), [day, old_day])
        self.assertEqual(our.metadata()['snapshot_date'], day)
        dated = our.at(old_day)
        self.assertEqual((dated.metadata()['snapshot_date'], dated.load()['metadata']['collected_at']), (old_day, old.isoformat()))
        self.assertEqual(len(dated.load()['rows']), 3)
        self.assertEqual(exports.sync(self.root, ('uat',), self.store_for), [])


class ConnectPageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); app.DATA_ROOT = Path(self.tmp.name); app.STORES.clear(); app.SESSIONS.clear()
        self.web = ThreadingHTTPServer(('127.0.0.1', 0), app.Handler)
        threading.Thread(target=self.web.serve_forever, daemon=True).start()
        self.base = f'http://127.0.0.1:{self.web.server_port}'
        self.client = build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def tearDown(self):
        self.web.shutdown(); self.web.server_close(); self.tmp.cleanup()

    def test_import_without_aws_opens_copied_folder_and_top_bar_switches_source(self):
        from unittest.mock import patch
        from lineage_app.data import aws_import
        write_export(app.DATA_ROOT, datetime.now(timezone.utc))  # e.g. copied into Docker
        with self.client.open(self.base + '/') as response:
            body = response.read().decode()
        self.assertIn('action="/import"', body)
        self.assertIn('name="s3_uri" value="s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/"', body)
        for gone in ('aws-profile', 'Last import', 'connect-steps'):self.assertNotIn(gone, body)
        for gone in ('Open local data', 'local-cache', 'CLICX1', 'Imported'):
            self.assertNotIn(gone, body)
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        def offline(root, cancel, progress, **chosen):raise aws_import.ImportFailed('The AWS CLI is not installed where the dashboard runs.')
        with patch.object(aws_import, 'download', offline):
            with self.client.open(Request(self.base + '/import', urlencode(dict(csrf=csrf)).encode())) as response:response.read()
            session = next(s for s in app.SESSIONS.values() if s['csrf'] == csrf)
            deadline = time.monotonic() + 10
            while not session['job']['done']:
                self.assertLess(time.monotonic(), deadline); time.sleep(.05)
        with self.client.open(self.base + '/progress') as response:
            body = response.read().decode()
        day = datetime.now(BKK).date().isoformat()
        self.assertIn('Import stopped', body); self.assertIn(f'Showing data downloaded earlier (newest {day})', body)
        self.assertIn('href="/app">Open saved data', body)
        our, other = (c.cache_key('uat', s['base_url']) + '@' + day for s in (OUR, OTHER))
        with self.client.open(self.base + '/app?view=inventory') as response:
            page = response.read().decode()
        our, other = (key.partition('@')[0] for key in (our, other))
        self.assertIn('· All Airflows · Bangkok</div>', page)
        with self.client.open(self.base + '/app?view=settings') as response:
            settings = response.read().decode()
        self.assertIn('<span class="saved-current">All Airflows</span>', settings)
        for key, name in ((our, 'Data Platform'), (other, 'Risk Team')):
            self.assertIn(f'airflow={key.replace(":", "%3A")}">{name}</a>', settings)
        self.assertIn('our_dag', page); self.assertIn('other_dag', page)
        with self.client.open(self.base + f'/app?view=raws&raw_table=airflow_dag_links&airflow={other}') as response:
            page = response.read().decode()
        self.assertIn('· Risk Team · Bangkok</div>', page); self.assertIn('Cross-DAG links', page)
        with self.client.open(self.base + '/app?view=inventory') as response:
            page = response.read().decode()
        self.assertIn('other_dag', page); self.assertNotIn('our_dag', page)

if __name__ == '__main__':
    unittest.main()
