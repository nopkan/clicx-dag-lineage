"""Import button: AWS sign-in → aws s3 sync → import → open any saved date. Uses a fake `aws` CLI."""
import http.cookiejar
import os
import re
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

from lineage_app import connections as c, server as app
from lineage_app.data import aws_import
from lineage_app.data.model import BKK
from test_exports import OUR, write_export

FAKE_AWS = r'''#!{python}
"""Fake AWS CLI: expired sign-in until `sso login`, then s3 sync copies $FAKE_S3."""
import os, shutil, sys
state = os.environ['FAKE_AWS_STATE']; args = sys.argv[1:]
open(os.path.join(state, 'calls'), 'a').write(' '.join(args) + '\n')
profile = args[args.index('--profile') + 1] if '--profile' in args else None
marker = os.path.join(state, 'signed-in' + ('-' + os.path.basename(os.environ.get('HOME', '')) if os.environ.get('FAKE_AWS_PER_HOME') else ''))
signed = os.path.exists(marker)
open(os.path.join(state, 'homes'), 'a').write(os.environ.get('HOME', '') + '|' + os.environ.get('AWS_CONFIG_FILE', '') + '\n')
no_role = profile in os.environ.get('FAKE_AWS_NO_ROLE', '').split(',')  # this person lacks the role behind the profile
if args[:2] == ['sts', 'get-caller-identity']:
    if signed and no_role: sys.exit(print('An error occurred (ForbiddenException) when calling the GetRoleCredentials operation: No access') or 255)
    sys.exit(0 if signed else print('Error when retrieving token from sso: Token has expired') or 255)
if args[:3] == ['configure', 'get', 'source_profile']:
    sys.exit(print('sso-base') if profile == 'nonprod_DP_DataEngineerOS' else 1)
if args[:2] == ['sso', 'login']:
    print('Browser will not be automatically opened. Visit https://device.sso.example/?user_code=ABCD-EFGH')
    if os.environ.get('FAKE_AWS_DENY_LOGIN'): sys.exit(1)
    open(marker, 'w').close(); sys.exit(0)
if args[:2] == ['s3', 'sync']:
    if os.environ.get('FAKE_AWS_DENY_S3') or profile in os.environ.get('FAKE_AWS_DENY_S3_FOR', '').split(','):
        sys.exit(print('An error occurred (AccessDenied) when calling the ListObjectsV2 operation') or 1)
    shutil.copytree(os.environ['FAKE_S3'], args[3], dirs_exist_ok=True); sys.exit(0)
sys.exit(2)
'''


class FakeAWS:
    def __init__(self, root):
        self.bin, self.state, self.s3 = (Path(root) / n for n in ('bin', 'state', 's3'))
        for path in (self.bin, self.state, self.s3):
            path.mkdir(parents=True)
        (self.bin / 'fake_aws.py').write_text(FAKE_AWS.replace('#!{python}\n', ''))
        script = self.bin / 'aws'  # Shell wrapper: the interpreter path may contain spaces.
        script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{self.bin / "fake_aws.py"}" "$@"\n')
        script.chmod(0o755)
        # write_export(self.s3, …) creates uat/dag_lineage/<date>/; the bucket prefix is that dag_lineage/ folder.
        prefix = self.s3 / 'uat' / 'dag_lineage'; prefix.mkdir(parents=True)
        self.env = patch.dict(os.environ, {'PATH': f'{self.bin}{os.pathsep}{os.environ["PATH"]}',
                                           'FAKE_AWS_STATE': str(self.state), 'FAKE_S3': str(prefix)})

    def calls(self):
        path = self.state / 'calls'
        return path.read_text().splitlines() if path.exists() else []


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.aws = FakeAWS(self.tmp.name); self.aws.env.start()
        self.messages = []

    def tearDown(self):
        self.aws.env.stop(); self.tmp.cleanup()

    def test_expired_sign_in_opens_sso_login_for_the_source_profile_then_syncs(self):
        write_export(self.aws.s3, datetime.now(timezone.utc))
        target = aws_import.download(Path(self.tmp.name) / 'data', threading.Event(), self.messages.append)
        day = datetime.now(BKK).date().isoformat()
        self.assertTrue((target / day / 'manifest.json').exists())
        self.assertEqual(target, Path(self.tmp.name) / 'data' / 'uat' / 'dag_lineage')
        calls = self.aws.calls()
        self.assertIn('sso login --profile sso-base', calls)
        self.assertTrue(any(call.startswith('s3 sync s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/ ') and
                            call.endswith('--profile nonprod_DP_DataEngineerOS --only-show-errors --no-progress')
                            for call in calls))
        self.assertTrue(any('https://device.sso.example/?user_code=ABCD-EFGH' in m for m in self.messages))
        before = len(calls)
        aws_import.download(Path(self.tmp.name) / 'data', threading.Event(), self.messages.append)
        self.assertNotIn('sso login --profile sso-base', self.aws.calls()[before:])  # Still signed in.

    def test_failed_sign_in_and_denied_bucket_have_clear_messages(self):
        with patch.dict(os.environ, {'FAKE_AWS_DENY_LOGIN': '1'}):
            with self.assertRaisesRegex(aws_import.ImportFailed, 'AWS sign-in did not complete'):
                aws_import.download(self.tmp.name, threading.Event(), self.messages.append)
        (self.aws.state / 'signed-in').touch()
        with patch.dict(os.environ, {'FAKE_AWS_DENY_S3': '1'}):
            with self.assertRaisesRegex(aws_import.ImportFailed, 's3:ListBucket and s3:GetObject'):
                aws_import.download(self.tmp.name, threading.Event(), self.messages.append)

    def test_docker_uses_device_code_sign_in_and_shows_the_link(self):
        with patch.object(aws_import, 'DEVICE_CODE', True):
            aws_import.download(Path(self.tmp.name) / 'data', threading.Event(), self.messages.append)
        self.assertIn('sso login --profile sso-base --use-device-code', self.aws.calls())
        self.assertTrue(any(m.startswith('Sign in to AWS: open https://device.sso.example/?user_code=ABCD-EFGH') for m in self.messages))

    SSO_ENV = {'LINEAGE_AWS_SSO_START_URL': 'https://d-0000000000.awsapps.com/start', 'LINEAGE_AWS_SSO_REGION': 'ap-southeast-1',
               'LINEAGE_AWS_SSO_ACCOUNT_ID': '111122223333', 'LINEAGE_AWS_REGION': 'ap-southeast-7',
               'LINEAGE_AWS_ROLES': 'EngineerOS>arn:aws:iam::111122223333:role/DP_EngineerOS,Engineer>arn:aws:iam::111122223333:role/DP_Engineer'}

    def routed(self, home, **env):
        """One import in Docker with a private AWS folder; returns the AWS calls it made."""
        before = len(self.aws.calls())
        with patch.dict(os.environ, {**self.SSO_ENV, **env}), patch.object(aws_import, 'DEVICE_CODE', True):
            aws_import.download(Path(self.tmp.name) / 'data', threading.Event(), self.messages.append, aws_home=home)
        return self.aws.calls()[before:]

    def test_docker_signs_in_once_and_uses_the_first_role_route(self):
        home = Path(self.tmp.name) / 'alice'
        calls = self.routed(home)
        self.assertIn('sso login --sso-session lineage-sso --use-device-code', calls)
        self.assertTrue(any(c.startswith('s3 sync') and c.endswith('--profile lineage-1 --only-show-errors --no-progress') for c in calls))
        self.assertTrue(any('using EngineerOS → DP_EngineerOS' in m for m in self.messages))
        config = (home / '.aws' / 'config').read_text()
        for line in ('[sso-session lineage-sso]', 'sso_start_url = https://d-0000000000.awsapps.com/start',
                     '[profile lineage-1-sso]', 'sso_role_name = EngineerOS', '[profile lineage-1]',
                     'role_arn = arn:aws:iam::111122223333:role/DP_EngineerOS', 'source_profile = lineage-1-sso',
                     '[profile lineage-2-sso]', 'sso_role_name = Engineer', 'role_arn = arn:aws:iam::111122223333:role/DP_Engineer'):
            self.assertIn(line, config)
        self.assertEqual((home / '.aws' / 'config').stat().st_mode & 0o777, 0o600)
        self.assertNotIn('sso login', ' '.join(self.routed(home)))  # still signed in next time

    def test_someone_with_only_the_other_role_uses_it(self):
        calls = self.routed(Path(self.tmp.name) / 'bob', FAKE_AWS_NO_ROLE='lineage-1,lineage-1-sso')
        self.assertTrue(any(c.startswith('s3 sync') and '--profile lineage-2 ' in c for c in calls))
        self.assertTrue(any('using Engineer → DP_Engineer' in m for m in self.messages))

    def test_a_role_that_cannot_read_the_bucket_falls_back_to_the_next_route(self):
        calls = self.routed(Path(self.tmp.name) / 'carol', FAKE_AWS_DENY_S3_FOR='lineage-1')
        syncs = [c for c in calls if c.startswith('s3 sync')]
        self.assertEqual([c.split('--profile ')[1].split()[0] for c in syncs], ['lineage-1', 'lineage-1-sso'])  # then the SSO role directly

    def test_someone_without_either_role_gets_a_clear_message(self):
        with self.assertRaisesRegex(aws_import.ImportFailed, 'no access to EngineerOS or Engineer in account 111122223333'):
            self.routed(Path(self.tmp.name) / 'dave', FAKE_AWS_NO_ROLE='lineage-1,lineage-1-sso,lineage-2,lineage-2-sso')
        with self.assertRaisesRegex(aws_import.ImportFailed, 'denied access .* for EngineerOS → DP_EngineerOS, EngineerOS, Engineer → DP_Engineer, Engineer'):
            self.routed(Path(self.tmp.name) / 'erin', FAKE_AWS_DENY_S3='1')

    def test_each_browser_session_signs_in_with_its_own_private_aws_folder(self):
        users = Path(self.tmp.name) / 'users'
        with patch.dict(os.environ, {'LINEAGE_AWS_USERS_DIR': str(users)}), patch.object(aws_import, 'DEVICE_CODE', True):
            alice, bob = aws_import.user_home('session-a'), aws_import.user_home('session-b')
        self.assertNotEqual(alice, bob); self.assertEqual(alice.stat().st_mode & 0o777, 0o700)
        logins = lambda calls: sum('sso login' in c for c in calls)
        self.assertEqual(logins(self.routed(alice, FAKE_AWS_PER_HOME='1')), 1)
        self.assertEqual(logins(self.routed(alice, FAKE_AWS_PER_HOME='1')), 0)  # Alice stays signed in…
        self.assertEqual(logins(self.routed(bob, FAKE_AWS_PER_HOME='1')), 1)    # …Bob signs in himself.
        homes = (self.aws.state / 'homes').read_text().splitlines()
        self.assertTrue(all(h.startswith(str(users)) and h.split('|')[1].endswith('/.aws/config') for h in homes))
        with patch.object(aws_import, 'DEVICE_CODE', False):
            self.assertIsNone(aws_import.user_home('session-a'))  # outside Docker: the machine's own ~/.aws

    def test_routes_come_from_env_and_bad_entries_are_ignored(self):
        with patch.dict(os.environ, {'LINEAGE_AWS_ROLES': 'Good>arn:aws:iam::111122223333:role/Ok, bad role ,Direct,Evil>arn:aws:iam::1:role/x'}):
            self.assertEqual(aws_import._routes(), [('Good', 'arn:aws:iam::111122223333:role/Ok'), ('Direct', '')])
        with patch.dict(os.environ, {'LINEAGE_AWS_ROLES': '', 'LINEAGE_AWS_SSO_ROLE_NAME': 'Single', 'LINEAGE_AWS_ROLE_ARN': ''}):
            self.assertEqual(aws_import._routes(), [('Single', '')])
        with patch.dict(os.environ, self.SSO_ENV), patch.object(aws_import, 'DEVICE_CODE', False):
            self.assertIsNone(aws_import._sso_settings())  # never used outside Docker

    def test_missing_cli_is_reported(self):
        with patch.object(aws_import.shutil, 'which', return_value=None), patch.object(aws_import.os, 'access', return_value=False):
            with self.assertRaisesRegex(aws_import.ImportFailed, 'AWS CLI is not installed'):
                aws_import.download(self.tmp.name, threading.Event(), self.messages.append)


class SnapshotLabelTests(unittest.TestCase):
    def test_labels_use_bangkok_time_and_name_recent_days(self):
        from datetime import date
        from lineage_app.views.render import snapshot_label
        today = date(2026, 11, 9)
        self.assertEqual(snapshot_label('2026-11-09', '2026-11-09T08:36:00+00:00', today), 'Today · as of 15:36')
        self.assertEqual(snapshot_label('2026-11-08', '2026-11-08T16:10:00+00:00', today), 'Yesterday · as of 23:10')
        self.assertEqual(snapshot_label('2026-11-05', '2026-11-06T02:12:00+00:00', today), '5 Nov 2026 · as of 6 Nov 09:12')
        self.assertEqual(snapshot_label('2026-11-05', '', today), '5 Nov 2026')


class ImportButtonTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.aws = FakeAWS(self.tmp.name); self.aws.env.start()
        app.DATA_ROOT = Path(self.tmp.name) / 'data'; app.STORES.clear(); app.SESSIONS.clear()
        self.web = ThreadingHTTPServer(('127.0.0.1', 0), app.Handler)
        threading.Thread(target=self.web.serve_forever, daemon=True).start()
        self.base = f'http://127.0.0.1:{self.web.server_port}'
        self.client = build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def tearDown(self):
        self.web.shutdown(); self.web.server_close(); self.aws.env.stop(); self.tmp.cleanup()

    def page(self, path, values=None):
        request = Request(self.base + path, urlencode(values).encode() if values is not None else None)
        with self.client.open(request) as response:
            return response.read().decode(), response.url

    def wait(self, csrf):
        session = next(s for s in app.SESSIONS.values() if s['csrf'] == csrf)
        deadline = time.monotonic() + 20
        while not session['job']['done']:
            self.assertLess(time.monotonic(), deadline, 'Import did not finish')
            time.sleep(.05)
        return self.page('/progress')

    def test_import_button_downloads_every_date_and_the_workspace_switches_dates(self):
        now = datetime.now(timezone.utc)
        for moment in (now, now - timedelta(days=1)):
            write_export(self.aws.s3, moment)
        body, _ = self.page('/')
        self.assertNotIn('local-cache', body); self.assertNotIn('name="profile"', body)
        self.assertIn('href="https://d257oz8wesvwn0.cloudfront.net/dags/dp_dag_lineage_export" target="_blank" rel="noopener noreferrer">dp_dag_lineage_export', body)
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        self.page('/import', dict(csrf=csrf))
        body, url = self.wait(csrf)
        self.assertTrue(url.endswith('/app'), url); self.assertIn('<h1>Overview</h1>', body)
        today, yesterday = now.astimezone(BKK).date().isoformat(), (now - timedelta(days=1)).astimezone(BKK).date().isoformat()
        our = c.cache_key('uat', OUR['base_url'])
        self.assertNotIn('source-badge', body); self.assertNotIn('snapshot-switch', body)
        self.assertNotIn('aria-label="Airflow"', body); self.assertNotIn('name="date"', body)  # Chosen in Settings only.
        self.assertIn('· All Airflows · Bangkok</div>', body)  # Merged by default.
        self.assertIn('<span class="metric-top">DAGs</span><span class="metric-value">2</span>', body)
        settings, _ = self.page('/app?view=settings')
        self.assertIn('<span class="saved-current">All Airflows</span>', settings)
        self.assertIn(f'href="/app?view=settings&amp;airflow={our.replace(":", "%3A")}">Data Platform</a>', settings)
        self.assertIn(f'href="/app?view=settings&amp;date={yesterday}">Open</a>', settings)
        self.page(f'/app?view=settings&date={yesterday}'); self.page(f'/app?view=settings&airflow={our}')
        body, _ = self.page('/app?view=overview')  # The Settings choice applies to every page…
        self.assertIn(f'Snapshot {yesterday}', body); self.assertIn('· Data Platform · Bangkok</div>', body)
        self.assertIn('<span class="metric-top">DAGs</span><span class="metric-value">1</span>', body)
        body, _ = self.page('/app?view=settings&airflow=all')  # …until changed there.
        self.assertIn('<span class="saved-current">All Airflows</span>', body)

    def test_custom_s3_folder_profile_and_environment_are_used_and_remembered(self):
        write_export(self.aws.s3, datetime.now(timezone.utc))
        body, _ = self.page('/')
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        self.page('/import', dict(csrf=csrf, s3_uri='s3://other-bucket.example/exports/lineage', stage='uat'))
        body, url = self.wait(csrf)
        self.assertTrue(url.endswith('/app'), url)
        self.assertTrue(any(c.startswith('s3 sync s3://other-bucket.example/exports/lineage/ ') for c in self.aws.calls()))
        self.page('/disconnect', dict(csrf=csrf))
        start, _ = self.page('/')
        self.assertIn('value="s3://other-bucket.example/exports/lineage/"', start)
        self.assertTrue(any(c.endswith('--profile nonprod_DP_DataEngineerOS --only-show-errors --no-progress') for c in self.aws.calls()))

    def test_settings_lists_every_saved_date_with_its_collection_time_and_switches_dates(self):
        now = datetime.now(timezone.utc)
        for moment in (now, now - timedelta(days=1)):
            write_export(self.aws.s3, moment)
        body, _ = self.page('/')
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        self.page('/import', dict(csrf=csrf)); self.wait(csrf)
        today, yesterday = now.astimezone(BKK).date().isoformat(), (now - timedelta(days=1)).astimezone(BKK).date().isoformat()
        body, _ = self.page('/app?view=settings')
        self.assertIn('Data shown', body)
        self.assertRegex(body, r'<strong>Today · as of \d\d:\d\d</strong>')
        self.assertIn(f'{today} · Data Platform', body)
        self.assertIn('<span class="saved-current">Viewing</span>', body)
        self.assertRegex(body, r'<p class="saved-summary">2 dates saved · Viewing <strong>Today · as of \d\d:\d\d</strong></p>')
        self.assertIn('<ul class="saved-list" tabindex="0" aria-label="Saved dates">', body)
        self.assertIn(f'href="/app?view=settings&amp;date={yesterday}">Open</a>', body)
        body, _ = self.page(f'/app?view=settings&date={yesterday}')
        self.assertRegex(body, r'Viewing <strong>Yesterday · as of \d\d:\d\d</strong>')
        body, _ = self.page('/app?view=overview')
        self.assertIn(f'Snapshot {yesterday}', body)

    def test_invalid_s3_folder_is_rejected_on_the_first_page(self):
        body, _ = self.page('/')
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        from urllib.error import HTTPError
        with self.assertRaises(HTTPError) as error:
            self.page('/import', dict(csrf=csrf, s3_uri='https://bucket/x', stage='uat'))
        page = error.exception.read().decode(); error.exception.close()
        self.assertEqual(error.exception.code, 400)
        self.assertIn('s3://bucket/prefix/', page); self.assertIn('value="https://bucket/x"', page)
        self.assertEqual(self.aws.calls(), [])

    def test_import_failure_is_shown_and_saved_data_stays(self):
        body, _ = self.page('/')
        csrf = re.search('name="csrf" value="([^"]+)"', body)[1]
        with patch.dict(os.environ, {'FAKE_AWS_DENY_LOGIN': '1'}):
            self.page('/import', dict(csrf=csrf))
            body, _ = self.wait(csrf)
        self.assertIn('AWS sign-in did not complete', body)
        with self.assertRaises(Exception):
            self.page('/cached', dict(csrf=csrf, cache='uat:' + '0' * 64 + '@2026-01-01'))


if __name__ == '__main__':
    unittest.main()
