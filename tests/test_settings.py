"""Display preferences survive navigation and sessions without touching data."""
import http.cookiejar
import json
import re
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor,Request,build_opener
from unittest.mock import patch

from lineage_app import server as app,preferences
from sample_data import sample_collection


class SettingsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory()
        cls.original=(app.DATA_ROOT,dict(app.STORES),dict(app.SESSIONS))
        app.DATA_ROOT=Path(cls.tmp.name);app.STORES.clear();app.SESSIONS.clear()
        cls.key=app.connections.cache_key('uat','https://airflow.example.com')
        app.store_for(cls.key).save_collection(sample_collection(),'https://airflow.example.com','uat')
        cls.web=ThreadingHTTPServer(('127.0.0.1',0),app.Handler)
        threading.Thread(target=cls.web.serve_forever,daemon=True).start()
        cls.base=f'http://127.0.0.1:{cls.web.server_port}'

    @classmethod
    def tearDownClass(cls):
        cls.web.shutdown();cls.web.server_close();cls.tmp.cleanup()
        app.DATA_ROOT=cls.original[0];app.STORES.clear();app.STORES.update(cls.original[1])
        app.SESSIONS.clear();app.SESSIONS.update(cls.original[2])

    def setUp(self):
        self.jar=http.cookiejar.CookieJar()
        self.client=build_opener(HTTPCookieProcessor(self.jar))
        self.open_workspace()

    def open_workspace(self):
        body=self.get('/')
        self.csrf=re.search(r'name="csrf" value="([^"]+)"',body)[1]
        session=next(s for s in app.SESSIONS.values() if s['csrf']==self.csrf)
        app.open_view(session,'uat',app.store_for(self.key).metadata()['snapshot_date']);session['mode']='cached'

    def get(self,path):
        with self.client.open(self.base+path) as response:return response.read().decode()

    def post(self,values):
        with self.client.open(Request(self.base+'/settings',urlencode(values,doseq=True).encode())) as response:
            self.assertIn("script-src 'none'",response.headers['Content-Security-Policy'])
            return response.read().decode()

    def nav(self,body):return re.search(r'<nav aria-label="Workspace">(.*?)</nav>',body,re.S)[1]

    def values(self,**changes):
        return {'csrf':self.csrf,**preferences.defaults(),**changes}

    def test_saved_tabs_landing_and_display_survive_session_and_do_not_change_parquet(self):
        before={p:p.stat().st_mtime_ns for p in app.DATA_ROOT.rglob('*.parquet')}
        body=self.post(self.values(tabs=['lineage','dependency'],start='dependency',density='compact',text='large'))
        self.assertIn('Settings saved',body)
        self.assertLess(body.index('value="save"'),body.index('value="reset"'))
        self.assertNotIn('aria-label="Overview"',self.nav(body))
        self.assertIn('aria-label="Lineage"',self.nav(body))
        self.assertIn('aria-label="Settings"',body)
        self.assertIn('data-density="compact" data-text="large"',body)
        self.assertIn('<h1>Dependency</h1>',self.get('/app'))
        self.get('/app?view=lineage&sidebar=collapsed')
        # A hidden tab's direct link still works; it is a navigation preference.
        body=self.get('/app?view=overview')
        self.assertIn('<h1>Overview</h1>',body)
        self.assertNotIn('aria-label="Overview"',self.nav(body))
        self.assertRegex(body,r'id="sidebar-collapsed"[^>]*checked')
        # Recreate the server's in-memory sessions; preferences remain in browser.
        app.SESSIONS.clear();self.open_workspace()
        body=self.get('/app')
        self.assertIn('<h1>Dependency</h1>',body)
        self.assertIn('data-text="large"',body)
        display=next(c for c in self.jar if c.name==preferences.COOKIE)
        self.assertIn('HttpOnly',display._rest)
        self.assertEqual(before,{p:p.stat().st_mtime_ns for p in app.DATA_ROOT.rglob('*.parquet')})

    def test_more_display_options_apply_to_every_page_and_reject_unknown_values(self):
        body=self.post(self.values(text='xlarge',accent='purple',width='centered',rows='striped',status='colorblind'))
        self.assertIn('Settings saved',body)
        for chosen in ('<option value="xlarge" selected>Extra large</option>','<option value="purple" selected>Purple</option>',
                       '<option value="centered" selected>Centred</option>','<option value="striped" selected>Striped</option>',
                       '<option value="colorblind" selected>Colour-blind friendly (blue / orange)</option>'):
            self.assertIn(chosen,body)
        page=self.get('/app?view=inventory')
        self.assertIn('data-text="xlarge" data-accent="purple" data-width="centered" data-rows="striped" data-status="colorblind"',page)
        app.SESSIONS.clear();self.open_workspace()  # stored in the browser, not the server session
        self.assertIn('data-accent="purple"',self.get('/app'))
        for bad in (dict(accent='neon'),dict(width='<b>'),dict(rows='zigzag'),dict(status='x'),dict(text='huge')):
            with self.subTest(**bad):
                with self.assertRaises(HTTPError) as error:self.post(self.values(**bad))
                self.assertEqual(error.exception.code,400);error.exception.close()
        body=self.post(dict(csrf=self.csrf,action='reset'))
        self.assertNotIn('data-accent=',body);self.assertNotIn('data-rows=',body)
        old=preferences.read('atlas_display='+__import__('urllib.parse').parse.quote(json.dumps({'tabs':['overview'],'start':'auto','density':'compact','text':'large'})))
        self.assertEqual((old['accent'],old['width'],old['rows'],old['status']),('teal','full','plain','standard'))  # older cookies keep working

    def test_sun_moon_switch_toggles_dark_mode_and_returns_to_the_same_page(self):
        page=self.get('/app?view=inventory')
        self.assertIn('class="theme-switch" name="theme" value="dark" role="switch" aria-checked="false"',page)
        self.assertNotIn('data-theme=',page)
        from urllib.parse import urlencode as enc
        with self.client.open(Request(self.base+'/theme',enc(dict(csrf=self.csrf,theme='dark',return_to='/app?view=inventory')).encode())) as response:
            body=response.read().decode();url=response.url
        self.assertTrue(url.endswith('/app?view=inventory'),url)
        self.assertIn('data-theme="dark"',body);self.assertIn('aria-checked="true"',body);self.assertIn('value="light"',body)
        self.assertIn('<option value="dark" selected>Dark</option>',self.get('/app?view=settings'))
        app.SESSIONS.clear();self.open_workspace()
        self.assertIn('data-theme="dark"',self.get('/app'))  # remembered in the browser
        with self.client.open(Request(self.base+'/theme',enc(dict(csrf=self.csrf,theme='light',return_to='https://evil.example/')).encode())) as response:
            self.assertTrue(response.url.endswith('/app'));self.assertNotIn('data-theme=',response.read().decode())
        with self.assertRaises(HTTPError) as error:
            self.client.open(Request(self.base+'/theme',enc(dict(csrf='wrong',theme='dark')).encode()))
        self.assertEqual(error.exception.code,403);error.exception.close()

    def test_hide_overview_with_automatic_landing_and_restore_all(self):
        self.post(self.values(tabs=['lineage']))
        self.assertIn('<h1>Lineage</h1>',self.get('/app'))
        # Independent browsers use defaults; no global UI configuration.
        with build_opener().open(Request(self.base+'/app',headers={'Cookie':'atlas_session='+next(c.value for c in self.jar if c.name=='atlas_session')})) as response:
            self.assertIn('aria-label="Overview"',self.nav(response.read().decode()))
        self.get('/app?view=settings&sidebar=collapsed')
        body=self.post(dict(csrf=self.csrf,action='reset'))
        self.assertIn('aria-label="Overview"',self.nav(body))
        self.assertRegex(body,r'id="sidebar-collapsed"[^>]*checked')
        self.assertNotIn('data-density=',body)
        self.assertIn('<h1>Overview</h1>',self.get('/app'))

    def test_invalid_forms_are_recoverable_without_saving_and_require_csrf(self):
        for changes in (dict(tabs=[]),dict(tabs=['lineage'],start='overview'),dict(tabs=['<script>'])):
            with self.subTest(changes=changes):
                with self.assertRaises(HTTPError) as error:self.post(self.values(**changes))
                self.assertEqual(error.exception.code,400)
                body=error.exception.read().decode()
                error.exception.close()
                self.assertIn('<h1>Settings</h1>',body)
                self.assertIn('role="alert"',body)
                self.assertNotIn('<script>',body)
                self.assertFalse(any(c.name==preferences.COOKIE for c in self.jar))
        with self.assertRaises(HTTPError) as error:self.post(self.values(csrf='wrong'))
        self.assertEqual(error.exception.code,403)
        error.exception.close()
        self.assertIn('<h1>Overview</h1>',self.get('/app'))

    def test_corrupt_or_unknown_cookie_falls_back_safely(self):
        for raw in ('atlas_display=bad','atlas_display=%7B%7D','atlas_display=%5B%5D','atlas_display='+json.dumps({'tabs':['removed']})):
            self.assertEqual(preferences.read(raw),preferences.defaults())
        self.assertEqual(preferences.read(preferences.cookie(preferences.defaults())),preferences.defaults())

    def test_tag_colors_persist_across_pages_and_sessions_and_restore_defaults(self):
        before={p:p.stat().st_mtime_ns for p in app.DATA_ROOT.rglob('*.parquet')}
        body=self.post(self.values(data_platform_color='#C04020',risk_team_color='#2277CC'))
        self.assertIn('id="settings-data-platform-color"',body)
        self.assertIn('id="settings-risk-team-color"',body)
        self.assertIn('--airflow-data-platform-accent:#c04020',body)
        self.assertIn('--airflow-risk-team-accent:#2277cc',body)
        for view in ('overview','lineage','inventory','performance','troubleshooting','health'):
            html=self.get('/app?view='+view)
            self.assertIn('--airflow-data-platform-accent:#c04020',html)
            self.assertIn('--airflow-risk-team-accent:#2277cc',html)
        app.SESSIONS.clear();self.open_workspace()
        self.assertIn('--airflow-risk-team-accent:#2277cc',self.get('/app'))
        body=self.post(dict(csrf=self.csrf,action='reset'))
        self.assertNotIn('--airflow-risk-team-accent:',body)
        self.assertIn('value="#6550a3"',body)
        self.assertEqual(before,{p:p.stat().st_mtime_ns for p in app.DATA_ROOT.rglob('*.parquet')})

    def test_old_color_preferences_migrate_and_invalid_colors_cannot_inject_css(self):
        from urllib.parse import quote
        old={**preferences.defaults(),'density':'compact'}
        for key in preferences.BADGE_COLORS:old.pop(key)
        migrated=preferences.read('atlas_display='+quote(json.dumps(old)))
        self.assertEqual(migrated['density'],'compact')
        self.assertEqual({key:migrated[key] for key in preferences.BADGE_COLORS},preferences.BADGE_COLORS)
        for color in ('red','#123','var(--bad)','#000000;display:none','"><script>'):
            with self.subTest(color=color),self.assertRaises(HTTPError) as error:
                self.post(self.values(data_platform_color=color))
            self.assertEqual(error.exception.code,400)
            body=error.exception.read().decode();error.exception.close()
            self.assertIn('Choose a valid color',body)
            self.assertNotIn('<script>',body)

    def test_light_tag_colors_keep_readable_text_contrast(self):
        def luminance(color):
            values=[int(color[i:i+2],16)/255 for i in (1,3,5)]
            return sum((c/12.92 if c<=.04045 else ((c+.055)/1.055)**2.4)*w for c,w in zip(values,(.2126,.7152,.0722)))
        for color in ('#ffffff','#ffff00','#00ff00','#ff8800','#000000','#0055cc'):
            palette=preferences.badge_palette(color)
            contrast=(luminance(palette['background'])+.05)/(luminance(palette['color'])+.05)
            self.assertGreaterEqual(contrast,4.5)

    def test_old_sidebar_choices_gain_planner_but_explicit_hidden_choice_persists(self):
        from urllib.parse import quote
        old={**preferences.defaults(),'tabs':['lineage','dependency']}
        old.pop('catalog')
        migrated=preferences.read('atlas_display='+quote(json.dumps(old)))
        self.assertEqual(migrated['tabs'],['lineage','dependency','planner'])
        without_dependency={k:v for k,v in old.items()};without_dependency['tabs']=['lineage','inventory']
        self.assertEqual(preferences.read('atlas_display='+quote(json.dumps(without_dependency)))['tabs'],['lineage','planner','inventory'])
        explicit={**preferences.defaults(),'tabs':['lineage','dependency']}
        self.assertEqual(preferences.read(preferences.cookie(explicit))['tabs'],['lineage','dependency'])

    def test_ai_landing_initializes_chat_context_without_sending_a_question(self):
        self.post(self.values(tabs=['ai','lineage'],start='ai'))
        with patch.object(app,'ai_service') as service:
            service.return_value.page.return_value=dict(history=[],chat=None,messages=[],current=True,
                pending=False,ready=True,model='fixture',catalog=[],error='')
            body=self.get('/app')
            self.assertIn('<h1>AI</h1>',body)
            service.return_value.page.assert_called_once()
            service.return_value.ask.assert_not_called()


if __name__=='__main__':unittest.main()
