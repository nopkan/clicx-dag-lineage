"""Native chat forms, ownership, downloads and async status using a fake OpenAI client."""
import http.cookiejar
import html
import json
import re
import secrets
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode,parse_qs,urlsplit
from urllib.request import build_opener,HTTPCookieProcessor,Request
from unittest.mock import patch

from lineage_app import server as app
from lineage_app.ai.chat import AISettings,ChatService
from sample_data import sample_collection
from lineage_app.data.parquet import ParquetStore
from test_ai import FakeOpenAI

class AIHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.prior_root=app.DATA_ROOT;app.DATA_ROOT=Path(cls.tmp.name)
        app.STORES.clear();app.AI_SERVICES.clear();app.SESSIONS.clear()
        cls.key=app.connections.cache_key('uat','https://airflow.example.com');cls.store=app.store_for(cls.key);cls.result=sample_collection();cls.store.save_collection(cls.result,'https://airflow.example.com','uat')
        cls.fake=FakeOpenAI();cls.service=ChatService(app.DATA_ROOT/'ai'/'conversations.sqlite3',AISettings('fixture-api-key'),cls.fake)
        app.AI_SERVICES[(app.DATA_ROOT/'ai'/'conversations.sqlite3').resolve()]=cls.service
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),app.Handler)
        threading.Thread(target=cls.server.serve_forever,daemon=True).start()
        cls.base=f'http://127.0.0.1:{cls.server.server_port}'
    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();app.DATA_ROOT=cls.prior_root
        app.STORES.clear();app.AI_SERVICES.clear();app.SESSIONS.clear();cls.tmp.cleanup()
    def setUp(self):
        self.jar=http.cookiejar.CookieJar();self.client=build_opener(HTTPCookieProcessor(self.jar))
        with self.client.open(self.base+'/') as r:body=r.read().decode()
        self.csrf=re.search('name="csrf" value="([^"]+)"',body)[1]
        with self.post('/cached',dict(csrf=self.csrf,cache=self.key+'@'+self.result['snapshot_date'].isoformat())) as r:r.read()
    def get(self,path):return self.client.open(self.base+path)
    def post(self,path,data):return self.client.open(Request(self.base+path,urlencode(data).encode()))
    def test_chat_ask_followup_export_and_gets_do_not_reissue_model_calls(self):
        with self.get('/app?view=ai') as response:
            body=response.read().decode();self.assertIn('Ask your data.',body)
            self.assertNotIn('fixture-api-key',body)
            self.assertIn("script-src 'none'",response.headers['Content-Security-Policy'])
        files={str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')}
        request=secrets.token_hex(16)
        with self.post('/ai/ask',dict(csrf=self.csrf,question='What failed?',request_id=request,chat='')) as response:
            chat=parse_qs(urlsplit(response.url).query)['chat'][0];response.read()
        until=time.monotonic()+5
        while time.monotonic()<until:
            with self.get('/app?view=ai&chat='+chat) as response:body=response.read().decode()
            if 'There are' in body:break
            time.sleep(.01)
        else:self.fail('No rendered AI answer')
        self.assertIn('Evidence',body);self.assertIn('ai-citation',body)
        calls=len(self.fake.calls)
        with self.get('/app?view=ai&chat='+chat) as response:response.read()
        with self.post('/ai/ask',dict(csrf=self.csrf,question='What failed?',request_id=request,chat=chat)) as response:response.read()
        self.assertEqual(len(self.fake.calls),calls)
        with self.get('/ai/export?chat='+chat) as response:
            exported=json.load(response);self.assertEqual(len(exported['messages']),2)
            self.assertIn('attachment;',response.headers['Content-Disposition'])
            self.assertNotIn('fixture-api-key',json.dumps(exported))
        self.assertEqual(files,{str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')})
        # A different browser cannot see the first browser's conversation or export.
        other=build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
        with other.open(self.base+'/') as response:csrf=re.search('name="csrf" value="([^"]+)"',response.read().decode())[1]
        with other.open(Request(self.base+'/cached',urlencode(dict(csrf=csrf,cache=self.key+'@'+self.result['snapshot_date'].isoformat())).encode())) as response:response.read()
        with other.open(self.base+'/app?view=ai&chat='+chat) as response:
            foreign=response.read().decode();self.assertIn('Conversation not found.',foreign);self.assertNotIn('There are',foreign)
        with self.assertRaises(HTTPError) as error:other.open(self.base+'/ai/export?chat='+chat)
        self.assertEqual(error.exception.code,400);error.exception.close()
        with self.assertRaises(HTTPError) as error:other.open(self.base+'/ai/wait?chat='+chat)
        self.assertEqual(error.exception.code,400);error.exception.close()
        with self.assertRaises(HTTPError) as error:self.get('/.env')
        self.assertEqual(error.exception.code,404);error.exception.close()
        # Changing connection clears the session, but local history and Parquet
        # remain available after opening the same saved environment again.
        with self.post('/disconnect',dict(csrf=self.csrf)) as response:
            self.csrf=re.search('name="csrf" value="([^"]+)"',response.read().decode())[1]
        with self.post('/cached',dict(csrf=self.csrf,cache=self.key+'@'+self.result['snapshot_date'].isoformat())) as response:response.read()
        with self.get('/ai/export?chat='+chat) as response:self.assertEqual(len(json.load(response)['messages']),2)
        self.assertEqual(files,{str(p):p.stat().st_mtime_ns for p in Path(self.tmp.name).rglob('*.parquet')})
        self.assertEqual(len(self.fake.calls),calls)
    def test_pending_state_navigates_to_wait_and_csrf_is_required(self):
        with self.get('/app?view=ai') as response:response.read()
        owner=next(c.value for c in self.jar if c.name=='atlas_ai_owner')
        chat,answer,_=self.service.store.enqueue(owner,self.store.metadata(),'','Pending question',secrets.token_hex(16))
        with self.get('/app?view=ai&chat='+chat) as response:
            body=response.read().decode();self.assertIn('http-equiv="refresh"',body)
            self.assertIn('Thinking…',body)
            self.assertIn('content="0;url=/ai/wait?',body)
        self.service.store.fail(answer,'Fixture complete')
        with self.assertRaises(HTTPError) as error:self.post('/ai/new',dict(csrf='bad'))
        self.assertEqual(error.exception.code,403);error.exception.close()
    def test_held_request_wakes_on_completed_answer_or_error(self):
        with self.get('/app?view=ai') as response:response.read()
        owner=next(c.value for c in self.jar if c.name=='atlas_ai_owner')
        for failed in (False,True):
            with self.subTest(failed=failed):
                chat,answer,_=self.service.store.enqueue(owner,self.store.metadata(),'','Wait for this answer',secrets.token_hex(16))
                with self.get('/app?view=ai&chat='+chat) as response:body=response.read().decode()
                refresh=html.unescape(re.search(r'<meta http-equiv="refresh" content="([^"]+)"',body)[1])
                delay,target=refresh.split(';url=',1);self.assertEqual(delay,'0')
                self.assertEqual(urlsplit(target).path,'/ai/wait')
                self.assertRegex(body,r'<input[^>]*id="ai-question"[^>]*\sdisabled')
                calls=len(self.fake.calls);entered=threading.Event();done=threading.Event();result={}
                original_wait=self.service.store.wait
                def observe_wait(*args,**kwargs):
                    entered.set();return original_wait(*args,**kwargs)
                def fetch_answer():
                    try:
                        with self.get(target) as response:result.update(body=response.read().decode(),url=response.url)
                    except Exception as exc:result['error']=exc
                    finally:done.set()
                with patch.object(self.service.store,'wait',side_effect=observe_wait):
                    reader=threading.Thread(target=fetch_answer,daemon=True);reader.start()
                    try:
                        self.assertTrue(entered.wait(2));self.assertFalse(done.is_set())
                        if failed:self.service.store.fail(answer,'The saved answer failed.')
                        else:self.service.store.finish(answer,'The answer is ready automatically.',[],{},'fixture')
                        self.assertTrue(done.wait(2),'The waiting response did not wake on completion')
                        self.assertNotIn('error',result)
                        self.assertIn('The saved answer failed.' if failed else 'The answer is ready automatically.',result['body'])
                        self.assertNotIn('http-equiv="refresh"',result['body'])
                        self.assertNotRegex(result['body'],r'<input[^>]*id="ai-question"[^>]*\sdisabled')
                        self.assertNotIn('_ai_result=',result['body'])
                        final=urlsplit(result['url']);self.assertEqual(final.fragment,'ai-latest')
                        self.assertIn('_ai_result',parse_qs(final.query))
                        self.assertEqual(len(self.fake.calls),calls)
                    finally:
                        self.service.store.fail(answer,'Fixture cleanup');reader.join(2)
    def test_wait_timeout_reconnects_without_repeating_the_question(self):
        with self.get('/app?view=ai') as response:response.read()
        owner=next(c.value for c in self.jar if c.name=='atlas_ai_owner')
        chat,answer,_=self.service.store.enqueue(owner,self.store.metadata(),'','A slow answer',secrets.token_hex(16))
        original_wait=self.service.store.wait;calls=len(self.fake.calls)
        try:
            with patch.object(self.service.store,'wait',side_effect=lambda *args:original_wait(*args,timeout=0)):
                with self.get('/ai/wait?chat='+chat) as response:body=response.read().decode()
            self.assertIn('content="0;url=/ai/wait?',body)
            self.assertEqual(len(self.fake.calls),calls)
            self.assertTrue(self.service.store.page(owner,self.store.metadata(),chat)['pending'])
        finally:self.service.store.fail(answer,'Fixture cleanup')

if __name__=='__main__':unittest.main()
