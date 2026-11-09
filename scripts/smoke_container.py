#!/usr/bin/env python3
"""Check a running dashboard through HTTP; optional live AI test is explicit."""
import argparse
import http.cookiejar
import io
import json
import re
import time
import zipfile
from html.parser import HTMLParser
from urllib.error import HTTPError
from urllib.parse import parse_qs,urlencode,urlsplit
from urllib.request import HTTPCookieProcessor,Request,build_opener
import xml.etree.ElementTree as ET


class Page(HTMLParser):
    def __init__(self,text):
        super().__init__();self.fields={};self.composer=False;self.wait_url='';self.feed(text)
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='form':self.composer='ai-composer' in attrs.get('class','').split()
        if self.composer and tag=='input' and attrs.get('name'):
            self.fields[attrs['name']]=attrs.get('value','')
        if tag=='meta' and attrs.get('http-equiv')=='refresh':
            self.wait_url=attrs.get('content','').partition(';url=')[2]
    def handle_endtag(self,tag):
        if tag=='form':self.composer=False


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://localhost:8766')
    parser.add_argument('--ask-ai',action='store_true',help='Send one small live OpenAI question using container runtime configuration.')
    args=parser.parse_args();base=args.url.rstrip('/')
    client=build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def get(path):return client.open(base+path,timeout=75)
    def post(path,fields):return client.open(Request(base+path,urlencode(fields).encode()),timeout=75)
    checks=[]
    with get('/health') as response:assert json.load(response)['status']=='ok'
    def open_data(csrf):
        """Import data. The image has no AWS CLI, so it opens folders copied into the volume."""
        with post('/import',dict(csrf=csrf)) as response:body=response.read().decode()
        for _ in range(60):
            if 'Import stopped' in body or '<h1>' in body and 'Importing from S3' not in body:break
            time.sleep(.5)
            with get('/progress') as response:body=response.read().decode()
        assert 'Open saved data' in body or '<h1>Overview</h1>' in body,'Import opened no data; copy dag_lineage/ into the volume.'
        with get('/app?view=overview') as response:assert '<h1>Overview</h1>' in response.read().decode()
    with get('/') as response:page=response.read().decode()
    assert 'local-cache' not in page and 'action="/import"' in page and 'name="s3_uri"' in page
    csrf=re.search('name="csrf" value="([^"]+)"',page)[1]
    open_data(csrf)
    for view in ('overview','lineage','dependency','inventory','performance','troubleshooting','health','raws','ai'):
        with get('/app?'+urlencode(dict(view=view))) as response:
            page=response.read().decode();assert response.status==200
            assert "script-src 'none'" in response.headers['Content-Security-Policy']
            assert 'aria-label="Change connection"' in page
            assert 'aria-label="Connection"' not in page and 'aria-label="Disconnect"' not in page
            if view=='ai':ai_page=page
        checks.append(view)
    for path in ('/static/style.css','/static/clicx.svg'):
        with get(path) as response:assert response.status==200;response.read()
    for path in ('/graph.svg?view=lineage','/dependency.svg?view=dependency'):
        with get(path) as response:assert ET.fromstring(response.read()).tag.endswith('svg')
    with get('/raws/download-all') as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as archive:
            assert {'manifest.json','metadata.json','monitoring-schema.json'}<=set(archive.namelist())
            assert len(json.loads(archive.read('manifest.json'))['tables'])==10  # incl. airflow_dag_links
    with get('/raws/download?table=airflow_task_runs&format=json') as response:
        task_rows=json.load(response)
        expected=sum(row['state']=='failed' for row in task_rows)
    try:
        with get('/.env') as response:response.read()
    except HTTPError as exc:assert exc.code==404;exc.close()
    else:raise AssertionError('Environment file must not be served.')
    result=dict(status='passed',views=checks,svg_exports=True,raw_downloads=True,credentials_not_served=True)
    failed_rows=[row for row in task_rows if row['state']=='failed']
    if failed_rows:
        row=failed_rows[0]
        log_query=urlencode(dict(dag=row['dag_id'],run=row['run_id'],task=row['task_id'],map=row['map_index']))
        with get('/task/log?'+log_query) as response:
            body=response.read().decode()
            assert "frame-ancestors 'self'" in response.headers['Content-Security-Policy']
            assert 'task-log-page' in body and '<script' not in body
        result['saved_task_log_viewer']=True
    with get('/export') as response:before_refresh=response.read()
    # Refresh runs Import data. The image has no AWS CLI, so it must stop clearly and keep the snapshot.
    with post('/refresh',dict(csrf=csrf,return_to='/app?view=inventory')) as response:body=response.read().decode()
    for _ in range(60):
        if 'Import stopped' in body or '<h1>DAGs</h1>' in body:break
        time.sleep(.5)
        with get('/progress') as response:body=response.read().decode()
    stopped='Import stopped' in body
    assert stopped or '<h1>DAGs</h1>' in body,'Refresh/import did not finish.'
    if stopped:
        assert 'AWS' in body  # No AWS login in the container (or no network): reported, saved data kept.
        with get('/export') as response:assert response.read()==before_refresh
    result['local_refresh']='import stopped (AWS not available in this container); snapshot unchanged' if stopped else 're-imported from S3'
    if args.ask_ai:
        fields=Page(ai_page).fields
        fields['question']='Count task instances with state failed in this saved snapshot. Answer in one short sentence with evidence.'
        started=time.perf_counter()
        with post('/ai/ask',fields) as response:body=response.read().decode();url=response.url
        for _ in range(6):
            target=Page(body).wait_url
            if not target:break
            assert urlsplit(target).path=='/ai/wait'
            with get(target) as response:body=response.read().decode();url=response.url
        else:raise AssertionError('The AI answer did not complete in time.')
        chat=parse_qs(urlsplit(url).query)['chat'][0]
        with get('/ai/export?'+urlencode(dict(chat=chat))) as response:export=json.load(response)
        answer=export['messages'][-1]
        assert answer['state']=='complete',answer['content']
        assert 'Terry AI' in body and 'Evidence' in body
        assert any(source['rows']==[[expected]] for source in answer['evidence']), 'AI evidence does not match the downloaded raw count.'
        result['ai']=dict(status='passed',expected_failed_tasks=expected,
            requests=answer['usage']['requests'],tool_calls=answer['usage']['tool_calls'],
            model=answer['model'],elapsed_seconds=round(time.perf_counter()-started,2))
    with post('/disconnect',dict(csrf=csrf)) as response:setup=response.read().decode()
    csrf=re.search('name="csrf" value="([^"]+)"',setup)[1]
    with get('/app') as response:assert urlsplit(response.url).path=='/'
    open_data(csrf)
    if args.ask_ai:
        with get('/ai/export?'+urlencode(dict(chat=chat))) as response:
            assert json.load(response)['messages']==export['messages']
    result['change_connection']='session ended; saved workspace reopened'
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
