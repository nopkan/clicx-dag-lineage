#!/usr/bin/env python3
"""CLICX Lineage: local HTML + Python + Parquet, with no JavaScript."""
import argparse
import base64
import hmac
import json
import secrets
import re
import threading
import time
from datetime import datetime, timezone
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit, urlencode

from lineage_app.clients.airflow import AirflowClient, APIError
from lineage_app.data.model import CONTRACT
from lineage_app.data.combined import CombinedStore
from lineage_app.data.parquet import ParquetStore
from lineage_app.views.render import TITLES, Workspace, connect_page, progress_page
from lineage_app.paths import PROJECT_ROOT,STATIC_DIR,DEFAULT_DATA_DIR
from lineage_app import preferences
from lineage_app import connections

ROOT=PROJECT_ROOT
DATA_ROOT=DEFAULT_DATA_DIR
SESSIONS={}
STORES={}
AI_SERVICES={}
BUSY=set()
LOCK=threading.RLock()
SESSION_TTL=24*3600
STAGES=connections.STAGES
KEY=re.compile(r'(uat|dev|prd):([0-9a-f]{64})(?:@([0-9]{4}-[0-9]{2}-[0-9]{2}))?')

def ai_service():
    from lineage_app.ai.chat import ChatService,settings
    path=DATA_ROOT.resolve()/'ai'/'conversations.sqlite3'
    with LOCK:
        if path not in AI_SERVICES:AI_SERVICES[path]=ChatService(path,settings(ROOT))
        return AI_SERVICES[path]

def store_for(key,base=None):
    """'stage:<source_id>' is a source's newest snapshot; '…@YYYY-MM-DD' is one saved date."""
    if base is not None:key=connections.cache_key(key,base)
    match=KEY.fullmatch(key or '')
    if not match:raise ValueError('Unknown saved source.')
    stage,sid,day=match.groups()
    with LOCK:
        source=stage+':'+sid
        if source not in STORES:STORES[source]=ParquetStore(DATA_ROOT/stage/('source_id='+sid))
        if day and key not in STORES:STORES[key]=STORES[source].at(day)
        return STORES[key]

def cached_sources():
    """Every saved source and date: newest date first, then Airflow name (Data Platform, Risk Team)."""
    result=[]
    for stage in STAGES:
        for path in sorted((DATA_ROOT/stage).glob('source_id=*')):
            if not (path.is_dir() and re.fullmatch('source_id=[0-9a-f]{64}',path.name)):continue
            source=stage+':'+path.name.removeprefix('source_id=')
            for day in store_for(source).dates():
                meta=store_for(source+'@'+day).metadata()
                if meta:result.append(dict(key=source+'@'+day,source=source,date=day,stage=stage,
                    name=meta.get('source_name') or meta.get('base_url','Airflow'),base_url=meta.get('base_url',''),
                    collected_at=meta.get('collected_at') or ''))
    result.sort(key=lambda s:s['name'])
    return sorted(result,key=lambda s:s['date'],reverse=True)

def snapshot_list(stage):
    """One entry per saved date: newest collection time across its Airflows, and which Airflows it has."""
    days={}
    for c in cached_sources():
        if c['stage']!=stage:continue
        entry=days.setdefault(c['date'],dict(date=c['date'],collected_at='',sources=[]))
        entry['collected_at']=max(entry['collected_at'],c['collected_at'])
        if c['name'] not in entry['sources']:entry['sources'].append(c['name'])
    return [days[d] for d in sorted(days,reverse=True)]

VIEW=re.compile(r'(uat|dev|prd)@([0-9]{4}-[0-9]{2}-[0-9]{2})')

def view_choices(session):
    """Saved sources for the session's environment and date (session['cache'] = 'stage@YYYY-MM-DD')."""
    match=VIEW.fullmatch(session.get('cache') or '')
    if not match:return []
    stage,day=match.groups()
    return [c for c in cached_sources() if c['stage']==stage and c['date']==day]

def current_store(session):
    """Both Airflows merged by default; session['airflow'] narrows to one source."""
    from datetime import date
    choices=view_choices(session)
    picked=[c for c in choices if c['source']==session.get('airflow')] or choices
    if not picked:return None
    return CombinedStore([(c['name'],store_for(c['key'])) for c in picked],date.fromisoformat(picked[0]['date']))

def session_store(session):
    store=current_store(session)
    if not store:raise ValueError('The saved data is no longer available. Click Import data.')
    return store

def open_view(session,stage,day,airflow='all'):
    session.update(cache=f'{stage}@{day}',stage=stage,airflow=airflow)

def apply_view_params(session,params):
    """Airflow and Date filters are sticky for the session; they are removed from page links."""
    airflow,day=params.pop('airflow',None),params.pop('date',None)
    # The Planner remembers the last DAG viewed, so leaving the page and coming back reopens it.
    if params.get('view')=='planner':
        if params.get('plan_dag'):
            session['plan_dag']=params['plan_dag'][:250]
            # An experiment belongs to its DAG: keep it while set, clear it when a DAG is opened without one.
            session['plan_exp']={k:params[k][:8] for k in ('plan_start','plan_end') if params.get(k)}
        elif session.get('plan_dag'):
            params['plan_dag']=session['plan_dag']
            params.update(session.get('plan_exp') or {})
    match=VIEW.fullmatch(session.get('cache') or '')
    if not match:return
    stage=match[1]
    if day and any(c['stage']==stage and c['date']==day for c in cached_sources()):
        session['cache']=f'{stage}@{day}'
    if airflow=='all' or (airflow and any(c['source']==airflow for c in view_choices(session))):
        session['airflow']=airflow

def sync_exports():
    """Import dag_lineage/<date>/ folders copied from dp_dag_lineage_export (see data/exports.py)."""
    from lineage_app.data import exports
    def claim(key):
        with LOCK:
            if key in BUSY:return False
            BUSY.add(key);return True
    def release(key):
        with LOCK:BUSY.discard(key)
    try:return exports.sync(DATA_ROOT,STAGES,store_for,claim,release)
    except OSError as exc:return [f'Copied exports could not be read: {exc.strerror or type(exc).__name__}.']

IMPORT_COOKIE='atlas_import'

def import_settings(cookie_header):
    """S3 folder, AWS profile and environment last used on the first page (public values only)."""
    from lineage_app.data import aws_import
    jar=cookies.SimpleCookie()
    try:
        jar.load(cookie_header or '')
        if IMPORT_COOKIE in jar and len(jar[IMPORT_COOKIE].value)<=2048:
            saved=json.loads(base64.urlsafe_b64decode(jar[IMPORT_COOKIE].value))
            return aws_import.settings(saved.get('s3_uri'),saved.get('profile'),saved.get('stage'))
    except (ValueError,TypeError,AttributeError,cookies.CookieError):pass
    return aws_import.settings()

def import_cookie(chosen):
    raw=base64.urlsafe_b64encode(json.dumps(chosen,separators=(',',':')).encode()).decode()
    return f'{IMPORT_COOKIE}={raw}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Strict'

def clear_tokens(session):
    session.pop('token',None)

def new_job(session,message,return_to):
    job={'message':message,'history':[],'done':False,'error':None,'cancel':threading.Event(),'return_to':return_to}
    session['job']=job
    def progress(text):
        with LOCK:
            job['message']=text
            job['history']=(job['history']+[text])[-8:]
    return job,progress

def start_import_job(session,return_to='/app',chosen=None):
    """AWS sign-in (browser if expired) → aws s3 sync → import → open the newest date."""
    from lineage_app.data import aws_import
    chosen=chosen or aws_import.settings()
    with LOCK:
        if 'import' in BUSY:raise ValueError('An import is already running. Please wait for it to finish.')
        BUSY.add('import')
    job,progress=new_job(session,'Starting import…',return_to)
    job.update(title='Importing from S3',stopped='Import stopped')
    current=session.get('airflow','all')
    def run():
        try:
            try:
                # Docker: each browser session signs in to AWS with its own account (private AWS folder).
                aws_import.download(DATA_ROOT,job['cancel'],progress,aws_home=aws_import.user_home(session['csrf']),**chosen);offline=None
            except aws_import.ImportFailed as exc:
                offline=str(exc)  # Still open what was downloaded before (also how Docker reads copied folders).
            progress('Importing downloaded Parquet…')
            notices=sync_exports()
            choices=cached_sources()
            if not choices:
                raise RuntimeError(offline or f'No exported dates were found in {chosen["s3_uri"]}. Run dp_dag_lineage_export first.')
            pick=choices[0]
            clear_tokens(session)
            open_view(session,pick['stage'],pick['date'],current if any(c['source']==current for c in choices) else 'all')
            session['mode']='cached'
            problems=[n for n in notices if not n.startswith('Imported ')]
            if offline:problems.insert(0,offline+f' Showing data downloaded earlier (newest {pick["date"]}).')
            if problems:job['error']=' '.join(problems);job['saved']=True
        except (aws_import.ImportFailed,RuntimeError,ValueError,OSError) as exc:
            job['error']=str(exc)
        except Exception as exc:
            job['error']=f'Import stopped ({type(exc).__name__}). Downloaded files remain in data/; click Import data to retry.'
        finally:
            job['done']=True
            with LOCK:BUSY.discard('import')
    threading.Thread(target=run,daemon=True,name='s3-import').start()

def refresh_destination(value):
    parsed=urlsplit(value or '/app')
    if parsed.scheme or parsed.netloc or parsed.path!='/app':return '/app'
    params=parse_qs(parsed.query)
    for key in ('refresh_token','_ai_result','_ai_poll'):params.pop(key,None)
    return '/app'+('?' + urlencode(params,doseq=True) if params else '')

def start_job(session,return_to='/app'):
    """Direct single-Airflow API collection (no first-page UI; used by tests and developers)."""
    clicked_at=datetime.now(timezone.utc)
    key=connections.cache_key(session['stage'],session['base'])
    with LOCK:
        if key in BUSY:raise ValueError('A collection is already running for this source. Please wait for it to finish.')
        BUSY.add(key)
    job,progress=new_job(session,'Starting collection…',return_to)
    def run():
        client=None
        try:
            client=AirflowClient(session['base'],session['token'],session['stage'],progress,job['cancel'])
            result=client.collect(collected_at=clicked_at)
            if job['cancel'].is_set():raise RuntimeError('Collection cancelled.')
            progress('Saving daily Parquet partitions and rebuilding the monitoring model…')
            store_for(key).save_collection(result,session['base'],session['stage'])
            open_view(session,session['stage'],result['snapshot_date'].isoformat(),key);session['mode']='live'
            progress('Saved. Opening the workspace from local Parquet…')
        except APIError as exc:
            job['error']=str(exc)
            if exc.status==401:
                session.pop('token',None)
                session['token_error']='Your Airflow token has expired or is invalid. Paste a new token to refresh.'
                job['auth_error']=True
        except (ValueError,RuntimeError,OSError) as exc:
            job['error']=str(exc)
        except Exception as exc:
            job['error']=f'Collection stopped while processing data ({type(exc).__name__}). Saved files remain on disk; retry collection to complete the snapshot.'
        finally:
            if client:client.clear()
            job['done']=True
            with LOCK:BUSY.discard(key)
    threading.Thread(target=run,daemon=True,name='airflow-collection').start()

class Handler(BaseHTTPRequestHandler):
    server_version='Lineage/1.0'
    def log_message(self,format,*args):
        pass  # No request headers, tokens, form contents, or private URLs in logs.

    def session(self):
        now=time.monotonic()
        with LOCK:
            for key,s in list(SESSIONS.items()):
                if now-s['seen']>SESSION_TTL:
                    clear_tokens(s)
                    if s.get('job'):s['job']['cancel'].set()
                    del SESSIONS[key]
            jar=cookies.SimpleCookie()
            try:jar.load(self.headers.get('Cookie',''))
            except cookies.CookieError:pass
            sid=jar['atlas_session'].value if 'atlas_session' in jar else ''
            if sid not in SESSIONS:
                sid=secrets.token_urlsafe(32)
                SESSIONS[sid]={'csrf':secrets.token_urlsafe(32),'seen':now}
            SESSIONS[sid]['seen']=now
            return sid,SESSIONS[sid]

    def send(self,body,status=200,mime='text/html; charset=utf-8',extra=None,embedded=False):
        body=body.encode() if isinstance(body,str) else body
        self.send_response(status)
        self.send_header('Content-Type',mime);self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Referrer-Policy','no-referrer')
        ancestors="'self'" if embedded else "'none'"
        self.send_header('Content-Security-Policy',"default-src 'none'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors "+ancestors+"; script-src 'none'")
        if hasattr(self,'sid'):self.send_header('Set-Cookie',f'atlas_session={self.sid}; Path=/; HttpOnly; SameSite=Strict')
        if getattr(self,'new_ai_owner',None):self.send_header('Set-Cookie',f'atlas_ai_owner={self.new_ai_owner}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Strict')
        for k,v in (extra or {}).items():self.send_header(k,v)
        self.end_headers();self.wfile.write(body)

    def redirect(self,url):self.send('',303,extra={'Location':url})
    def connection_page(self,session,error='',values=None):
        return connect_page(session['csrf'],error,values or import_settings(self.headers.get('Cookie','')))
    def valid_host(self):
        # Docker may publish a different host port than the container listens on.
        # Keep the hostname restricted to loopback, while accepting that mapping.
        host=re.fullmatch(r'(?:127\.0\.0\.1|localhost)(?::([0-9]{1,5}))?',self.headers.get('Host',''),re.I)
        if not host or (host[1] is not None and not 1<=int(host[1])<=65535):
            self.send('Use this app through its localhost URL.',403,mime='text/plain')
            return False
        return True
    def ai_owner(self):
        if getattr(self,'_ai_owner',None):return self._ai_owner
        jar=cookies.SimpleCookie()
        try:jar.load(self.headers.get('Cookie',''))
        except cookies.CookieError:pass
        owner=jar['atlas_ai_owner'].value if 'atlas_ai_owner' in jar else ''
        if not re.fullmatch(r'[0-9a-f]{64}',owner):owner=secrets.token_hex(32);self.new_ai_owner=owner
        self._ai_owner=owner
        return owner
    def workspace(self,session,params,include_diagnostics=False):
        if not session.get('cache'):return None
        apply_view_params(session,params)
        store=current_store(session)
        data=store.load(include_diagnostics=include_diagnostics) if store else None
        jar=cookies.SimpleCookie()
        try:jar.load(self.headers.get('Cookie',''))
        except cookies.CookieError:pass
        compact='atlas_sidebar' in jar and jar['atlas_sidebar'].value=='collapsed'
        display=preferences.read(self.headers.get('Cookie',''))
        landing=display['tabs'][0] if display['start']=='auto' else display['start']
        view=params.get('view',landing)
        if view not in (*TITLES,'timeline','analysis','schema'):view=landing
        ai_state=None
        if data and view=='ai':
            service=ai_service();owner=self.ai_owner()
            try:ai_state=service.page(owner,store,params.get('chat',''))
            except ValueError as exc:ai_state=service.page(owner,store);ai_state['error']=str(exc)
            ai_state['error']=session.pop('ai_error',ai_state.get('error',''))
        prompt={'error':session.get('token_error','')} if params.pop('refresh_token',None)=='1' else None
        choices=view_choices(session)
        stage=session['cache'].partition('@')[0]
        filters=dict(airflow=session.get('airflow','all') if any(c['source']==session.get('airflow') for c in choices) else 'all',
            airflows=[(c['source'],c['name']) for c in choices],
            dates=sorted({c['date'] for c in cached_sources() if c['stage']==stage},reverse=True),
            snapshots=snapshot_list(stage))
        return Workspace(data,params,session['csrf'],session.get('mode','cached'),raw_store=store,sidebar_collapsed=compact,ai_state=ai_state,refresh_prompt=prompt,preferences=display,
            source_filters=filters) if data else None

    def do_GET(self):
        if not self.valid_host():return
        self.sid,session=self.session()
        parsed=urlsplit(self.path);path=parsed.path
        params={k:','.join(v) if k=='expanded' else v[-1] for k,v in parse_qs(parsed.query).items()}
        try:
            if path=='/app' and params.get('sidebar') in ('collapsed','expanded'):
                preference=params.pop('sidebar')
                destination='/app'+('?' + urlencode(params) if params else '')
                self.send('',303,extra={'Location':destination,
                    'Set-Cookie':f'atlas_sidebar={preference}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Strict'})
                return
            if path=='/':self.send(self.connection_page(session))
            elif path=='/static/style.css':self.send((STATIC_DIR/'style.css').read_bytes(),mime='text/css; charset=utf-8')
            elif path=='/static/clicx.svg':self.send((STATIC_DIR/'clicx.svg').read_bytes(),mime='image/svg+xml')
            elif path=='/favicon.ico':self.send(b'',204,mime='image/x-icon')
            elif path=='/health':self.send(json.dumps({'status':'ok','renderer':'python','storage':'parquet'}),mime='application/json')
            elif path=='/schema.json':self.send(json.dumps(CONTRACT,indent=2),mime='application/json',extra={'Content-Disposition':'attachment; filename="monitoring-schema.json"'})
            elif path in ('/planner/export','/planner.svg'):
                workspace=self.workspace(session,{**params,'view':'planner'})
                if not workspace:self.redirect('/');return
                from lineage_app.views.planner import export_plan,planner_svg
                from lineage_app.data.planner import plan_model
                try:
                    if path=='/planner.svg':
                        self.send(planner_svg(plan_model(workspace),interactive=False,only_conflicts=workspace.params.get('plan_show')=='conflicts')[0],mime='image/svg+xml',extra={'Content-Disposition':'attachment; filename="schedule-plan.svg"'})
                    else:
                        self.send(json.dumps(export_plan(workspace),default=lambda v:v.isoformat(),indent=2),mime='application/json',extra={'Content-Disposition':'attachment; filename="schedule-plan.json"'})
                except ValueError as exc:self.send(str(exc),400,mime='text/plain')
            elif path in ('/task/log','/task/log/download'):
                if not session.get('cache'):self.send('Open a workspace first.',401,mime='text/plain',embedded=True);return
                from lineage_app.views.task_logs import task_log
                store=session_store(session)
                with store.lock:
                    try:body=task_log(store,params,download=path.endswith('/download'))
                    except ValueError as exc:self.send(str(exc),404,mime='text/plain',embedded=True);return
                if path.endswith('/download'):
                    self.send(body,mime='text/plain; charset=utf-8',extra={'Content-Disposition':'attachment; filename="task-log.txt"'})
                else:self.send(body,embedded=True)
            elif path=='/ai/wait':
                if not session.get('cache'):self.redirect('/');return
                stage=session_store(session).metadata()['stage']
                chat_id=params.get('chat','')
                # The browser holds this request while the existing answer runs.
                # Completion/failure wakes it immediately; no model call is made.
                ai_service().store.wait(self.ai_owner(),chat_id,stage)
                self.redirect('/app?'+urlencode(dict(view='ai',chat=chat_id,_ai_result=secrets.token_hex(8)))+'#ai-latest')
            elif path=='/ai/export':
                if not session.get('cache'):self.redirect('/');return
                result=ai_service().store.export(self.ai_owner(),params.get('chat',''))
                if result['stage']!=session_store(session).metadata()['stage']:raise ValueError('Open this conversation in its original environment.')
                self.send(json.dumps(result,ensure_ascii=False,indent=2),mime='application/json; charset=utf-8',extra={'Content-Disposition':'attachment; filename="airflow-conversation.json"'})
            elif path=='/progress':
                job=session.get('job')
                if not job:self.redirect('/')
                elif job['done'] and job.get('auth_error'):
                    if session.get('cache'):
                        destination=job['return_to']
                        self.redirect(destination+('&' if '?' in destination else '?')+'refresh_token=1')
                    else:self.send(self.connection_page(session,session['token_error']))
                elif job['done'] and not job['error']:self.redirect(job['return_to'])
                else:self.send(progress_page(session['csrf'],job))
            elif path in ('/raws/download','/raws/download-all'):
                if not session.get('cache'):self.redirect('/');return
                from lineage_app.data.raw import RawData
                store=session_store(session);meta=store.metadata()
                if not meta:self.redirect('/');return
                with store.lock:
                    source=RawData(store,meta)
                    body,name,mime=source.bundle() if path=='/raws/download-all' else source.download(params.get('table',''),params.get('format','parquet'))
                self.send(body,mime=mime,extra={'Content-Disposition':f'attachment; filename="{name}"'})
            elif path=='/dependency/find' or (path=='/app' and params.get('view') in ('dependency','timeline') and params.get('scope')!='tasks' and params.get('q')):
                from lineage_app.views.dependency import find_dependency,dependency_anchor,dependency_model
                query=params.get('dag_search',params.get('q','')).strip()[:250]
                context={'view':'dependency',**{k:v for k,v in params.items() if k in ('zoom','depth','dependency_fullscreen')}}
                if not query:self.redirect('/app?'+urlencode(context));return
                workspace=self.workspace(session,{**context,'dag_search':query})
                if not workspace:self.redirect('/');return
                matches=find_dependency(workspace,query)
                if len(matches)==1:
                    model=dependency_model(workspace)
                    dag=matches[0]['dag_id']
                    if dag in model['nodes']:
                        self.redirect('/app?'+urlencode(dict(context,focus=dag,_dag_jump=secrets.token_hex(8)))+'#'+dependency_anchor(dag));return
                workspace.dependency_search={'matches':matches}
                self.send(workspace.render())
            elif path in ('/app','/graph.svg','/dependency.svg','/export'):
                workspace=self.workspace(session,params,include_diagnostics=path=='/export')
                if not workspace:self.redirect('/');return
                if path=='/app':self.send(workspace.render())
                elif path=='/dependency.svg':
                    from lineage_app.views.dependency import dependency_svg
                    self.send(dependency_svg(workspace),mime='image/svg+xml',extra={'Content-Disposition':'attachment; filename="dag-dependency.svg"'})
                elif path=='/graph.svg':
                    svg=workspace.graph_svg()[0]
                    if not svg.startswith('<svg'):raise ValueError('No graph available for this DAG.')
                    self.send(svg,mime='image/svg+xml',extra={'Content-Disposition':'attachment; filename="task-lineage.svg"'})
                else:
                    data=workspace.data
                    snapshot=dict(schema_version=CONTRACT['version'],source='local_parquet',metadata=data['metadata'],
                        inventory=data['inventory'],definitions=data['definitions'],dependencies=data['dependencies'],
                        monitoring_rows=workspace.rows,operations=data.get('operations',{}))
                    self.send(json.dumps(snapshot,default=lambda v:v.isoformat(),indent=2),mime='application/json',
                              extra={'Content-Disposition':'attachment; filename="airflow-snapshot.json"'})
            else:self.send('Not found',404,mime='text/plain')
        except (BrokenPipeError,ConnectionResetError):return
        except (ValueError,OSError) as exc:self.send(self.connection_page(session,str(exc)),400)

    def do_POST(self):
        if not self.valid_host():return
        self.sid,session=self.session()
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=196608:raise ValueError('Invalid form size.')
            submitted=parse_qs(self.rfile.read(length).decode())
            form={k:v[-1] for k,v in submitted.items()}
            if not hmac.compare_digest(form.get('csrf',''),session['csrf']):
                self.send('This form expired. Reload the page and try again.',403,mime='text/plain');return
            if session.get('job') and not session['job']['done'] and self.path not in ('/cancel','/disconnect'):
                self.redirect('/progress');return
            if self.path=='/import':
                from lineage_app.data import aws_import
                try:chosen=aws_import.settings(form.get('s3_uri'),None,form.get('stage'))  # Profile: LINEAGE_AWS_PROFILE.
                except ValueError as exc:
                    typed={k:form.get(k,'')[:300] for k in ('s3_uri','stage')}
                    self.send(self.connection_page(session,str(exc),typed),400);return
                start_import_job(session,'/app',chosen)
                self.send('',303,extra={'Location':'/progress','Set-Cookie':import_cookie(chosen)})
            elif self.path=='/settings':
                if not session.get('cache'):self.redirect('/');return
                draft=dict(tabs=submitted.get('tabs',[]),start=form.get('start'),density=form.get('density'),text=form.get('text'),
                           **{key:form.get(key) for key in preferences.LOOK})
                current=preferences.read(self.headers.get('Cookie',''))
                draft.update({key:form.get(key,current[key]) for key in preferences.BADGE_COLORS})
                try:
                    value=preferences.defaults() if form.get('action')=='reset' else preferences.validate(draft)
                except ValueError as exc:
                    workspace=self.workspace(session,{'view':'settings'})
                    if not workspace:self.redirect('/');return
                    workspace.settings_draft=draft;workspace.settings_error=str(exc)
                    self.send(workspace.render(),400);return
                self.send('',303,extra={'Location':'/app?view=settings&saved=1','Set-Cookie':preferences.cookie(value)})
            elif self.path in ('/ai/ask','/ai/new'):
                if not session.get('cache'):self.redirect('/');return
                store=session_store(session);service=ai_service();owner=self.ai_owner()
                if self.path=='/ai/new':chat_id=service.store.new(owner,store.metadata())
                else:chat_id=service.ask(owner,store,form.get('chat',''),form.get('question',''),form.get('request_id',''))
                self.redirect('/app?'+urlencode(dict(view='ai',chat=chat_id))+'#ai-latest')
            elif self.path=='/connect':
                stage=form.get('stage','uat')
                if stage not in STAGES:raise ValueError('Choose a supported environment.')
                base=form.get('base','').strip().rstrip('/');token=form.get('token','').strip()
                client=AirflowClient(base,token,stage);client.clear()
                session.update(stage=stage,base=base,token=token)
                session.pop('token_error',None)
                start_job(session);self.redirect('/progress')
            elif self.path=='/cached':
                # 'stage@YYYY-MM-DD' opens both Airflows; 'stage:<source_id>@YYYY-MM-DD' opens one.
                key=form.get('cache','');source,_,day=key.partition('@')
                choice=next((c for c in cached_sources() if c['date']==day and (c['stage']==source or c['source']==source)),None)
                if not choice:raise ValueError('Choose a saved source and date.')
                clear_tokens(session)
                open_view(session,choice['stage'],day,choice['source'] if ':' in source else 'all');session['mode']='cached'
                self.redirect(refresh_destination(form.get('return_to')))
            elif self.path=='/theme':
                # Sun/moon switch in the top bar: saves light or dark with the other display settings.
                value=preferences.read(self.headers.get('Cookie',''))
                value['theme']='dark' if form.get('theme')=='dark' else 'light'
                self.send('',303,extra={'Location':refresh_destination(form.get('return_to')),'Set-Cookie':preferences.cookie(value)})
            elif self.path=='/refresh':
                if not session.get('cache'):self.redirect('/');return
                destination=refresh_destination(form.get('return_to'))
                if session.get('mode')!='live':
                    start_import_job(session,destination,import_settings(self.headers.get('Cookie','')));self.redirect('/progress');return
                token=form.get('token','').strip()
                if token:
                    client=AirflowClient(session['base'],token,session['stage']);client.clear()
                    session['token']=token;session.pop('token_error',None)
                if session.get('token'):start_job(session,destination);self.redirect('/progress')
                else:self.redirect(destination+('&' if '?' in destination else '?')+'refresh_token=1')
            elif self.path in ('/disconnect','/cancel'):
                if session.get('job'):session['job']['cancel'].set()
                clear_tokens(session)
                if self.path=='/disconnect':
                    with LOCK:SESSIONS.pop(self.sid,None)
                self.redirect('/')
            else:self.send('Not found',404,mime='text/plain')
        except (ValueError,OSError) as exc:
            if self.path=='/refresh' and session.get('cache'):
                session['token_error']=str(exc)
                destination=refresh_destination(locals().get('form',{}).get('return_to'))
                self.redirect(destination+('&' if '?' in destination else '?')+'refresh_token=1');return
            if self.path.startswith('/ai/'):
                session['ai_error']=str(exc);chat=locals().get('form',{}).get('chat','')
                self.redirect('/app?'+urlencode({**dict(view='ai'),**({'chat':chat} if re.fullmatch(r'[0-9a-f]{32}',chat) else {})}));return
            self.send(self.connection_page(session,str(exc)),400)

def main():
    global DATA_ROOT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port',type=int,default=8765)
    parser.add_argument('--data-dir',type=Path,default=DEFAULT_DATA_DIR)
    parser.add_argument('--host',default='127.0.0.1',help='Bind address; use 0.0.0.0 inside Docker.')
    args=parser.parse_args();DATA_ROOT=args.data_dir.resolve()
    server=ThreadingHTTPServer((args.host,args.port),Handler)
    print(f'CLICX Lineage: http://127.0.0.1:{server.server_port}',flush=True)
    print(f'Local Parquet: {DATA_ROOT}',flush=True)
    print('HTML + Python. No JavaScript. Press Ctrl+C to stop.',flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:
        for s in SESSIONS.values():
            clear_tokens(s)
            if s.get('job'):s['job']['cancel'].set()
        server.server_close()

if __name__=='__main__':main()
