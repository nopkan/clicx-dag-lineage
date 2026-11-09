"""Read-only Airflow 3 API adapter. No credentials are written to disk."""
import json
import socket
import ssl
import threading
from http.client import IncompleteRead
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as day_time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from lineage_app.data.model import BKK, map_dag, map_task

class APIError(RuntimeError):
    def __init__(self, status, path):
        self.status = status
        explanation = {401:'Token expired or invalid. Paste a fresh Airflow token.',
                       403:'This token does not have access to this Airflow resource.',
                       404:'This Airflow resource is unavailable.',
                       429:'Airflow rate limit reached. Please retry shortly.'}.get(status,'Airflow request failed.')
        super().__init__(f'{explanation} HTTP {status} · {path.split("?")[0]}')

class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def connection_failure(exc):
    """Describe network failures without echoing URLs, headers or credentials."""
    reason=exc.reason if isinstance(exc,URLError) else exc
    if isinstance(reason,ssl.SSLCertVerificationError):
        return 'Airflow TLS certificate verification failed. Check the trusted certificate or corporate proxy certificate.',False
    if isinstance(reason,socket.gaierror):
        return 'Cannot resolve the Airflow hostname. Check the URL, DNS and VPN.',True
    if isinstance(reason,TimeoutError):
        return 'Airflow connection timed out after 35 seconds. Retry collection or check the network/VPN.',True
    if isinstance(reason,ConnectionRefusedError):
        return 'Airflow refused the connection. Check the host, port and VPN.',True
    if isinstance(reason,(ConnectionError,IncompleteRead)):
        return 'Airflow closed or interrupted the connection. Retry collection.',True
    if isinstance(reason,ssl.SSLError):
        return 'The TLS connection to Airflow failed. Check the network/proxy and TLS configuration.',True
    return 'Airflow connection failed. Check the URL and network/VPN, then retry collection.',True

class AirflowClient:
    def __init__(self, base, token, stage, progress=lambda text:None, cancel=None, allow_http=False):
        parsed = urlsplit(base)
        local = parsed.hostname in ('localhost','127.0.0.1')
        if parsed.scheme != 'https' and not (parsed.scheme=='http' and (local or allow_http)):
            raise ValueError('Use an HTTPS Airflow URL (HTTP is supported for localhost tests).')
        if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('Enter the Airflow base URL without credentials, query, or fragment.')
        if parsed.path.rstrip('/') in ('/api/v2','/auth/token'):
            raise ValueError('Enter the base URL, without /api/v2 or /auth/token.')
        self.base, self.token, self.stage = base.rstrip('/'), token.removeprefix('Bearer ').strip(), stage
        if not self.token or '\n' in self.token or '\r' in self.token:
            raise ValueError('Paste a valid Airflow bearer token.')
        self.progress, self.cancel = progress, cancel or threading.Event()
        self.auth_failed=False

    def get(self, path, _max_bytes=None, **params):
        if self.auth_failed:raise APIError(401,path)
        if self.cancel.is_set(): raise RuntimeError('Collection cancelled.')
        url = self.base+path+('?' + urlencode(params) if params else '')
        request = Request(url, headers={'Authorization':'Bearer '+self.token,'Accept':'application/json'})
        for attempt in range(3):
            if self.cancel.is_set():raise RuntimeError('Collection cancelled.')
            if self.auth_failed:raise APIError(401,path)
            try:
                with build_opener(NoRedirects()).open(request, timeout=35) as response:
                    if _max_bytes:
                        payload=response.read(_max_bytes+1)
                        if len(payload)>_max_bytes:raise ValueError('Response exceeds preview limit.')
                        data=json.loads(payload)
                    else:data = json.load(response)
                if not isinstance(data,dict): raise ValueError('Airflow returned an invalid JSON object.')
                return data
            except HTTPError as exc:
                code=exc.code
                exc.close()
                if code==401:self.auth_failed=True
                if code in (429,502,503,504) and attempt < 2:
                    if self.cancel.wait(attempt+1): raise RuntimeError('Collection cancelled.')
                    continue
                raise APIError(code,path) from None
            except (URLError,OSError,IncompleteRead) as exc:
                message,retry=connection_failure(exc)
                if retry and attempt<2:
                    if self.cancel.wait(attempt+1):raise RuntimeError('Collection cancelled.')
                    continue
                raise RuntimeError(message) from None
        raise RuntimeError('Airflow request failed.')

    def pages(self, path, collection, identity, **params):
        rows, seen, offset = [], set(), 0
        while True:
            data = self.get(path,limit=100,offset=offset,**params)
            page, total = data.get(collection), data.get('total_entries')
            if not isinstance(page,list) or type(total) is not int or total < 0:
                raise ValueError(f'Invalid {collection} collection response.')
            for row in page:
                if not isinstance(row,dict): raise ValueError(f'Invalid {collection} record.')
                key = identity(row)
                if key in seen: raise ValueError(f'{collection} changed during pagination. Retry collection.')
                seen.add(key); rows.append(row)
            offset += len(page)
            if offset >= total:
                if len(rows) != total: raise ValueError(f'{collection} count changed during pagination. Retry collection.')
                return rows
            if not page: raise ValueError(f'Incomplete {collection} pagination. Retry collection.')

    def collect(self, *, collected_at=None):
        # Resolve the Bangkok day once at Connect/Refresh, even if collection crosses midnight.
        now = collected_at or datetime.now(timezone.utc)
        today = now.astimezone(BKK).date()
        lower = datetime.combine(today,day_time.min,BKK).astimezone(timezone.utc)
        upper = datetime.combine(today+timedelta(days=1),day_time.min,BKK).astimezone(timezone.utc)
        warnings = []
        self.progress('Discovering all accessible DAGs…')
        inventory = self.pages('/api/v2/dags','dags',lambda d:d['dag_id'],order_by='dag_id',exclude_stale='false')
        details, definitions, dependencies = {}, [], []

        def inspect(dag):
            dag_id=dag['dag_id']; encoded=quote(dag_id,safe='')
            if dag.get('is_stale'): return dag,[],[]
            notes=[]
            try: extra=self.get(f'/api/v2/dags/{encoded}/details')
            except APIError as exc:
                if exc.status in (401,403): raise
                extra={};notes.append('DAG details unavailable: '+dag_id)
            if extra and extra.get('dag_id') != dag_id: raise ValueError('Mismatched DAG details response.')
            try:
                data=self.get(f'/api/v2/dags/{encoded}/tasks')
                tasks=data.get('tasks')
                if not isinstance(tasks,list) or type(data.get('total_entries')) is not int or len(tasks)!=data['total_entries']:
                    raise ValueError('Incomplete task definitions for '+dag_id)
                ids={t['task_id'] for t in tasks}
                if len(ids)!=len(tasks):raise ValueError('Duplicate task definitions for '+dag_id)
                for t in tasks:
                    downstream=t.get('downstream_task_ids') or []
                    if not isinstance(downstream,list) or any(x not in ids for x in downstream):
                        raise ValueError('Invalid dependency targets for '+dag_id)
            except APIError as exc:
                if exc.status in (401,403): raise
                tasks=[];notes.append('Task definitions unavailable: '+dag_id)
            return {**dag,**extra},tasks,notes

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures={pool.submit(inspect,d):d for d in inventory}
            for i,future in enumerate(as_completed(futures),1):
                detail,tasks,notes=future.result();dag_id=detail['dag_id']
                details[dag_id]=detail;warnings.extend(notes)
                definitions.extend([{**t,'dag_id':dag_id} for t in tasks])
                edges={(t['task_id'],d) for t in tasks for d in (t.get('downstream_task_ids') or [])}
                for a,b in sorted(edges or {(None,None)}):
                    dependencies.append(dict(stage=self.stage,dag_id=dag_id,snapshot_date=today,
                        upstream_task_id=a,downstream_task_id=b,collected_at=now))
                self.progress(f'Reading DAG definitions · {i:,} / {len(inventory):,}')
        self.progress('Collecting today’s DAG runs…')
        identity=lambda r:(r['dag_id'],r['dag_run_id'])
        raw=self.pages('/api/v2/dags/~/dagRuns','dag_runs',identity,order_by='id',
                       start_date_gte=lower.isoformat(),start_date_lte=upper.isoformat())
        queued=self.pages('/api/v2/dags/~/dagRuns','dag_runs',identity,order_by='id',state='queued')
        selected={}
        for run in raw+queued:
            if run['dag_id'] not in details:continue
            row=map_dag(self.stage,run,details[run['dag_id']],now,self.base)
            if row['run_date']==today:selected[identity(run)]=row
        runs=list(selected.values());instances=[]
        indexed={(t['dag_id'],t['task_id']):t for t in definitions}
        def collect_instances(run):
            path=f"/api/v2/dags/{quote(run['dag_id'],safe='')}/dagRuns/{quote(run['run_id'],safe='')}/taskInstances"
            rows=self.pages(path,'task_instances',lambda t:(t['task_id'],t['map_index']),order_by='id')
            return [map_task(run,t,indexed.get((run['dag_id'],t['task_id']),{}),now) for t in rows]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures=[pool.submit(collect_instances,r) for r in runs]
            for i,future in enumerate(as_completed(futures),1):
                instances.extend(future.result())
                self.progress(f'Collecting task instances · {i:,} / {len(runs):,} DAG runs')
        from lineage_app.data.operations import collect_operations
        operations,operational_coverage=collect_operations(self,instances,now,lower,upper)
        return dict(collected_at=now,snapshot_date=today,start=today,end=today,
            inventory=[details[d['dag_id']] for d in inventory],definitions=definitions,
            dependencies=dependencies,dag_runs=runs,task_runs=instances,warnings=warnings,
            operations=operations,operational_coverage=operational_coverage,
            coverage=dict(dags=len(inventory),stale_dags=sum(bool(d.get('is_stale')) for d in inventory),
                          definitions=len(definitions),dag_runs=len(runs),
                          task_instances=len(instances)))

    def clear(self):
        self.token=''
