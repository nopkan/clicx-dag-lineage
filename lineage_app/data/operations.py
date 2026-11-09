"""Optional, read-only operational API snapshots. No browser-time API requests."""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

TABLES={
    'health':('airflow_system_health','System health'),
    'import_errors':('airflow_import_errors','Import errors'),
    'diagnostics':('airflow_task_diagnostics','Task diagnostics'),
}

def collect_operations(client,instances,now,lower,upper):
    """Optional failures are recorded separately from core snapshot coverage."""
    data={k:[] for k in TABLES};coverage={}
    def attempt(fn):
        try:return fn(),{'state':'complete'}
        except (RuntimeError,ValueError,OSError,KeyError,TypeError) as exc:
            if client.cancel.is_set():raise RuntimeError('Collection cancelled.') from None
            code=getattr(exc,'status',None)
            if code==401:raise
            # Never save response bodies, URLs with paging tokens, or credentials.
            message={401:'Token unavailable',403:'Permission denied',404:'Endpoint unavailable'}.get(code,'Resource unavailable')
            return None,dict(state='unavailable',message=message,http_status=code)
    specs={
        'health':lambda:[client.get('/api/v2/monitor/health')],
        'import_errors':lambda:client.pages('/api/v2/importErrors','import_errors',lambda r:r['import_error_id']),
    }
    client.progress('Reading health and import errors…')
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(attempt,fn):key for key,fn in specs.items()}
        for future in as_completed(futures):
            key=futures[future];rows,state=future.result();data[key]=rows or []
            coverage[key]={**state,'rows':len(data[key]),'collected_at':now.isoformat()}
    selected=sorted((r for r in instances if r.get('state')=='failed'),key=lambda r:(r['dag_id'],r['run_id'],r['task_id'],r['map_index']))
    def inspect(row):
        dag,run,task=(quote(str(row[k]),safe='') for k in ('dag_id','run_id','task_id'))
        path=f'/api/v2/dags/{dag}/dagRuns/{run}/taskInstances/{task}'
        mapped=f'{path}/{row["map_index"]}'
        result={k:row.get(k) for k in ('dag_id','run_id','task_id','map_index','try_number','state')}
        tries,state=attempt(lambda:client.pages(mapped+'/tries','task_instances',lambda r:(r['try_number'],r['map_index'])))
        # All attempts belong to a run in the pinned daily snapshot, including
        # retries spanning midnight. No unrelated historical runs are requested.
        result['tries']=tries or []
        result['tries_status']=state
        numbers={t.get('try_number') for t in result['tries']}
        numbers.add(row.get('try_number'))
        current=row.get('try_number')
        if type(current) is int and current>0:numbers.update(range(1,current+1))
        numbers=sorted(n for n in numbers if type(n) is int and n>0)
        result['logs']=[]
        for number in numbers:
            def logs():
                chunks=[];token=None;seen=set();complete=False;previous=None
                while not client.cancel.is_set():
                    params=dict(map_index=row['map_index'],full_content='true')
                    if token:params['token']=token
                    response=client.get(path+f'/logs/{number}',**params)
                    content=response.get('content',[])
                    if not isinstance(content,list):raise ValueError('Invalid logs')
                    next_token=response.get('continuation_token')
                    if token and next_token==token and content==previous:break
                    chunks.extend(format_log(v) for v in content)
                    previous=content
                    if not next_token:complete=True;break
                    if next_token in seen or next_token==token:break
                    # A running task can keep advertising a cursor after the
                    # currently available lines have been consumed. Do not tail
                    # forever; record that it can grow after this collection.
                    if token and not content:break
                    seen.add(next_token);token=next_token
                if client.cancel.is_set():raise RuntimeError('Collection cancelled.')
                text='\n'.join(chunks)
                return dict(text=text.replace(client.token,'[redacted]') if client.token else text,
                    truncated=not complete,try_number=number)
            log,state=attempt(logs)
            result['logs'].append(dict(try_number=number,log=log,status=state))
        return result
    client.progress(f'Reading failed task logs · {len(selected):,} failed tasks…')
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures=[pool.submit(inspect,row) for row in selected]
        for i,future in enumerate(as_completed(futures),1):
            data['diagnostics'].append(future.result())
            client.progress(f'Reading failed task logs · {i:,} / {len(selected):,} failed tasks')
    data['diagnostics'].sort(key=lambda r:(r['dag_id'],r['run_id'],r['task_id'],r['map_index']))
    logs=[log for row in data['diagnostics'] for log in row['logs']]
    unavailable=sum(log['status']['state']!='complete' for log in logs)
    incomplete=sum(bool((log.get('log') or {}).get('truncated')) for log in logs)
    missing_tries=sum(row['tries_status']['state']!='complete' for row in data['diagnostics'])
    coverage['diagnostics']=dict(state='partial' if unavailable or incomplete or missing_tries else 'complete',
        rows=len(selected),eligible=len(selected),scope='failed_tasks',log_attempts=len(logs),unavailable_logs=unavailable,
        incomplete_logs=incomplete,collected_at=now.isoformat())
    return data,coverage

def format_log(value):
    """Readable structured Airflow lines, retaining all additional fields."""
    if not isinstance(value,dict):return str(value)
    if 'event' not in value and 'message' not in value:return json.dumps(value,ensure_ascii=False)
    stamp=value.get('timestamp') or ''
    level=str(value.get('level') or '').upper()
    message=value.get('event',value.get('message',''))
    if not isinstance(message,str):message=json.dumps(message,ensure_ascii=False)
    rest={k:v for k,v in value.items() if k not in ('timestamp','level','event','message')}
    return (f'[{stamp}] ' if stamp else '')+(f'{level} - ' if level else '')+message+(' '+json.dumps(rest,ensure_ascii=False) if rest else '')
