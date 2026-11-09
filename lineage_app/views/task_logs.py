"""Saved task logs in an independent HTML pane; no API calls or JavaScript."""
from urllib.parse import urlencode

from lineage_app.views.render import e,option,shell

LINES_PER_PAGE=200

def log_frame(w,task,run=None,map_index=None,dag=None):
    run=run or (w.run['run_id'] if w.run else '')
    dag=dag or w.dag_id
    failed=any(r['dag_id']==dag and r['run_id']==run and r['task_id']==task and r['task_state']=='failed'
        and (map_index is None or r['map_index']==map_index) for r in w.tasks)
    if not failed:return ''
    params=dict(dag=dag,run=run,task=task)
    if map_index is not None:params['map']=str(map_index)
    url='/task/log?'+urlencode(params)
    return (f'<div class="task-log-section"><h3>Task log</h3>'
        f'<iframe class="task-log-frame" src="{e(url)}" title="Log for {e(task)}" loading="lazy" '
        'sandbox="allow-forms allow-same-origin allow-downloads"></iframe></div>')

def saved_logs(diag):
    if 'logs' in diag:return diag['logs']
    if 'log' in diag:
        return [dict(try_number=(diag.get('log') or {}).get('try_number',diag.get('try_number')),
            log=diag.get('log'),status=diag.get('log_status',{}))]
    return []

def task_log(store,params,download=False):
    dag,run,task=(params.get(k,'') for k in ('dag','run','task'))
    meta=store.metadata()
    if not meta:raise ValueError('Load a saved snapshot first.')
    from datetime import date
    instances=[r for r in store.raw_rows('airflow_task_runs',date.fromisoformat(meta['snapshot_date']))
        if (r['dag_id'],r['run_id'],r['task_id'])==(dag,run,task)]
    if not instances:raise ValueError('No task instance in this snapshot.')
    instances=[r for r in instances if r['state']=='failed']
    if not instances:raise ValueError('Logs are available only for failed tasks.')
    diagnostics={r['map_index']:r for r in store.task_diagnostics(dag,run,task)}
    entries={}
    for row in sorted(instances,key=lambda r:r['map_index']):
        logs=saved_logs(diagnostics.get(row['map_index'],{}))
        if not logs:
            message='No execution log: this task has not started.' if not row.get('try_number') else 'Logs were not collected. Refresh with an Airflow token.'
            logs=[dict(try_number=row.get('try_number') or 0,log=None,status={'message':message})]
        for item in sorted(logs,key=lambda item:item['try_number']):
            key=f'{row["map_index"]}:{item["try_number"]}'
            caption=f'Attempt {item["try_number"]}' if item['try_number'] else 'No attempt'
            if row['map_index']!=-1:caption+=f' · Map {row["map_index"]}'
            entries[key]=dict(item,caption=caption)
    default_map=params.get('map',str(min(r['map_index'] for r in instances)))
    candidates=[k for k in entries if k.split(':')[0]==default_map]
    selected=params.get('entry')
    if selected and selected not in entries:raise ValueError('No saved log for this failed task instance and attempt.')
    if selected not in entries:selected=(candidates or list(entries))[-1]
    item=entries[selected];log=item.get('log') or {};text=log.get('text','')
    if download:
        if not log:raise ValueError(item.get('status',{}).get('message','No saved log for this attempt.'))
        return text
    fields=dict(dag=dag,run=run,task=task)
    hidden=''.join(f'<input type="hidden" name="{e(k)}" value="{e(v)}">' for k,v in fields.items())
    controls=f'<form action="/task/log" method="get">{hidden}<label class="screen-reader" for="log-entry">Attempt and map index</label><select id="log-entry" name="entry">'+''.join(option(k,v['caption'],selected) for k,v in entries.items())+'</select><button class="button small">View</button></form>'
    download_url='/task/log/download?'+urlencode(dict(fields,entry=selected))
    download_link=f'<a class="button small" href="{e(download_url)}" download>↓ Download log</a>' if log else ''
    header=f'<header class="task-log-toolbar">{controls}{download_link}</header>'
    notice=''
    if log.get('truncated'):
        notice='<p class="task-log-notice">Saved log is incomplete. Refresh to collect the available log again.</p>'
    if not text:
        message=item.get('status',{}).get('message','No log content returned for this attempt.')
        body=f'<p class="task-log-empty">{e(message)}</p>'
    else:
        lines=text.splitlines()
        pages=max(1,(len(lines)+LINES_PER_PAGE-1)//LINES_PER_PAGE)
        try:page=max(1,min(pages,int(params.get('page','1'))))
        except ValueError:page=1
        offset=(page-1)*LINES_PER_PAGE
        def link(label,target):
            url='/task/log?'+urlencode(dict(fields,entry=selected,page=target))
            return f'<a class="button small" href="{e(url)}">{label}</a>'
        navigation=(link('← Previous',page-1) if page>1 else '')+(link('Next →',page+1) if page<pages else '')
        body=f'<div class="task-log-pagination"><span>Lines {offset+1:,}–{min(offset+LINES_PER_PAGE,len(lines)):,} of {len(lines):,}</span><div>{navigation}</div></div>'
        rendered=[]
        for number,line in enumerate(lines[offset:offset+LINES_PER_PAGE],offset+1):
            tone=' log-error' if 'ERROR' in line or 'Traceback' in line else ' log-warning' if 'WARNING' in line else ''
            rendered.append(f'<div class="task-log-line{tone}"><span class="task-log-number" aria-hidden="true">{number}</span><code>{e(line) or " "}</code></div>')
        body+='<div class="task-log-lines" tabindex="0" role="region" aria-label="Saved log lines">'+''.join(rendered)+'</div>'
    return shell('Task log','<main class="task-log-page">'+header+notice+body+'</main>')
