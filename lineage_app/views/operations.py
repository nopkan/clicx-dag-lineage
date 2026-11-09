"""Compact operational views, computed exclusively from the saved daily snapshot."""
import hashlib
import json
from collections import Counter,defaultdict
from statistics import median

from lineage_app.data.model import trace
from lineage_app.data.operations import TABLES
from lineage_app.views.render import airflow_badge,e,elapsed,fmt,human,option,safe_link,status

def panel(title,body,aside=''):
    return f'<section class="panel ops-panel"><div class="panel-heading"><h2>{e(title)}</h2>{aside}</div>{body}</section>'

def empty(text):return f'<div class="empty ops-empty">{e(text)}</div>'

def metrics(items):
    return '<div class="ops-metrics">'+''.join(f'<div><span>{e(label)}</span><strong class="{tone}">{e(value)}</strong></div>' for label,value,tone in items)+'</div>'

def table(headers,rows,caption='',cls=''):
    if not rows:return empty('No matching records.')
    return f'<div class="ops-table-scroll {cls}" tabindex="0" role="region" aria-label="{e(caption or headers[0])}"><table class="ops-table"><caption class="screen-reader">{e(caption)}</caption><thead><tr>'+''.join(f'<th scope="col">{e(h)}</th>' for h in headers)+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>'

def resource(w,key):return w.data.get('operations',{}).get(key,[])

def coverage(w,key):return w.meta.get('operational_coverage',{}).get(key,{'state':'not_collected'})

def resource_notice(w,key):
    state=coverage(w,key)
    if state['state']=='complete':return ''
    if state['state']=='not_collected':text='Not collected. Connect or refresh with a token.'
    elif state['state']=='partial':
        if key=='diagnostics' and 'log_attempts' in state:
            text=f'{state.get("unavailable_logs",0):,} logs unavailable · {state.get("incomplete_logs",0):,} incomplete logs'
        else:text=f'{state.get("rows",0):,} of {state.get("eligible",0):,} tasks collected.'
    else:text=state.get('message','Resource unavailable.')+'. Reconnect to retry.'
    return f'<div class="ops-notice">{e(text)}</div>'

def raw_link(key):
    return f'<a class="ops-text-link" href="/app?view=raws&amp;raw_table={TABLES[key][0]}">Raw data ↗</a>'

def lineage_link(w,row,text='→',task=True):
    return f'<a class="ops-text-link" href="{w.url(view="lineage",dag=row["dag_id"],run=row["run_id"],task=row.get("task_id") if task else None)}">{e(text)}</a>'

def queue_wait(row):
    queued=row.get('task_queued_at_bkk');started=row.get('task_started_at_bkk')
    if row.get('task_state') in ('upstream_failed','skipped','removed') or not queued or not started:return None
    seconds=(started-queued).total_seconds()
    return seconds if seconds>=0 else None

def runtime(row,level):
    value=row.get(f'{level}_duration_seconds')
    return value if row.get(f'{level}_state') in ('success','failed') and value is not None and value>=0 else None

def performance_stats(w):
    durations=[v for t in w.tasks if (v:=runtime(t,'task')) is not None]
    waits=[v for t in w.tasks if (v:=queue_wait(t)) is not None]
    starts=Counter()
    for task in w.tasks:
        at=task.get('task_started_at_bkk')
        if at and at.date()==w.day and task.get('task_state') not in ('upstream_failed','skipped','removed'):
            starts[at.hour]+=1
    return dict(durations=durations,waits=waits,starts=starts,
        dag_runtimes=sorted([r for r in w.runs if runtime(r,'dag') is not None],key=lambda r:runtime(r,'dag'),reverse=True),
        task_runtimes=sorted([r for r in w.tasks if runtime(r,'task') is not None],key=lambda r:runtime(r,'task'),reverse=True))

def performance_run_key(row):
    return hashlib.sha256(json.dumps([row['dag_id'],row['run_id']]).encode()).hexdigest()[:20]

def performance_rankings(w,stats):
    selected_run=next((r for r in w.runs if performance_run_key(r)==w.params.get('perf_run')),None)
    selected_task=next((r for r in stats['task_runtimes'] if task_key(r)==w.params.get('perf_task')
        and (not selected_run or performance_run_key(r)==performance_run_key(selected_run))),None)
    dags=[r for r in w.runs if performance_run_key(r)==performance_run_key(selected_task)] if selected_task else stats['dag_runtimes']
    tasks=[r for r in stats['task_runtimes'] if performance_run_key(r)==performance_run_key(selected_run)] if selected_run else stats['task_runtimes']
    return dict(selected_run=selected_run,selected_task=selected_task,dag_rows=dags,task_rows=tasks)

def performance(w):
    stats=performance_stats(w)
    selection=performance_rankings(w,stats)
    selected_run=selection['selected_run'];selected_task=selection['selected_task']
    def select_url(run=None,task=None):
        return w.url(view='performance',perf_run=run,perf_task=task,dag=None,run=None,task=None,focus=None,native=None,expanded=None,scope=None)+'#performance-rankings'
    cards=metrics([('Completed runs',f'{len(stats["dag_runtimes"]):,}',''),
        ('Median task runtime',elapsed(median(stats['durations'])) if stats['durations'] else '—',''),
        ('Median queue wait',elapsed(median(stats['waits'])) if stats['waits'] else '—',''),
        ('Attempts > 1',f'{sum((r.get("try_number") or 0)>1 for r in w.tasks):,}','')])
    def ranking(rows,level):
        ceiling=max((runtime(r,level) or 0 for r in rows),default=1) or 1
        bars=[]
        for row in rows[:10]:
            name=row['dag_id'] if level=='dag' else row['task_id']
            subtitle=f'<small>{e(fmt(row.get("dag_started_at_bkk"),False))} · {e(human(row.get("run_type")))}</small>' if level=='dag' else f'<small>{e(row["dag_id"])} · map {row.get("map_index",-1)}</small>'
            duration=runtime(row,level)
            run_key=performance_run_key(row)
            if level=='dag':
                active=any(r and performance_run_key(r)==run_key for r in (selected_run,selected_task))
                url=select_url() if active else select_url(run=run_key)
                action='Clear selection' if active else 'Show tasks for this DAG run'
            else:
                active=selected_task is not None and task_key(selected_task)==task_key(row)
                url=select_url(run=performance_run_key(selected_run) if selected_run else None,task=None if active else task_key(row))
                action='Clear task selection' if active else 'Show this task’s DAG run'
            current=' aria-current="true"' if active else ''
            if duration is None:subtitle+=f'<small>{e(human(row.get(level+"_state")))}</small>'
            heading=f'<div class="ops-rank-heading"><span class="ops-rank-name">{e(name)}</span>{airflow_badge(row,metadata=w.meta)}</div>'
            bars.append(f'<a class="ops-rank{" selected" if active else ""}" href="{url}" title="{action}"{current} data-rank="{level}" data-dag="{e(row["dag_id"])}" data-run="{e(row["run_id"])}" data-task="{e(row.get("task_id") or "")}" data-map="{e(row.get("map_index"))}"><div>{heading}{subtitle}</div><strong>{elapsed(duration)}</strong><div class="ops-rank-track"><i style="width:{(duration or 0)/ceiling*100:.2f}%" class="{e(row.get(level+"_state"))}"></i></div></a>')
        return '<div class="ops-ranks">'+''.join(bars)+'</div>' if rows else empty('No completed executions.')
    peak=max(stats['starts'].values(),default=1) or 1
    hours=''.join(f'<div class="ops-hour" title="{hour:02d}:00 · {stats["starts"][hour]:,} task starts"><span>{stats["starts"][hour] or ""}</span><div><i style="height:{stats["starts"][hour]/peak*100:.2f}%"></i></div><small>{hour:02d}</small></div>' for hour in range(24))
    activity=panel('Task starts by hour',f'<div class="ops-hours" role="img" aria-label="Task starts by Bangkok hour">{hours}</div>','<span class="count">Bangkok · 00–23</span>')
    context=''
    if selected_run or selected_task:
        chosen=selected_task if selected_task is not None else selected_run
        caption=chosen['dag_id']+' · '+fmt(chosen.get('dag_started_at_bkk'),False)
        if selected_task:caption+=' · '+selected_task['task_id']+' · map '+str(selected_task['map_index'])
        context=f'<div class="performance-selection"><span>{e(caption)} {airflow_badge(chosen,metadata=w.meta)}</span><a class="button small" href="{select_url()}" aria-label="Clear performance selection">Clear ×</a></div>'
    dag_count='<span class="count">Selected run</span>' if selected_task else '<span class="count">Top 10</span>'
    ranks=f'<div id="performance-rankings">{context}<div class="ops-two">{panel("Longest DAG runs",ranking(selection["dag_rows"],"dag"),dag_count)}{panel("Longest tasks",ranking(selection["task_rows"],"task"),"<span class=\"count\">Top 10</span>")}</div></div>'
    rows=[]
    timed=sorted([r for r in w.tasks if queue_wait(r) is not None],key=lambda r:-queue_wait(r))
    for row in timed[:50]:
        rows.append(f'<tr><td>{e(row["dag_id"])}</td><td>{airflow_badge(row,metadata=w.meta)}</td><td class="ops-id">{e(row["task_id"])}</td><td>{row["map_index"]}</td><td>{e(fmt(row.get("dag_started_at_bkk"),False))}</td><td>{elapsed(queue_wait(row))}</td><td>{elapsed(runtime(row,"task"))}</td><td>{e(row.get("task_pool") or "—")}</td><td>{status(row["task_state"])}</td><td>{lineage_link(w,row)}</td></tr>')
    waits=panel('Longest queue waits',table(['DAG','Airflow','Task','Map','Run started','Wait','Runtime','Pool','State','Lineage'],rows,'Queue wait for latest saved task attempts'),f'<span class="count">Top {len(rows):,} of {len(timed):,} timed attempts</span>')
    return cards+activity+ranks+waits+'<p class="ops-footnote">Latest saved task attempts. Runtime includes completed executions only; queue wait excludes tasks without both timestamps.</p>'

def task_key(row):
    return hashlib.sha256(json.dumps([row.get(k) for k in ('dag_id','run_id','task_id','map_index')]).encode()).hexdigest()[:20]

def related_tasks(w,selected,direction):
    adjacency=defaultdict(set)
    for edge in w.data['dependencies']:
        if edge['dag_id']!=selected['dag_id']:continue
        a,b=edge['upstream_task_id'],edge['downstream_task_id']
        if a and b:adjacency[a if direction=='down' else b].add(b if direction=='down' else a)
    ids=trace(selected['task_id'],adjacency)
    return [r for r in w.tasks if r['dag_id']==selected['dag_id'] and r['run_id']==selected['run_id'] and r['task_id'] in ids]

def troubleshooting(w):
    counts=Counter(t['task_state'] for t in w.tasks)
    cards=metrics([('Failed',f'{counts["failed"]:,}','danger'),('Upstream failed',f'{counts["upstream_failed"]:,}','danger'),
        ('Retrying',f'{counts["up_for_retry"]:,}',''),('Waiting',f'{sum(counts[k] for k in ("queued","scheduled","deferred","up_for_reschedule",None)):,}','')])
    choices=('failed','upstream_failed','up_for_retry','queued','scheduled','deferred','up_for_reschedule')
    current=w.params.get('issue_state','');query=w.params.get('task_q','').strip().casefold()[:250]
    issues=[r for r in w.tasks if r['task_state'] in (*choices,None) and (not current or r['task_state']==current)
        and (not query or query in (r['dag_id']+' '+r['task_id']+' '+r['run_id']).casefold())]
    issues.sort(key=lambda r:(choices.index(r['task_state']) if r['task_state'] in choices else 99,r['dag_id'],r['task_id'],r['map_index']))
    selected=next((r for r in issues if task_key(r)==w.params.get('issue')),issues[0] if issues else None)
    filters=f'<form class="ops-inline-form" action="/app"><input type="hidden" name="view" value="troubleshooting"><input type="hidden" name="q" value="{e(w.params.get("q",""))}"><input type="hidden" name="tag" value="{e(w.params.get("tag",""))}"><input name="task_q" placeholder="Search tasks" aria-label="Search issue tasks" value="{e(w.params.get("task_q",""))}"><select name="issue_state" aria-label="Issue state">'+option('','All issues',current)+''.join(option(s,human(s),current) for s in choices)+'</select><button class="button small">Apply</button></form>'
    rows=[]
    for row in issues:
        active=selected is row
        rows.append(f'<tr class="{"ops-selected" if active else ""}"><td><a class="ops-task-select" href="{w.url(issue=task_key(row))}">{e(row["task_id"])}</a><small class="ops-sub-id">{e(row["dag_id"])}</small></td><td>{airflow_badge(row,metadata=w.meta)}</td><td>{status(row["task_state"])}</td><td>{row["map_index"]}</td><td>{row.get("try_number") or "—"}</td><td class="ops-nowrap">{e(fmt(row.get("task_completed_at_bkk") or row.get("task_started_at_bkk"),False))}</td></tr>')
    listing=panel('Task issues',filters+table(['Task / DAG','Airflow','State','Map','Try','At'],rows,'Task issues'),f'<span class="count">{len(issues):,}</span>')
    if not selected:return cards+listing
    row=selected
    diagnostics=w.raw_store.task_diagnostics(row['dag_id'],row['run_id'],row['task_id']) if w.raw_store else resource(w,'diagnostics')
    diag=next((r for r in diagnostics if task_key(r)==task_key(row)),None)
    down=[t for t in related_tasks(w,row,'down') if t['task_state']=='upstream_failed']
    upstream=[t for t in related_tasks(w,row,'up') if t['task_state']=='failed']
    facts='<dl class="ops-facts">'+''.join(f'<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>' for k,v in [
        ('State',human(row['task_state'])),('Map index',str(row['map_index'])),('Try',str(row.get('try_number') or '—')),
        ('Pool',row.get('task_pool') or '—'),('Queue',row.get('task_queue') or '—'),('Runtime',elapsed(runtime(row,'task')))])+'</dl>'
    body=f'<div class="ops-detail-title"><h2>{e(row["task_id"])}</h2><div class="ops-detail-source">{airflow_badge(row,metadata=w.meta)}</div><p>{e(row["dag_id"])}<br>{e(row["run_id"])}</p><div>{lineage_link(w,row,"View lineage →")}<a class="ops-text-link" href="{safe_link(row.get("dag_run_url"))}" target="_blank" rel="noopener noreferrer">Airflow run ↗</a></div></div>'+facts
    def related(title,items):
        links=''.join(f'<a class="ops-related-link" href="{w.url(issue=task_key(t),issue_state=None,task_q=None)}">{e(t["task_id"])}<small>map {t["map_index"]}</small> →</a>' for t in items)
        return f'<div class="ops-detail-section"><h3>{title} <span>{len(items)}</span></h3>'+ (links or '<p class="ops-muted">None in this run.</p>')+'</div>'
    body+=related('Upstream failed tasks',upstream)+related('Downstream upstream-failed tasks',down)
    body+='<p class="ops-footnote">Same run · current task definitions · relationships do not establish a sole cause.</p>'
    if diag:
        tries=diag.get('tries',[])
        tries_html=table(['Try','State','Started','Ended','Runtime'],[
            f'<tr><td>{t.get("try_number","—")}</td><td>{status(t.get("state"))}</td><td>{e(fmt(t.get("start_date"),False))}</td><td>{e(fmt(t.get("end_date"),False))}</td><td>{elapsed(t.get("duration") if t.get("state") in ("success","failed") else None)}</td></tr>' for t in tries],'Saved attempt history') if tries else empty(diag.get('tries_status',{}).get('message','No attempt history returned for today.'))
        body+='<div class="ops-detail-section"><h3>Attempts</h3>'+tries_html+'</div>'
        if 'blockers_status' in diag:
            blockers=''.join(f'<li><strong>{e(b.get("name"))}</strong><p>{e(b.get("reason"))}</p></li>' for b in diag.get('blockers',[]))
            body+='<div class="ops-detail-section"><h3>Scheduling blockers at collection</h3>'+ ('<ul class="ops-blockers">'+blockers+'</ul>' if blockers else empty(diag['blockers_status'].get('message','No scheduling blockers returned.')))+'</div>'
    else:
        body+='<div class="ops-detail-section"><h3>Attempts</h3>'+empty('Not collected for this task.')+'</div>'
    from lineage_app.views.task_logs import log_frame
    body+=log_frame(w,row['task_id'],row['run_id'],row['map_index'],row['dag_id'])
    detail='<section class="panel ops-task-detail">'+body+'</section>'
    return cards+resource_notice(w,'diagnostics')+f'<div class="ops-trouble-layout">{listing}{detail}</div>'


def health(w):
    snapshots=resource(w,'health')
    sources=[s['name'] for s in w.meta.get('sources',[]) if s.get('name')]
    sources+= [r['airflow_source'] for r in snapshots if r.get('airflow_source')]
    sources=list(dict.fromkeys(sources)) or [None]
    groups=[]
    for source in sources:
        snapshot=next((r for r in snapshots if r.get('airflow_source')==source),{})
        cards=[]
        for key,title,heartbeat in [('metadatabase','Database',None),('scheduler','Scheduler','latest_scheduler_heartbeat'),('triggerer','Triggerer','latest_triggerer_heartbeat'),('dag_processor','DAG processor','latest_dag_processor_heartbeat')]:
            data=snapshot.get(key) or {};state=data.get('status') or 'Not reported'
            tone='healthy' if state=='healthy' else 'unhealthy' if state=='unhealthy' else 'unknown'
            beat=f'<small>Heartbeat {e(fmt(data.get(heartbeat)))}</small>' if heartbeat else '<small>Connection status</small>'
            cards.append(f'<div class="panel ops-health-card"><span>{title}</span><strong class="{tone}"><i></i>{e(human(state))}</strong>{beat}</div>')
        groups.append(f'<section class="ops-health-source" aria-label="{e(source or w.meta.get("source_name") or "Airflow health")}"><div class="ops-health-source-heading">{airflow_badge({"airflow_source":source},metadata=w.meta)}</div><div class="ops-health-grid">'+''.join(cards)+'</div></section>')
    errors=[]
    for error in resource(w,'import_errors'):
        errors.append(f'<tr><td class="ops-id">{e(error.get("filename"))}</td><td>{airflow_badge(error,metadata=w.meta)}</td><td>{e(error.get("bundle_name") or "—")}</td><td class="ops-nowrap">{e(fmt(error.get("timestamp")))}</td><td><details class="ops-traceback"><summary>Traceback</summary><pre>{e(error.get("stack_trace") or "No traceback returned.")}</pre></details></td></tr>')
    errorbody=table(['File','Airflow','Bundle','Reported at','Error'],errors,'Current import errors') if errors else empty('No current import errors.' if coverage(w,'import_errors')['state']=='complete' else 'Import errors have not been collected.')
    facts=[('Snapshot',w.day.isoformat()),('Environment',w.meta['stage'].upper()),('Collected at',fmt(w.meta['collected_at'])),
        ('Core warnings',str(len(w.meta.get('warnings',[])))),('DAGs',f'{len(w.data["inventory"]):,}'),('Stale DAGs',str(sum(bool(d.get('is_stale')) for d in w.data['inventory'])))]
    snapshotfacts='<dl class="ops-facts ops-snapshot-facts">'+''.join(f'<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>' for k,v in facts)+'</dl>'
    return resource_notice(w,'health')+''.join(groups)+panel('Import errors',resource_notice(w,'import_errors')+errorbody,raw_link('import_errors'))+panel('Snapshot',snapshotfacts,raw_link('health'))+'<p class="ops-footnote">Saved status at collection time. A healthy scheduler response does not verify every scheduler or worker.</p>'
