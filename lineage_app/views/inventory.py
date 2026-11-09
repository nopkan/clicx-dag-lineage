"""Complete operational DAG inventory, with native HTML column controls."""
from collections import Counter,defaultdict
from datetime import datetime
from urllib.parse import quote

from lineage_app.data.model import BKK

# Only DAG identity is pinned (left, always shown); every other column, including the links, can be hidden.
COLUMNS=[
    ('dag','DAG','',320),
    ('airflow','Airflow','Airflow source for this DAG',145),
    ('tags','Tags','All tags from the saved DAG definition',240),
    ('owners','Owner','DAG owners',140),
    ('state','Latest state','Latest observed DAG run today',125),
    ('paused','Paused','Saved Airflow pause setting',90),
    ('definition','Definition','Current or stale task definition',110),
    ('import_errors','Import errors','Saved Airflow has_import_errors flag',110),
    ('tasks','Tasks','Task definitions in the current snapshot; unavailable definitions show —',90),
    ('dependencies','Dependencies','Unique task-to-task edges in the current definition',115),
    ('runs','Runs today','All collected DAG runs on the snapshot Bangkok date',100),
    ('failed_runs','Failed runs','Failed DAG runs today',100),
    ('running_runs','Running runs','Collected DAG runs currently marked running today',115),
    ('queued_runs','Queued runs','Collected DAG runs currently marked queued today',110),
    ('success_rate','Success rate','Successful / finished DAG runs today; finished means success or failed',110),
    ('failed_tasks','Failed tasks','Failed / total collected task instances for the latest run, including mapped instances',120),
    ('upstream_failed','Upstream failed','Collected task instances marked upstream_failed in the latest run',135),
    ('schedule','Schedule','Airflow timetable summary, in the DAG timezone',180),
    ('schedule_detail','Schedule detail','Human-readable Airflow timetable description',260),
    ('timezone','Timezone','Timezone used by the DAG schedule',150),
    ('run_id','Latest run ID','Exact identity of the latest observed run today',310),
    ('run_type','Run type','Type of the latest observed run today',120),
    ('logical_date','Logical date','Latest run logical date, shown in Bangkok time',185),
    ('interval_start','Data interval start','Latest run data interval start, shown in Bangkok time',190),
    ('interval_end','Data interval end','Latest run data interval end, shown in Bangkok time',190),
    ('queued_at','Queued at','Latest run queued timestamp, shown in Bangkok time',185),
    ('started_at','Started at','Latest run start timestamp, shown in Bangkok time',185),
    ('ended_at','Ended at','Latest run completion timestamp, shown in Bangkok time',185),
    ('duration','Duration','Observed duration of the latest run; unfinished durations remain unknown',110),
    ('last_parsed','Last parsed','Airflow definition parse timestamp, shown in Bangkok time',185),
    ('run_after','Scheduler run after','Airflow next_dagrun_run_after as saved; this is not a guarantee of a future execution',205),
    ('active_runs','Active runs','Airflow active_runs_count at collection; not restricted to today’s run list',110),
    ('catchup','Catchup','Saved DAG catchup setting',95),
    ('max_runs','Max active runs','Saved DAG concurrency limit for active runs',135),
    ('max_tasks','Max active tasks','Saved DAG concurrency limit for active tasks',140),
    ('timeout','Run timeout','Saved DAG run timeout, if configured',130),
    ('failure_limit','Failure limit','Saved max_consecutive_failed_dag_runs setting',110),
    ('file','Source file','DAG source path in Airflow',260),
    ('bundle','Bundle','Airflow DAG bundle name',150),
    ('description','Description','Saved DAG description',320),
    ('dag_url','DAG URL','Open this DAG in Airflow',120),
    ('run_url','Run URL','Open the exact latest run in Airflow',120),
    ('lineage','', 'Open the latest run in Lineage',54),
]
LOCKED={'dag'}  # Only the DAG column is pinned and always shown; links scroll and can be hidden.


def inventory_table(workspace):
    from lineage_app.views.render import airflow_badge,e,elapsed,human,label,safe_link,status,tags
    w=workspace
    definitions=Counter(t['dag_id'] for t in w.data['definitions'])
    dependencies=Counter(dag for dag,a,b in {(r['dag_id'],r['upstream_task_id'],r['downstream_task_id']) for r in w.data['dependencies'] if r['upstream_task_id'] and r['downstream_task_id']})
    instances=defaultdict(list)
    for row in w.tasks:instances[(row['dag_id'],row['run_id'])].append(row)
    unavailable={warning.removeprefix('Task definitions unavailable: ') for warning in w.meta.get('warnings',[]) if warning.startswith('Task definitions unavailable: ')}
    def value(v):return e(v) if v is not None and v!='' else '—'
    def boolean(v):return 'Yes' if v is True else 'No' if v is False else '—'
    def time(v):
        if not v:return '—'
        if isinstance(v,str):
            try:v=datetime.fromisoformat(v)
            except ValueError:return e(v)
        if v.tzinfo:v=v.astimezone(BKK)
        return e(v.strftime('%d %b %Y · %H:%M:%S'))
    def external(url,text):return f'<a class="airflow-dag-link" href="{safe_link(url)}" title="{e(url)}" target="_blank" rel="noopener noreferrer">{text} ↗</a>'
    rows=[]
    for d in w.inventory:
        dag=d['dag_id'];runs=w.by_dag.get(dag,[]);latest=runs[0] if runs else None
        last=latest or {};tasks=instances.get((dag,last.get('run_id')),[])
        states=Counter(r['dag_state'] for r in runs)
        finished=states['success']+states['failed']
        has_definitions=not d.get('is_stale') and dag not in unavailable
        base=(d.get('airflow_url') or w.meta['base_url']).rstrip('/')  # Each DAG links to its own Airflow.
        dag_url=base+'/dags/'+quote(dag,safe='')+'/'
        run_url=((last.get('dag_run_url') or dag_url+'runs/'+quote(last.get('run_id',''),safe='')).rstrip('/')+'/') if latest else None
        lineage=w.url(view='lineage',dag=dag,run=last.get('run_id'),task=None,focus=None,
            fullscreen=None,dependency_fullscreen=None,expanded=None,native=None,scope=None)
        failed=sum(t['task_state']=='failed' for t in tasks)
        all_tags=''.join(f'<span class="tag-pill">{e(t)}</span>' for t in tags(d)) or '—'
        failure_count=f'<span class="{"failed-task-count" if failed else ""}">{failed}</span><span class="muted"> / {len(tasks)}</span>' if tasks else '—'
        success=(f'<span title="{states["success"]} successful / {finished} finished runs">{states["success"]/finished:.0%}</span>') if finished else '—'
        source=d.get('relative_fileloc') or d.get('fileloc')
        cells={
            'dag':f'<a class="dag-name" href="{lineage}">{e(label(d))}</a><div class="dag-id">{e(dag)}</div>',
            'airflow':airflow_badge(d,latest,w.meta),
            'tags':f'<div class="inventory-tags">{all_tags}</div>',
            'owners':value(', '.join(d.get('owners') or [])),
            'state':status(last['dag_state']) if latest else status('no_runs'),
            'paused':boolean(d.get('is_paused')),
            'definition':'Stale' if d.get('is_stale') else 'Unavailable' if dag in unavailable else 'Current',
            'import_errors':f'<span class="{"inventory-error" if d.get("has_import_errors") else ""}">{boolean(d.get("has_import_errors"))}</span>',
            'tasks':str(definitions[dag]) if has_definitions else '—',
            'dependencies':str(dependencies[dag]) if has_definitions else '—',
            'runs':str(len(runs)),
            'failed_runs':f'<span class="{"failed-task-count" if states["failed"] else ""}">{states["failed"]}</span>',
            'running_runs':str(states['running']),
            'queued_runs':str(states['queued']),
            'success_rate':success,
            'failed_tasks':failure_count,
            'upstream_failed':str(sum(t['task_state']=='upstream_failed' for t in tasks)) if tasks else '—',
            'schedule':value(d.get('timetable_summary')),
            'schedule_detail':f'<span class="inventory-excerpt" title="{e(d.get("timetable_description"))}">{value(d.get("timetable_description"))}</span>',
            'timezone':value(d.get('timezone')),
            'run_id':f'<span class="inventory-run-id" title="{e(last.get("run_id"))}">{value(last.get("run_id"))}</span>',
            'run_type':value(human(last.get('run_type'))),
            'logical_date':time(last.get('dag_logical_date_bkk')),
            'interval_start':time(last.get('data_interval_start_bkk')),
            'interval_end':time(last.get('data_interval_end_bkk')),
            'queued_at':time(last.get('dag_queued_at_bkk')),
            'started_at':time(last.get('dag_started_at_bkk')),
            'ended_at':time(last.get('dag_completed_at_bkk')),
            'duration':elapsed(last.get('dag_duration_seconds')),
            'last_parsed':time(d.get('last_parsed') or d.get('last_parsed_time')),
            'run_after':time(d.get('next_dagrun_run_after')),
            'active_runs':value(d.get('active_runs_count')),
            'catchup':boolean(d.get('catchup')),
            'max_runs':value(d.get('max_active_runs')),
            'max_tasks':value(d.get('max_active_tasks')),
            'timeout':value(d.get('dag_run_timeout')),
            'failure_limit':value(d.get('max_consecutive_failed_dag_runs')),
            'file':f'<span class="inventory-source" title="{e(d.get("fileloc") or source)}">{value(source)}</span>',
            'bundle':value(d.get('bundle_name')),
            'description':f'<span class="inventory-excerpt" title="{e(d.get("description"))}">{value(d.get("description"))}</span>',
            'dag_url':external(dag_url,'Open DAG'),
            'run_url':external(run_url,'Open run') if run_url else '—',
            'lineage':f'<a class="lineage-row-link" href="{lineage}" aria-label="View lineage for {e(label(d))}" title="View lineage">→</a>',
        }
        rows.append(f'<tr data-dag="{e(dag)}">'+''.join(f'<td class="inventory-col-{key}">{cells[key]}</td>' for key,title,tip,width in COLUMNS)+'</tr>')
    checkboxes=''.join(f'<label><input type="checkbox" id="inventory-col-{key}" checked><span>{e(title or "Lineage link")}</span></label>' for key,title,tip,width in COLUMNS if key not in LOCKED)
    controls='<details class="inventory-columns"><summary class="button small">Columns</summary><form class="inventory-column-menu"><fieldset><legend class="screen-reader">Visible DAG columns</legend>'+checkboxes+'</fieldset><button class="button small" type="reset">Show all</button></form></details>'
    styles=''.join(f'.inventory-panel:has(#inventory-col-{key}:not(:checked)) .inventory-col-{key}{{display:none}}' for key,title,tip,width in COLUMNS if key not in LOCKED)
    headers=''.join(f'<th scope="col" class="inventory-col-{key}" style="min-width:{width}px;width:{width}px" title="{e(tip)}">'+(e(title) if title else '<span class="screen-reader">Lineage</span>')+'</th>' for key,title,tip,width in COLUMNS)
    table='<div class="table-scroll inventory-scroll" tabindex="0" role="region" aria-label="DAG inventory"><table class="dag-table inventory-table"><caption class="screen-reader">DAG inventory from the current snapshot. Run statistics are limited to today in Bangkok. Run dates and parse timestamps are shown in Bangkok; schedules retain their DAG timezone.</caption><thead><tr>'+headers+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>' if rows else '<div class="empty">No matching DAGs.</div>'
    return '<style>'+styles+'</style><section class="panel inventory-panel"><div class="panel-heading"><div class="inventory-title"><h2>DAGs</h2><span class="count">'+str(len(rows))+'</span><span class="inventory-timezone">Times · Bangkok</span></div>'+controls+'</div>'+table+'</section>'
