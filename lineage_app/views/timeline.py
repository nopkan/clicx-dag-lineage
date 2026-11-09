"""Legacy-style vertical timetable built from today's monitoring rows."""
from collections import Counter, defaultdict
from datetime import datetime, timedelta

GROUPS=('Persist Data Layer','Curate Data Layer','Consumption Data Layer','Outbound','Risk','GT','Other')
# Column order on screen: Risk comes after Outbound and Other. GROUPS keeps each column's colour index.
ORDER=('Persist Data Layer','Curate Data Layer','Consumption Data Layer','Outbound','GT','Other','Risk')
# Risk Team DAGs have no layer tags; their whole Airflow is the Risk area.
SOURCE_GROUPS={'Risk Team':'Risk'}
ALIASES={'persist':'Persist Data Layer','curate':'Curate Data Layer','consumption':'Consumption Data Layer',
         'outbound':'Outbound','risk':'Risk','gt':'GT'}


def display_labels(dag):
    """Layer labels come only from Airflow tags, never workbook or DAG names."""
    from lineage_app.views.render import tags
    if dag.get('airflow_source') in SOURCE_GROUPS:return {'group':SOURCE_GROUPS[dag['airflow_source']]}
    explicit=set();plain=set()
    def canonical(value):
        return ALIASES.get(value,next((group for group in GROUPS if group.casefold()==value),None))
    for tag in tags(dag):
        value=tag.casefold().strip()
        if ':' in value:
            key,value=(part.strip() for part in value.split(':',1))
            if key=='layer' and value:explicit.add('Other' if value in ('unknown','none','n/a') else canonical(value) or value)
        elif (group:=canonical(value)):plain.add(group)
    layers=explicit or plain
    return {'group':next(iter(layers)) if len(layers)==1 else 'Other'}


def timetable(workspace):
    from lineage_app.views.render import e, elapsed, fmt, human, label, tags
    w=workspace;task_mode=w.params.get('scope','dags')=='tasks'
    rows=w.instances if task_mode else w.runs
    level='task' if task_mode else 'dag'
    started=level+'_started_at_bkk';queued=level+'_queued_at_bkk'
    completed=level+'_completed_at_bkk';state=level+'_state'
    duration=level+'_duration_seconds'
    lower=datetime.combine(w.day,datetime.min.time());upper=lower+timedelta(days=1)
    inventory={d['dag_id']:d for d in w.inventory}
    buckets=defaultdict(list);totals=Counter()
    for row in rows:
        when=row.get(started) or row.get(queued)
        if not when or not lower<=when<upper:continue
        dag=inventory[row['dag_id']]
        labels=display_labels(dag)
        group=(human(row[state]) or 'No state') if task_mode else labels['group']
        slot=when.hour*60+(when.minute//30)*30
        buckets[(slot,group)].append((when,row,labels));totals[group]+=1
    if task_mode:
        order=['Failed','Upstream failed','Running','Up for retry','Queued','Scheduled','Success','Skipped','No state']
    else:order=list(ORDER)
    groups=[g for g in order if totals[g]]+sorted(set(totals)-set(order))
    # Keep the empty day visible, including both midnight boundaries.
    if not groups:groups=['Tasks' if task_mode else 'DAGs']
    columns={g:max(1,min(2,max((len(items) for (slot,group),items in buckets.items() if group==g),default=1))) for g in groups}
    widths={g:columns[g]*244+(columns[g]-1)*8+24 for g in groups}
    tabs=f'<div class="tab-row"><a class="{"active" if not task_mode else ""}" href="{w.url(scope="dags",fullscreen=None)}">DAGs</a><a class="{"active" if task_mode else ""}" href="{w.url(scope="tasks",fullscreen=None)}">Tasks</a></div>'
    title=f'Tasks · {label(w.dag)}' if task_mode and w.dag else 'DAG timetable'
    header='<thead><tr><th class="timetable-corner" scope="col">Time <span>BKK</span></th>'+''.join(
        f'<th scope="col" class="timetable-group group-{GROUPS.index(g) if g in GROUPS else 6}"><strong>{e(g)}</strong><span>{totals[g]} {"tasks" if task_mode else "runs"}</span></th>' for g in groups)+'</tr></thead>'
    body=[]
    for slot in range(0,1441,30):
        time=f'{slot//60:02d}:{slot%60:02d}'
        cells=[]
        for group in groups:
            cards=[]
            for when,row,labels in sorted(buckets.get((slot,group),[]),key=lambda item:(item[0],item[1]['dag_id'],item[1].get('task_id') or '',item[1].get('map_index',-1))):
                dag=inventory[row['dag_id']]
                name=row['task_id'] if task_mode else label(dag)
                url=w.url(view='lineage',dag=row['dag_id'],run=row['run_id'],task=row['task_id'] if task_mode else None,
                          fullscreen=None,expanded=None,native=None)
                finish=row.get(completed)
                end=fmt(finish,False)[:5] if finish else '—'
                if finish and finish.date()>w.day:end+=' +1d'
                frequency=labels.get('job_type') or next((t.split(':',1)[1].capitalize() for t in tags(dag) if t.startswith('frequency:')),human(row.get('run_type')))
                module=labels.get('module') or ''
                if task_mode:module='Map '+str(row['map_index']) if row.get('map_index',-1)>=0 else row['task_id'].rsplit('.',1)[0] if '.' in row['task_id'] else ''
                caption=' · '.join(v for v in (frequency,module) if v)
                state_value=row[state] or 'pending';state_name=human(row[state]) or 'No state'
                queued_label='Queued ' if not row.get(started) else ''
                tooltip=f'{name} · {row["run_id"]} · {state_name} · {queued_label}{fmt(when)}'
                cards.append(f'<a class="timetable-card {e(state_value)}" href="{url}" title="{e(tooltip)}" data-dag="{e(row["dag_id"])}" data-run="{e(row["run_id"])}"'+(f' data-task="{e(row["task_id"])}"' if task_mode else '')+'>'
                    f'<div class="timetable-card-heading"><strong>{e(name)}</strong></div>'
                    f'<div class="timetable-card-time"><span>{queued_label}{when:%H:%M} → {e(end)}</span><span>{e(elapsed(row[duration]))}</span></div>'
                    f'<div class="timetable-card-caption"><span>{e(caption)}</span><span class="timetable-state">{e(state_name)}</span></div></a>')
            cells.append(f'<td class="timetable-cell group-{GROUPS.index(group) if group in GROUPS else 6}"><div class="timetable-cards" style="--lane-columns:{columns[group]}">'+''.join(cards)+'</div></td>')
        body.append(f'<tr data-slot="{slot}"><th scope="row" class="timetable-time"><span>{time}</span></th>'+''.join(cells)+'</tr>')
    colgroup='<colgroup><col style="width:88px">'+''.join(f'<col style="width:{widths[g]}px">' for g in groups)+'</colgroup>'
    count=sum(totals.values())
    notice='' if count else '<div class="empty">No executions today.</div>'
    return (f'<section class="panel timetable-panel"><div class="panel-heading"><div class="timetable-title"><h2>{e(title)}</h2><span class="count">{count} {"tasks" if task_mode else "runs"}</span></div>{tabs}</div>'
        f'{notice}<div class="timetable-scroll" tabindex="0" role="region" aria-label="Today’s Bangkok timetable"><table class="timetable" style="width:{88+sum(widths.values())}px">'
        f'<caption class="screen-reader">{w.day:%d %b %Y}, Bangkok. Rows are half-hour start-time slots; columns are {"task states" if task_mode else "data layers"}. Cards show actual runs, not planned schedules.</caption>'
        +colgroup+header+'<tbody>'+''.join(body)+'</tbody></table></div></section>')
