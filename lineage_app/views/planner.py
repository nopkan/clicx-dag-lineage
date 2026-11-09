"""Schedule (the planner view): a Gantt chart of every upstream and downstream DAG of the selected DAG.

Grey = runs that do not affect the selected DAG, blue = the selected DAG,
red = a DAG that might intercept it (an upstream DAG still running after the
selected DAG starts, or a downstream DAG starting before it finishes).
"""
import math
from collections import deque
from datetime import datetime,timedelta
from urllib.parse import urlencode

from lineage_app.data.planner import plan_model
from lineage_app.views.render import e,label,status,template

LABEL=330      # name column
ROW=30
BAR=14
HEADER=34
GAP=10         # space between the upstream / selected / downstream groups
PLOT=1000      # preferred plot width before the pixels-per-minute clamp
GREY='#b9c5cb';BLUE='#2f6fdb';RED='#d64f47'
INK='#33444f';MUTED='#8a9aa2'


def clock(value,day):
    if value is None:return '—'
    offset=(value.date()-day).days
    return value.strftime('%H:%M')+(f' {offset:+d}d' if offset else '')


def span(seconds):
    if seconds is None:return '—'
    seconds=abs(seconds)
    if seconds<60:return f'{seconds:.0f}s'
    hours,minutes=divmod(round(seconds/60),60)
    return f'{hours}h {minutes:02d}m' if hours else f'{minutes}m'


def levels(focus,graph):
    """Distance from the selected DAG along one direction (1 = direct)."""
    dist={focus:0};queue=deque([focus])
    while queue:
        node=queue.popleft()
        for nxt in sorted(graph.get(node,())):
            if nxt not in dist:dist[nxt]=dist[node]+1;queue.append(nxt)
    dist.pop(focus);return dist


def finish(run):
    """Completed end, or the collection time for a run that is still going."""
    return run['end'] or run['running_until']


def experiment(m):
    """True when the user tried a different start/end for the selected DAG."""
    return bool(m.get('moved'))


def focus_run(m):
    """The selected DAG's run on the chart: the experiment times when set, otherwise today's run."""
    base=m['baseline'][m['focus']]
    if not experiment(m):return base
    test=m['scenario'][m['focus']]
    return {**base,'start':test['start'],'end':test['end'],'duration':test['duration'],'running_until':None}


def intercepts(m,run=None):
    """DAGs whose run overlaps the selected DAG in a way that breaks the dependency order."""
    focus=run or focus_run(m);start,end=focus['start'],finish(focus)
    red=set()
    for dag in m['ancestors']:
        done=finish(m['baseline'][dag])
        if start and done and done>start:red.add(dag)
    for dag in m['descendants']:
        begins=m['baseline'][dag]['start']
        if end and begins and begins<end:red.add(dag)
    return red


def gantt_rows(m):
    """Upstream (farthest first), the selected DAG, downstream (nearest first)."""
    base=m['baseline'];focus=m['focus']
    up=levels(focus,m['graph']['up']);down={d:v for d,v in levels(focus,m['graph']['down']).items() if d not in up}
    key_up=lambda d:(-up[d],finish(base[d]) is None,finish(base[d]) or datetime.min,d)
    key_down=lambda d:(down[d],base[d]['start'] is None,base[d]['start'] or datetime.min,d)
    return sorted(up,key=key_up),[focus],sorted(down,key=key_down)


def planner_svg(model,interactive=True,only_conflicts=False,prefix=''):
    m=model;day=m['day'];focus=m['focus'];base=m['baseline']
    red=intercepts(m)
    groups=gantt_rows(m)
    if only_conflicts:groups=([d for d in groups[0] if d in red],groups[1],[d for d in groups[2] if d in red])
    midnight=datetime.combine(day,datetime.min.time())
    def minutes(t):return (t-midnight).total_seconds()/60
    shown=[d for g in groups for d in g]
    times=[v for d in shown for v in (base[d]['start'],finish(base[d])) if v is not None]
    earliest=min(map(minutes,times),default=0)
    low=math.floor(earliest/30)*30
    if earliest>=0:low=max(0,low)
    high=max(low+60,math.ceil(max(map(minutes,times),default=60)/30)*30)
    ppm=round(min(8,max(.6,PLOT/(high-low))),3)
    def x(t):return LABEL+(minutes(t)-low)*ppm
    step=next((v for v in (15,30,60,120,180,240,360) if v*ppm>=64),360)
    width=math.ceil(LABEL+(high-low)*ppm+24)
    tops=[];top=HEADER+4
    for index,group in enumerate(groups):
        if index:top+=GAP
        for dag in group:tops.append((dag,top));top+=ROW
    height=top+6
    patterns=''.join(f'<pattern id="{prefix}run-{name}" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="6" height="6" fill="{color}" fill-opacity=".35"/><rect width="3" height="6" fill="{color}"/></pattern>'
                     for name,color in (('grey',GREY),('blue',BLUE),('red',RED)))
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" class="planner-svg gantt" data-minute-px="{ppm}" role="img" aria-label="Gantt chart of the selected DAG and all its upstream and downstream DAGs">',
         '<title>DAG schedule</title><desc>Time runs left to right in Bangkok time. Blue is the selected DAG, red a DAG that might intercept it, grey the other related DAGs.</desc>',
         f'<defs>{patterns}</defs>',
         '<rect width="100%" height="100%" fill="white"/><g font-family="Arial,Helvetica,sans-serif">']
    for minute in range(math.ceil(low/step)*step,high+1,step):
        gx=LABEL+(minute-low)*ppm
        svg.append(f'<path d="M{gx:.1f},{HEADER-8}V{height}" stroke="#eef2f4"/><text x="{gx:.1f}" y="{HEADER-14}" font-size="11" fill="{MUTED}" text-anchor="middle">{clock(midnight+timedelta(minutes=minute),day)}</text>')
    svg.append(f'<path d="M0,{HEADER}H{width}" stroke="#dfe6e9"/>')
    # Faint guides at the selected DAG's start and end make an interception easy to see.
    run=base[focus]
    for when in (run['start'],finish(run)):
        if when:svg.append(f'<path class="gantt-guide" d="M{x(when):.1f},{HEADER}V{height}" stroke="{BLUE}" stroke-opacity=".35" stroke-dasharray="3 3"/>')
    for dag,y0 in tops:
        row=base[dag];selected=dag==focus
        tone='blue' if selected else 'red' if dag in red else 'grey'
        color={'blue':BLUE,'red':RED,'grey':GREY}[tone]
        mid=y0+ROW/2;by=mid-BAR/2
        anchor=f' id="{prefix}planner-focus"' if selected else ''
        svg.append(f'<g{anchor} data-planner-dag="{e(dag)}" data-row="{"selected" if selected else tone}">')
        if selected:svg.append(f'<rect x="0" y="{y0}" width="{width}" height="{ROW}" fill="{BLUE}" fill-opacity=".06"/>')
        name=label(m['inventory'][dag]) if dag in m['inventory'] else dag
        short=name if len(name)<=46 else name[:45]+'…'
        text_color={'blue':BLUE,'red':RED,'grey':INK}[tone]
        link=interactive and not selected and dag in m['inventory']
        if link:svg.append(f'<a href="/app?{e(urlencode(dict(view="planner",plan_dag=dag)))}">')
        svg.append(f'<text x="14" y="{mid+4}" fill="{text_color}" font-size="12" font-weight="{700 if selected else 400}">{e(short)}<title>{e(dag)}</title></text>')
        if link:svg.append('</a>')
        start,done=row['start'],finish(row)
        if start and done:
            sx=x(start);w=max(2,x(done)-sx)
            fill=f'url(#{prefix}run-{tone})' if row['end'] is None else color
            when=f'{clock(start,day)} → {clock(done,day)}'+(' (still running)' if row['end'] is None else f' · {span(row["duration"])}')
            svg.append(f'<rect class="planner-run" x="{sx:.1f}" y="{by:.1f}" width="{w:.1f}" height="{BAR}" rx="3" fill="{fill}"><title>{e(dag)} · {e(when)}</title></rect>')
        elif start:
            svg.append(f'<rect class="planner-run" x="{x(start):.1f}" y="{by:.1f}" width="3" height="{BAR}" fill="{color}"><title>{e(dag)} · starts {clock(start,day)} · end unknown</title></rect>')
        svg.append(f'<path d="M0,{y0+ROW}H{width}" stroke="#f3f6f7"/></g>')
    svg.append('</g></svg>')
    return ''.join(svg),width,height


def time_domain(times):
    """Earliest start to latest finish of every shown run, padded by 2% so bars never touch the edges."""
    if not times:return None
    low,high=min(times),max(times)
    if high<=low:high=low+timedelta(minutes=10)
    pad=(high-low)*0.02
    return low-pad,high+pad


def ticks(low,high,day):
    """Round clock times inside the domain, about 6-12 of them, whatever the range."""
    total=(high-low).total_seconds()/60
    step=next((v for v in (1,2,5,10,15,20,30,60,120,180,240,360,720) if total/v<=12),1440)
    midnight=datetime.combine(day,datetime.min.time())
    first=math.ceil((low-midnight).total_seconds()/60/step)*step
    out=[];minute=first
    while midnight+timedelta(minutes=minute)<=high:
        out.append(midnight+timedelta(minutes=minute));minute+=step
    return out


def _value(d,key):
    """Inventory values are often stored as text ('None', "['a']"); show them plainly."""
    value=d.get(key)
    if value in (None,'','None','null','{}','[]'):return None
    text=str(value)
    if text.startswith('[') and text.endswith(']'):
        import ast
        try:
            items=ast.literal_eval(text)
            return ', '.join(str(i.get('name',i)) if isinstance(i,dict) else str(i) for i in items) or None
        except (ValueError,SyntaxError):pass
    return {'True':'Yes','False':'No'}.get(text,text)


def _facts(pairs):
    rows=''.join(f'<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>' for k,v in pairs if v not in (None,''))
    return f'<dl class="dependency-facts">{rows}</dl>' if rows else ''


def gantt_details(w,m,red,keys):
    """One details box per row (run + config), shown by CSS when its row is selected."""
    from lineage_app.views.render import elapsed,fmt,status
    day=m['day'];focus=m['focus'];base=m['baseline'];f=focus_run(m)
    up=levels(focus,m['graph']['up']);down=levels(focus,m['graph']['down'])
    out=[]
    for dag,key in keys.items():
        d=m['inventory'].get(dag,{'dag_id':dag});runs=(getattr(w,'by_dag',{}) or {}).get(dag) or []
        row=runs[0] if runs else {}
        if dag==focus:relation='Selected DAG'+(f' · experiment {clock(f["start"],day)} → {clock(f["end"],day)}' if experiment(m) and f['start'] and f['end'] else '')
        elif dag in up:relation=f'Upstream · {"direct" if up[dag]==1 else str(up[dag])+" levels up"}'
        else:relation=f'Downstream · {"direct" if down.get(dag)==1 else str(down.get(dag))+" levels down"}'
        why=''
        if dag in red:
            if dag in up:why=f'Might intercept: still running {span((finish(base[dag])-f["start"]).total_seconds())} after the selected DAG starts.'
            else:why=f'Might intercept: starts {span((finish(f)-base[dag]["start"]).total_seconds())} before the selected DAG finishes.'
        heading=f'<div class="dependency-detail-heading"><h2>{e(label(d))}</h2>{status(row.get("dag_state")) if row else ""}</div>'
        intro=f'<p class="gantt-relation {("red" if dag in red else "blue" if dag==focus else "")}">{e(relation)}</p>'+(f'<p class="gantt-why">{e(why)}</p>' if why else '')
        run=_facts([('Started',fmt(row.get('dag_started_at_bkk'),False) if row.get('dag_started_at_bkk') else None),
                    ('Ended',fmt(row.get('dag_completed_at_bkk'),False) if row.get('dag_completed_at_bkk') else ('Still running' if row.get('dag_started_at_bkk') else None)),
                    ('Duration',span(row['dag_duration_seconds'] if row.get('dag_duration_seconds') is not None else base[dag]['duration']) if (row.get('dag_duration_seconds') is not None or base[dag]['duration'] is not None) else None),
                    ('Queued',fmt(row.get('dag_queued_at_bkk'),False) if row.get('dag_queued_at_bkk') else None),
                    ('Run type',row.get('run_type')),('Triggered by',row.get('dag_run_triggered_by')),
                    ('Run ID',row.get('run_id'))]) if row else '<p class="muted">No run in this snapshot.</p>'
        links=[]
        if row.get('dag_run_url'):links.append(f'<a class="button small" href="{e(row["dag_run_url"])}" target="_blank" rel="noopener noreferrer">Open in Airflow ↗</a>')
        if row and hasattr(w,'url'):
            links.append(f'<a class="button small" href="{e(w.url(view="lineage",dag=dag,run=row["run_id"],task=None,focus=None,fullscreen=None,dependency_fullscreen=None,expanded=None,native=None,scope=None))}">View tasks →</a>')
        if dag!=focus and dag in m['inventory']:links.append(f'<a class="button small" href="/app?{e(urlencode(dict(view="planner",plan_dag=dag)))}">Plan this DAG</a>')
        actions=f'<div class="gantt-actions">{"".join(links)}</div>' if links else ''
        today=''
        if len(runs)>1:
            today=f'<section><h3>Today’s runs <span>{len(runs)}</span></h3>'+''.join(
                f'<div class="dependency-run"><span class="mono">{e(fmt(r.get("dag_started_at_bkk") or r.get("dag_queued_at_bkk"),False))}</span>{status(r.get("dag_state"))}<span class="mono">{e(elapsed(r.get("dag_duration_seconds")))}</span></div>' for r in runs)+'</section>'
        config=_facts([('Schedule',' · '.join(v for v in (_value(d,'timetable_summary'),_value(d,'timetable_description')) if v) or row.get('dag_timetable_summary')),
                       ('Timezone',_value(d,'timezone')),('Paused',_value(d,'is_paused')),('Owners',_value(d,'owners')),
                       ('Max active runs',_value(d,'max_active_runs')),('Max active tasks',_value(d,'max_active_tasks')),
                       ('Catchup',_value(d,'catchup')),('Start date',_value(d,'start_date')),('Run timeout',_value(d,'dag_run_timeout')),
                       ('File',_value(d,'relative_fileloc')),('Airflow',_value(d,'airflow_source'))])
        extra=''
        if _value(d,'description'):extra+=f'<p class="gantt-description">{e(_value(d,"description"))}</p>'
        if _value(d,'tags'):extra+='<div class="gantt-tags">'+''.join(f'<span class="tag-pill">{e(t.strip())}</span>' for t in _value(d,'tags').split(','))+'</div>'
        out.append(f'<div class="dependency-detail {key}-detail" data-dag-detail="{e(dag)}">{heading}{intro}'
                   f'<section><h3>Run</h3>{run}</section>{actions}{today}'
                   f'<section><h3>Config</h3>{config or "<p class=muted>No configuration collected.</p>"}{extra}</section></div>')
    return ''.join(out)


def gantt_html(m,interactive=True,w=None):
    """Responsive Gantt: a fixed name column and a timeline that fills the remaining width.

    Each row is its own small SVG with x positions in percent, so the time range
    always stretches across the whole chart regardless of screen width. With a
    workspace, clicking a row opens its details box (no JavaScript: radio + CSS).
    """
    day=m['day'];focus=m['focus'];base=m['baseline']
    frun=focus_run(m);red=intercepts(m,frun);groups=gantt_rows(m)
    shown=[d for g in groups for d in g]
    domain=time_domain([v for d in shown for v in (base[d]['start'],finish(base[d])) if v is not None]+[v for v in (frun['start'],finish(frun)) if v is not None])
    if not domain:return '<div class="gantt-empty">No runs in this snapshot for these DAGs.</div>'
    low,high=domain;total=(high-low).total_seconds()
    def pct(t):return max(0.0,min(100.0,(t-low).total_seconds()/total*100))
    marks=ticks(low,high,day)
    grid=''.join(f'<line x1="{pct(t):.3f}%" x2="{pct(t):.3f}%" y1="0" y2="100%" stroke="#eef2f4"/>' for t in marks)
    run=frun
    guides=''.join(f'<line class="gantt-guide" x1="{pct(t):.3f}%" x2="{pct(t):.3f}%" y1="0" y2="100%" stroke="{BLUE}" stroke-opacity=".35" stroke-dasharray="3 3"/>'
                   for t in (run['start'],finish(run)) if t)
    labels=[]
    for t in marks:
        p=pct(t);anchor='start' if p<3 else 'end' if p>97 else 'middle'
        labels.append(f'<text x="{p:.3f}%" y="18" font-size="11" fill="{MUTED}" text-anchor="{anchor}">{clock(t,day)}</text>')
    header=(f'<div class="gantt-row gantt-axis"><div class="gantt-name"></div>'
            f'<svg class="gantt-track" width="100%" height="28" role="presentation">{"".join(labels)}</svg></div>')
    keys={dag:f'gsel-{i}' for i,dag in enumerate(shown)}
    body=[]
    for index,group in enumerate(groups):
        if index:body.append('<div class="gantt-gap"></div>')
        for dag in group:
            row=frun if dag==focus else base[dag];selected=dag==focus
            tone='blue' if selected else 'red' if dag in red else 'grey'
            color={'blue':BLUE,'red':RED,'grey':GREY}[tone]
            name=label(m['inventory'][dag]) if dag in m['inventory'] else dag
            state=base[dag].get('state')
            if state is None and not base[dag].get('start'):badge='<span class="state no-run">No run</span>'
            else:badge=status(state)
            title=f'<span class="gantt-label" title="{e(dag)}">{e(name)}</span>{badge}'
            start,done=row['start'],finish(row);bar=''
            if start and done:
                x=pct(start);bar_width=max(0.25,pct(done)-x)
                when=f'{clock(start,day)} → {clock(done,day)}'+(' (still running)' if row['end'] is None else f' · {span(row["duration"])}')
                fill=f'url(#run-{tone})' if row['end'] is None else color
                bar=f'<rect class="planner-run" x="{x:.3f}%" y="8" width="{bar_width:.3f}%" height="14" rx="3" fill="{fill}"><title>{e(dag)} · {e(when)}</title></rect>'
            elif start:
                bar=f'<rect class="planner-run" x="{pct(start):.3f}%" y="8" width="3" height="14" fill="{color}"><title>{e(dag)} · starts {clock(start,day)} · end unknown</title></rect>'
            if selected and experiment(m):
                real=base[dag];ghost=''
                if real['start'] and finish(real):
                    gx=pct(real['start']);gw=max(0.25,pct(finish(real))-gx)
                    ghost=(f'<rect class="planner-actual" x="{gx:.3f}%" y="8" width="{gw:.3f}%" height="14" rx="3" fill="none" stroke="{BLUE}" stroke-opacity=".45" stroke-dasharray="3 2">'
                           f'<title>{e(dag)} · actual run {clock(real["start"],day)} → {clock(finish(real),day)}</title></rect>')
                bar=ghost+bar.replace(f'<title>{e(dag)} · ',f'<title>{e(dag)} · experiment ',1)
            tag,pick='div',''
            if w is not None:
                tag='label';pick=f'<input type="radio" class="gantt-select" name="gantt_sel" id="{keys[dag]}" aria-label="Show details for {e(dag)}">'
            attrs=f' for="{keys[dag]}"' if w is not None else ''
            body.append(f'<{tag}{attrs} class="gantt-row {tone}" data-planner-dag="{e(dag)}" data-row="{"selected" if selected else tone}"{" id=\"planner-focus\"" if selected else ""}>{pick}'
                        f'<div class="gantt-name">{title}</div>'
                        f'<svg class="gantt-track" width="100%" height="30" role="presentation">{grid}{guides}{bar}</svg></{tag}>')
    patterns=''.join(f'<pattern id="run-{name}" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="6" height="6" fill="{color}" fill-opacity=".35"/><rect width="3" height="6" fill="{color}"/></pattern>'
                     for name,color in (('grey',GREY),('blue',BLUE),('red',RED)))
    defs=f'<svg width="0" height="0" class="gantt-defs" aria-hidden="true"><defs>{patterns}</defs></svg>'
    chart=(f'<div class="gantt" role="{"group" if w is not None else "img"}" aria-label="Gantt chart of the selected DAG and all its upstream and downstream DAGs, '
           f'{clock(low,day)} to {clock(high,day)} Bangkok time">{defs}{header}{"".join(body)}</div>')
    if w is None:return chart
    # Same box as the Dependency page: × closes, ⇆ moves it to the other side, – hides it.
    style='<style>'+''.join(f'.gantt-panel:has(#{k}:checked) .{k}-detail{{display:block}}' for k in keys.values())+'</style>'
    aside=('<aside class="dependency-sidebar gantt-sidebar" aria-label="DAG run and config">'
           '<label class="dependency-detail-close" title="Close details"><input type="radio" class="gantt-none" name="gantt_sel" aria-label="Close details"><span aria-hidden="true">×</span></label>'
           '<label class="dependency-detail-close dependency-panel-swap" title="Move details to the other side"><input type="checkbox" id="gantt-panel-swap" aria-label="Move details to the other side" autocomplete="off"><span aria-hidden="true">⇆</span></label>'
           '<label class="dependency-detail-close dependency-panel-hide" title="Hide or show details"><input type="checkbox" id="gantt-panel-hide" aria-label="Hide details" autocomplete="off"><span class="hide-icon" aria-hidden="true">–</span><span class="show-icon" aria-hidden="true">Details</span></label>'
           +gantt_details(w,m,red,keys)+'</aside>')
    return style+f'<div class="gantt-shell">{chart}{aside}</div>'


def export_plan(w):
    m=plan_model(w)
    return dict(stage=m['stage'],snapshot_date=str(m['day']),collected_at=m['collected_at'],
        source='airflow_upstream_tags',focus=m['focus'],buffer_percent=m['buffer'],other_starts='fixed',
        buffer_scope='all measured durations; custom end overrides selected DAG',
        status=m['status'],conflicts=m['conflicts'],resolved=m['resolved'],unassessed_links=m['unassessed'],
        timing_gaps=m['gaps'],incomplete_tag_dags=m['tag_gaps'],failed_baselines=m['failures'],cycle=m['cyclic'],
        dags={dag:dict(today=m['baseline'][dag],proposed=m['scenario'][dag]) for dag in sorted(m['scope'])})


def planner(w):
    suggestions=''.join(f'<option value="{e(d["dag_id"])}">{e(label(d))}</option>' for d in sorted(w.data['inventory'],key=lambda d:d['dag_id']))
    # Only the experiment's start/end are used; other old what-if fields are ignored.
    for key in [k for k in w.params if k.startswith('plan_') and k not in ('plan_dag','plan_start','plan_end')]:w.params.pop(key,None)
    for key in ('plan_start','plan_end'):
        if not w.params.get(key):w.params.pop(key,None)
    try:m=plan_model(w)
    except ValueError as exc:
        return f'<section class="panel planner-empty"><div class="error-box" role="alert">{e(exc)}</div><form action="/app" method="get"><input type="hidden" name="view" value="planner"><input name="plan_dag" list="planner-dags" value="{e(w.params.get("plan_dag",w.dag_id))}" aria-label="Choose DAG"><datalist id="planner-dags">{suggestions}</datalist><button class="button primary">View</button></form><a class="button" href="/app?view=planner&amp;plan_dag={e(w.params.get("plan_dag",""))}">Clear experiment</a></section>'
    run=focus_run(m);day=m['day']
    reset='/app?'+urlencode(dict(view='planner',plan_dag=m['focus']))
    chip=''
    if experiment(m):
        chip=(f'<a class="gantt-exp-chip" href="{e(reset)}" title="Clear the experiment and show the real run">'
              f'Experiment {clock(run["start"],day)} → {clock(run["end"],day) if run["end"] else "?"}<span aria-hidden="true">×</span></a>')
    return template('templates/planner.html',dag=e(m['focus']),suggestions=suggestions,graph=gantt_html(m,w=w),
        exp_start=e(run['start'].strftime('%H:%M') if run['start'] else ''),
        exp_end=e((w.params.get('plan_end') or '')[:5]),
        exp_open='open' if w.params.get('plan_edit')=='1' else '',reset=e(reset),chip=chip)
