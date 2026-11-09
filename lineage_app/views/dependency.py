"""DAG dependency timetable: Python positions, SVG arrows, native HTML selection."""
import json
import hashlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from math import ceil

from lineage_app.data.model import trace
from lineage_app.data.dag_tags import LINK_LABELS, dependency_graph
from lineage_app.views.timeline import GROUPS, ORDER, display_labels

CARD_WIDTH=170
# Line style per evidence: tag solid; asset/S3 marker dashed; trigger/sensor dotted.
DASH={'tag':'','asset':'7 4','s3_marker':'7 4','dag':'7 4','trigger':'2 4','sensor':'2 4','remote_trigger':'2 4'}

def edge_kind(kinds):
    """One line style per edge: Airflow's own evidence before tags."""
    return next((k for k in ('trigger','asset','sensor','s3_marker','remote_trigger','dag') if k in kinds),'tag')
CARD_HEIGHT=42
GAP=3
HEADER_HEIGHT=46
TIME_WIDTH=52
MAX_LANES=2  # DAG cards side by side per layer column; busy half-hours grow taller instead.

def timetable_geometry(groups,buckets):
    """Balance lane columns for a desktop overview, retaining every time slot."""
    peaks={g:max((len(v) for (slot,group),v in buckets.items() if group==g),default=1) for g in groups}
    candidates=[]
    for cap in range(1,MAX_LANES+1):
        columns={g:max(1,min(cap,peaks[g])) for g in groups}
        widths={g:columns[g]*CARD_WIDTH+(columns[g]-1)*GAP+13 for g in groups}
        heights={}
        for slot in range(0,1441,30):
            count=max((ceil(len(buckets.get((slot,g),[]))/columns[g]) for g in groups),default=0)
            heights[slot]=9+count*CARD_HEIGHT+max(0,count-1)*GAP if count else (16 if slot%60==0 else 10)
        width=TIME_WIDTH+sum(widths.values());height=HEADER_HEIGHT+sum(heights.values())
        # The responsive Fit control uses the actual viewport; this selects a
        # balanced, stable grid independently of selection and fullscreen state.
        score=min(1400/width,700/height)
        candidates.append((score,-width,columns,widths,heights))
    _,_,columns,widths,heights=max(candidates,key=lambda c:c[:2])
    return columns,widths,heights

def dependency_anchor(dag):
    return 'dependency-card-'+hashlib.sha256(dag.encode()).hexdigest()[:20]

def find_dependency(workspace,query):
    """Search identities without changing the DAGs or relationships on the map."""
    from lineage_app.views.render import label
    query=query.strip().casefold()
    if not query:return []
    inventory=workspace.data['inventory']
    exact=[d for d in inventory if d['dag_id'].casefold()==query]
    exact=exact or [d for d in inventory if label(d).casefold()==query]
    matches=exact or [d for d in inventory if query in d['dag_id'].casefold() or query in label(d).casefold()]
    return sorted(matches,key=lambda d:d['dag_id'])

def dependency_search_form(w,state_filter,tag_options):
    from lineage_app.views.render import e,label,template
    suggestions=''.join(f'<option value="{e(d["dag_id"])}">{e(label(d))}</option>' for d in sorted(w.data['inventory'],key=lambda d:d['dag_id']))
    return template('templates/dependency_search.html',query=e(w.params.get('dag_search',w.params.get('q',w.params.get('focus','')))),
        suggestions=suggestions,state_filter=state_filter,tags=tag_options,source_filters=w.source_filter_fields())


def dependency_curve(model,a,b):
    ax,ay=model['positions'][a];bx,by=model['positions'][b]
    if abs(ay-by)<CARD_HEIGHT:
        if ax<=bx:sx,sy,tx,ty=ax+CARD_WIDTH,ay+CARD_HEIGHT/2,bx,by+CARD_HEIGHT/2
        else:sx,sy,tx,ty=ax,ay+CARD_HEIGHT/2,bx+CARD_WIDTH,by+CARD_HEIGHT/2
        mid=(sx+tx)/2
        return f'M{sx},{sy} C{mid},{sy} {mid},{ty} {tx},{ty}'
    below=by>ay
    sx,sy=ax+CARD_WIDTH/2,ay+(CARD_HEIGHT if below else 0)
    tx,ty=bx+CARD_WIDTH/2,by+(0 if below else CARD_HEIGHT)
    bend=max(32,min(150,abs(ty-sy)*.45))*(1 if below else -1)
    return f'M{sx},{sy} C{sx},{sy+bend} {tx},{ty-bend} {tx},{ty}'


def dependency_model(workspace):
    """One node per DAG; declared edges never establish a run-to-run pairing."""
    w=workspace
    lower=datetime.combine(w.day,datetime.min.time());upper=lower+timedelta(days=1)
    runs=defaultdict(list)
    for row in w.runs:
        when=row.get('dag_started_at_bkk') or row.get('dag_queued_at_bkk')
        if when and lower<=when<upper:runs[row['dag_id']].append(row)
    inventory={d['dag_id']:d for d in w.inventory}
    nodes={};buckets=defaultdict(list);totals=Counter()
    for dag_id,observations in runs.items():
        observations.sort(key=lambda r:(r.get('dag_started_at_bkk') or r.get('dag_queued_at_bkk'),r['run_id']),reverse=True)
        row=observations[0];when=row.get('dag_started_at_bkk') or row.get('dag_queued_at_bkk')
        labels=display_labels(inventory[dag_id])
        group=labels['group'];slot=when.hour*60+when.minute//30*30
        nodes[dag_id]={'row':row,'when':when,'labels':labels,'group':group,'slot':slot,'runs':observations}
        buckets[(slot,group)].append(dag_id);totals[group]+=1
    # Tag-named layers outside the standard set sit before Other; Risk is last.
    groups=[g for g in ORDER[:-2] if totals[g]]+sorted(set(totals)-set(GROUPS))+[g for g in ORDER[-2:] if totals[g]]
    if not groups:groups=['DAGs']
    columns,widths,row_heights=timetable_geometry(groups,buckets)
    x=TIME_WIDTH;left={}
    for group in groups:left[group]=x;x+=widths[group]
    positions={};y=HEADER_HEIGHT
    for slot in range(0,1441,30):
        height=row_heights[slot]
        for group in groups:
            items=buckets.get((slot,group),[])
            items.sort(key=lambda dag:(nodes[dag]['when'],dag))
            for i,dag in enumerate(items):
                positions[dag]=(left[group]+6+(i%columns[group])*(CARD_WIDTH+GAP),y+4+(i//columns[group])*(CARD_HEIGHT+GAP))
        y+=height
    relationships=dependency_graph(w.data['inventory'],w.data.get('dag_links',[]))
    return dict(nodes=nodes,buckets=buckets,groups=groups,columns=columns,widths=widths,
        row_heights=row_heights,positions=positions,width=x,height=y,totals=totals,**relationships)


def dependency_timetable(workspace):
    from lineage_app.views.render import e, elapsed, fmt, human, label, status, tags
    w=workspace
    model=dependency_model(w)
    nodes=model['nodes'];groups=model['groups'];up=model['up'];down=model['down']
    keys={dag:f'dep-dag-{i}' for i,dag in enumerate(sorted(nodes))}
    inventory={d['dag_id']:d for d in w.inventory}
    all_inventory={d['dag_id']:d for d in w.data['inventory']}
    focus=w.params.get('focus','')
    selected=focus if focus in nodes else ''
    search_html=''
    search=getattr(w,'dependency_search',None)
    if search is not None:
        matches=search['matches']
        links=[]
        for d in matches:
            dag=d['dag_id']
            if dag in nodes:
                links.append(f'<a href="{w.url(focus=dag,dag_search=None,q=None)}#{dependency_anchor(dag)}">{e(dag)} →</a>')
            else:links.append(f'<span>{e(dag)} <small>No run today</small></span>')
        message='Choose a DAG' if len(matches)>1 else 'No run today' if matches else 'No matching DAG'
        search_html=f'<div class="dependency-search-results" role="status"><strong>{message}</strong>'+''.join(links)+'</div>'
    styles=[];paths=[];edge_keys={}
    for i,(a,b) in enumerate(model['edges']):
        if a not in nodes or b not in nodes:continue
        curve=dependency_curve(model,a,b)
        key=f'dep-edge-{i}';edge_keys[(a,b)]=key
        kind=edge_kind(model['kinds'].get((a,b),['tag']))
        paths.append(f'<path class="dependency-edge {key} dependency-edge-{kind}" data-upstream="{e(a)}" data-downstream="{e(b)}" data-link="{e(",".join(model["kinds"].get((a,b),["tag"])))}" d="{curve}"/>')
    for dag,key in keys.items():
        base=f'.dependency-panel:has(#{key}:checked)'
        styles.append(f'{base} .dependency-placeholder{{display:none}}{base} .{key}-detail{{display:block}}')
        for depth,full in (('all',True),('direct',False)):
            scope=base+f':has(#dependency-depth-{depth}:checked)'
            ancestors=trace(dag,up,full);descendants=trace(dag,down,full)
            relevant=ancestors|descendants|{dag}
            selectors=[f'{scope} .{keys[n]}-card' for n in sorted(relevant & nodes.keys())]
            styles.append(','.join(selectors)+'{opacity:1}')
            for related,color in ((ancestors,'#c78779'),(descendants,'#66b6a4')):
                selectors=[f'{scope} .{keys[n]}-card' for n in sorted(related & nodes.keys())]
                if selectors:styles.append(','.join(selectors)+f'{{border-color:{color}}}')
            upstream_edges={edge for edge in edge_keys if edge[0] in ancestors|{dag} and edge[1] in ancestors|{dag}}
            downstream_edges={edge for edge in edge_keys if edge[0] in descendants|{dag} and edge[1] in descendants|{dag}}
            for edges,color,marker in ((upstream_edges,'#bd8069','dep-arrow-up'),(downstream_edges,'#389c88','dep-arrow-down')):
                selectors=[f'{scope} .{edge_keys[edge]}' for edge in sorted(edges)]
                if selectors:styles.append(','.join(selectors)+f'{{display:block;stroke:{color};marker-end:url(#{marker})}}')
        styles.append(f'{base} .{key}-card{{opacity:1;border:2px solid #00bfb5;background:#edfcf9}}')
    # No JavaScript: the details box opens on the side away from the selected card, and ⇆ flips it.
    far=[f'#{keys[dag]}:checked' for dag in sorted(nodes) if model['positions'][dag][0]+CARD_WIDTH/2>model['width']/2]
    left='{inset:12px auto 12px 12px}';right='{inset:12px 12px 12px auto}'
    styles.append(f'.dependency-panel:has(#dependency-panel-swap:checked) .dependency-sidebar{left}')
    if far:
        styles.append(f'.dependency-panel:has({",".join(far)}) .dependency-sidebar{left}')
        styles.append(f'.dependency-panel:has(#dependency-panel-swap:checked):has({",".join(far)}) .dependency-sidebar{right}')
    def lineage_url(row):
        return w.url(view='lineage',dag=row['dag_id'],run=row['run_id'],task=None,focus=None,
            fullscreen=None,dependency_fullscreen=None,expanded=None,native=None,scope=None)
    def related_list(ids,anchor,upstream):
        items=[]
        home=(all_inventory.get(anchor) or {}).get('airflow_source')
        for dag in sorted(ids):
            kinds=model['kinds'].get((dag,anchor) if upstream else (anchor,dag),[])
            where=(all_inventory.get(dag) or {}).get('airflow_source')
            notes=' · '.join([*(LINK_LABELS[k] for k in kinds),*([where] if where and home and where!=home else [])])
            note=f'<small class="dependency-link-kind">{e(notes)}</small>' if notes else ''
            if dag in keys:
                item=f'<label for="{keys[dag]}" class="dependency-related">{e(dag)}</label>{note}'
            else:
                availability='No run today' if dag in inventory else 'Filtered' if dag in all_inventory else 'Not in snapshot'
                item=f'<div class="dependency-unavailable"><span>{e(dag)}</span><small>{availability}{" · "+e(notes) if notes else ""}</small></div>'
            items.append('<li>'+item+'</li>')
        return '<ul class="dependency-related-list">'+''.join(items)+'</ul>' if items else '<span class="muted">No recorded links</span>'
    details=[]
    for dag,key in keys.items():
        node=nodes[dag];row=node['row'];d=inventory[dag]
        sections=[]
        for title,adj in (('Upstream',up),('Downstream',down)):
            for depth,full in (('all',True),('direct',False)):
                related=trace(dag,adj,full)
                sections.append(f'<section class="dependency-{depth}"><h3>{title} <span>{len(related)}</span></h3>{related_list(related,dag,title=="Upstream")}</section>')
        observations=''.join(f'<a class="dependency-run" href="{lineage_url(r)}"><span class="mono">{e(fmt(r["dag_started_at_bkk"] or r["dag_queued_at_bkk"],False))}</span>{status(r["dag_state"])}<span>→</span></a>' for r in node['runs'])
        coverage=model['coverage'].get(dag,'missing')
        message=('Some upstream links are unknown' if coverage=='partial' else 'Upstream unknown' if coverage=='unknown'
                 else 'No upstream tags or Airflow links recorded' if coverage=='missing' and dag not in model['linked'] else '')
        availability=f'<p class="dependency-provenance">{message}</p>' if message else ''
        details.append(f'<div class="dependency-detail {key}-detail" data-dag-detail="{e(dag)}"><div class="dependency-detail-heading"><h2>{e(label(d))}</h2>{status(row["dag_state"])}</div>'
            f'{availability}'
            f'<dl class="dependency-facts"><div><dt>Started</dt><dd>{e(fmt(row["dag_started_at_bkk"],False))}</dd></div><div><dt>Ended</dt><dd>{e(fmt(row["dag_completed_at_bkk"],False))}</dd></div><div><dt>Duration</dt><dd>{e(elapsed(row["dag_duration_seconds"]))}</dd></div><div><dt>Layer</dt><dd>{e(node["group"])}</dd></div></dl>'
            f'<a class="button small" href="{lineage_url(row)}">View tasks →</a>'+''.join(sections)
            +f'<section><h3>Today’s runs <span>{len(node["runs"])}</span></h3>{observations}</section></div>')
    body=[]
    for slot in range(0,1441,30):
        cells=[]
        for group in groups:
            cards=[]
            for dag in model['buckets'].get((slot,group),[]):
                node=nodes[dag];row=node['row'];when=node['when'];key=keys[dag]
                finish=row['dag_completed_at_bkk'];end=fmt(finish,False)[:5] if finish else '—'
                if finish and finish.date()>w.day:end+=' +1d'
                frequency=node['labels'].get('job_type') or next((t.split(':',1)[1].capitalize() for t in tags(inventory[dag]) if t.startswith('frequency:')),human(row.get('run_type')))
                caption=' · '.join(v for v in (frequency,node['labels'].get('module','')) if v)
                checked=' checked' if selected==dag else ''
                cards.append(f'<div id="{dependency_anchor(dag)}" class="timetable-card dependency-card {key}-card {e(row["dag_state"] or "pending")}" data-dag="{e(dag)}" data-run="{e(row["run_id"])}" style="height:{CARD_HEIGHT}px" title="{e(dag)} · {e(human(row["dag_state"]))} · {when:%H:%M} → {e(end)} · Latest run today: {e(row["run_id"])}">'
                    f'<input type="radio" class="dependency-select" name="focus" form="dependency-state" id="{key}" value="{e(dag)}" aria-label="Show dependencies for {e(dag)}"{checked}>'
                    f'<input type="radio" class="dependency-deselect" name="focus" form="dependency-state" value="" aria-label="Deselect {e(dag)}">'
                    f'<div class="timetable-card-heading"><strong>{e(label(inventory[dag]))}</strong></div>'
                    f'<div class="timetable-card-time"><span>{"Queued " if not row["dag_started_at_bkk"] else ""}{when:%H:%M} → {e(end)}</span><span>{e(elapsed(row["dag_duration_seconds"]))}</span></div>'
                    f'<div class="timetable-card-caption"><span>{e(caption)}</span><span class="timetable-state">{e(human(row["dag_state"]) or "No state")}</span></div></div>')
            cells.append(f'<td class="timetable-cell group-{GROUPS.index(group) if group in GROUPS else 6}"><div class="timetable-cards" style="--lane-columns:{model["columns"][group]}">'+''.join(cards)+'</div></td>')
        empty=not any(model['buckets'].get((slot,g)) for g in groups)
        row_class='dependency-slot-empty' if empty else ''
        if empty and slot%60:row_class+=' dependency-slot-half'
        body.append(f'<tr class="{row_class}" data-slot="{slot}" style="height:{model["row_heights"][slot]}px"><th scope="row" class="timetable-time"><span>{slot//60:02d}:{slot%60:02d}</span></th>'+''.join(cells)+'</tr>')
    colgroup=f'<colgroup><col style="width:{TIME_WIDTH}px">'+''.join(f'<col style="width:{model["widths"][g]}px">' for g in groups)+'</colgroup>'
    header='<thead><tr><th class="timetable-corner" scope="col">Time <span>BKK</span></th>'+''.join(f'<th scope="col" class="timetable-group group-{GROUPS.index(g) if g in GROUPS else 6}"><strong>{e(g)}</strong><span>{model["totals"][g]} DAGs</span></th>' for g in groups)+'</tr></thead>'
    try:zoom=min(1.8,max(.1,float(w.params.get('zoom','1'))))
    except ValueError:zoom=1
    fit=w.params.get('zoom','1')=='fit'
    scene_zoom=f'min(1,calc((100cqw - 2px) / {model["width"]}px),calc((100cqh - 2px) / {model["height"]}px))' if fit else str(zoom)
    stage=(f'<div class="dependency-stage" style="width:{model["width"]}px;height:{model["height"]}px;zoom:{scene_zoom}">'
        f'<table class="timetable dependency-table" style="width:{model["width"]}px"><caption class="screen-reader">Today’s Bangkok DAG dependencies. Time is vertical; each DAG shows its latest run today. Links come from saved Airflow upstream tags and from Airflow’s own asset, trigger, sensor and S3-marker dependencies; downstream is derived by reversing them. Links do not verify run-to-run pairing.</caption>'
        +colgroup+header+'<tbody>'+''.join(body)+'</tbody></table>'
        +f'<svg xmlns="http://www.w3.org/2000/svg" class="dependency-arrows" width="{model["width"]}" height="{model["height"]}" aria-hidden="true"><defs>'
        '<marker id="dep-arrow-up" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#bd8069"/></marker>'
        '<marker id="dep-arrow-down" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#389c88"/></marker></defs>'+''.join(paths)+'</svg></div>')
    depth='direct' if w.params.get('depth')=='direct' else 'all'
    depth_controls='<div class="native-depth tab-row">'+''.join(f'<label><input type="radio" name="depth" form="dependency-state" id="dependency-depth-{k}" value="{k}"'+(' checked' if depth==k else '')+f'><span>{v}</span></label>' for k,v in (('all','All levels'),('direct','Direct')))+'</div>'
    fullscreen_checked=' checked' if w.params.get('dependency_fullscreen')=='1' else ''
    fullscreen=f'<label class="button small lineage-fullscreen" title="Toggle full screen dependency"><input type="checkbox" id="dependency-fullscreen" name="dependency_fullscreen" value="1" form="dependency-state" aria-label="Full screen dependency" autocomplete="off"{fullscreen_checked}><svg class="enter-fullscreen" width="18" height="18" viewBox="0 0 18 18" aria-hidden="true"><path d="M6 2H2V6M12 2H16V6M16 12V16H12M6 16H2V12"/></svg><svg class="leave-fullscreen" width="18" height="18" viewBox="0 0 18 18" aria-hidden="true"><path d="M2 6H6V2M12 2V6H16M16 12H12V16M6 16V12H2"/></svg></label>'
    fields={'view':'dependency',**{k:v for k,v in w.params.items() if k in ('state','tag','tag_q') or k.startswith('tag_')}}
    form='<form id="dependency-state" action="/app" method="get">'+''.join(f'<input type="hidden" name="{e(k)}" value="{e(v)}">' for k,v in fields.items())+'</form>'
    close_input='<input type="radio" id="dependency-focus-none" name="focus" value="" form="dependency-state" aria-label="Close DAG details"'+(' checked' if not selected else '')+'>'
    zoom_label='Fit' if fit else f'{zoom:.0%}'
    swap_checked=' checked' if w.params.get('panel_side')=='swap' else ''
    hide_checked=' checked' if w.params.get('panel_hidden')=='1' else ''
    toolbar=f'<button class="button small" form="dependency-state" name="zoom" value="{round(max(.1,zoom-.15),2)}" aria-label="Zoom out">−</button><span class="mono dependency-zoom-value">{zoom_label}</span><button class="button small" form="dependency-state" name="zoom" value="{round(min(1.8,zoom+.15),2)}" aria-label="Zoom in">+</button>'
    toolbar+='<button class="button small" form="dependency-state" name="zoom" value="fit">Fit</button><button class="button small" form="dependency-state" name="zoom" value="1">Reset</button>'
    toolbar+=f'<button class="button small" form="dependency-state" formaction="/dependency.svg" name="zoom" value="{e(w.params.get("zoom","1"))}">↓ SVG</button>'
    count=sum(len(n['runs']) for n in nodes.values())
    tag_notice='<div class="notice">No upstream tags or Airflow links · Import data after syncing Airflow tags</div>' if model['coverage'] and all(v=='missing' for v in model['coverage'].values()) and not model['linked'] else ''
    return (search_html+'<style>'+''.join(styles)+'</style><section class="panel dependency-panel">'+form+'<div class="panel-heading dependency-toolbar"><div class="timetable-title"><h2>Dependency</h2>'
        f'<span class="count">{len(nodes)} DAGs · {count} runs</span></div><div class="graph-controls">{depth_controls}{toolbar}{fullscreen}</div></div>'
        +tag_notice+'<div class="dependency-shell"><div class="dependency-scroll" tabindex="0" role="region" aria-label="Today’s DAG dependency timetable">'+stage+'</div>'
        f'<aside class="dependency-sidebar" aria-label="Selected DAG details"><label for="dependency-focus-none" class="dependency-detail-close" title="Close DAG details">{close_input}<span aria-hidden="true">×</span></label><label class="dependency-detail-close dependency-panel-swap" title="Move details to the other side"><input type="checkbox" id="dependency-panel-swap" name="panel_side" value="swap" form="dependency-state" aria-label="Move details to the other side" autocomplete="off"{swap_checked}><span aria-hidden="true">⇆</span></label>'
        f'<label class="dependency-detail-close dependency-panel-hide" title="Hide or show DAG details"><input type="checkbox" id="dependency-panel-hide" name="panel_hidden" value="1" form="dependency-state" aria-label="Hide DAG details" autocomplete="off"{hide_checked}><span class="hide-icon" aria-hidden="true">–</span><span class="show-icon" aria-hidden="true">Details</span></label><div class="dependency-placeholder"><h2>Select a DAG</h2><p>Latest run today</p><div class="dependency-legend"><span class="up">Upstream</span><span class="down">Downstream</span></div><div class="dependency-legend dependency-kinds"><span class="kind-tag">Tag</span><span class="kind-asset">Asset</span><span class="kind-trigger">Trigger</span></div></div>'
        +''.join(details)+'</aside></div></section>')


def dependency_svg(workspace):
    """Portable vector export of the current DAG selection and dependency depth."""
    import textwrap
    from lineage_app.views.render import e,elapsed,fmt,human,label
    w=workspace
    m=dependency_model(w);nodes=m['nodes'];focus=w.params.get('focus','')
    if focus not in nodes:focus=''
    full=w.params.get('depth')!='direct'
    ancestors=trace(focus,m['up'],full) if focus else set()
    descendants=trace(focus,m['down'],full) if focus else set()
    related=ancestors|descendants|{focus}
    inventory={d['dag_id']:d for d in w.inventory}
    width=m['width'];height=m['height']+56
    try:zoom=min(1.8,max(.1,float(w.params.get('zoom','1'))))
    except ValueError:zoom=1
    colors=['#f4f9ff','#f3faf7','#fffbf1','#f8f5ff','#fff7f5','#fff6fc','#fcfdfd']
    provenance={'source':'airflow_upstream_tags+airflow_dependency_evidence','stage':w.meta['stage'],'snapshot_date':str(w.day),
        'upstream_coverage':m['coverage'],
        'relationship_level':'dag','group_source':'airflow_layer_tags','run_pairing_verified':False,'focus':focus or None,'depth':'all' if full else 'direct'}
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width*zoom:.0f}" height="{height*zoom:.0f}" viewBox="0 0 {width} {height}" role="img" aria-label="DAG dependency timetable">'
        '<title>DAG dependency timetable</title><desc>Declared DAG relationships; each card shows its latest observed run today. Run pairing is not verified.</desc>'
        f'<metadata>{e(json.dumps(provenance))}</metadata><rect width="100%" height="100%" fill="#fff"/>'
        '<defs><marker id="export-arrow-up" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#bd8069"/></marker>'
        '<marker id="export-arrow-down" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#389c88"/></marker>'
        '<marker id="export-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#a9b6ba"/></marker></defs>'
        '<g font-family="Arial,sans-serif"><text x="18" y="33" font-size="18" fill="#252a36">Dependency</text>'
        f'<text x="{width-18}" y="33" text-anchor="end" font-size="12" fill="#83949b">{w.day:%d %b %Y} · Bangkok · {len(nodes)} DAGs</text><g transform="translate(0 56)">']
    x=TIME_WIDTH
    for group in m['groups']:
        color=colors[GROUPS.index(group)] if group in GROUPS else colors[-1]
        svg.append(f'<rect x="{x}" y="0" width="{m["widths"][group]}" height="{m["height"]}" fill="{color}"/><path d="M{x},0 V{m["height"]}" stroke="#e1e8ea"/>'
            f'<text x="{x+8}" y="20" font-size="12" fill="#567f7b">{e(group)}</text><text x="{x+8}" y="35" font-size="9" fill="#93a0a7">{m["totals"][group]} DAGs</text>')
        x+=m['widths'][group]
    svg.append('<text x="6" y="20" font-size="10" fill="#798792">Time</text><text x="6" y="35" font-size="9" fill="#98a4ab">BKK</text>')
    y=HEADER_HEIGHT
    for slot,h in m['row_heights'].items():
        svg.append(f'<path d="M0,{y} H{width}" stroke="#e5ebed"/>')
        if slot%60==0 or any(m['buckets'].get((slot,g)) for g in m['groups']):
            svg.append(f'<text x="6" y="{y+12}" font-family="monospace" font-size="9" fill="#83929b">{slot//60:02d}:{slot%60:02d}</text>')
        y+=h
    for a,b in m['edges']:
        if a not in nodes or b not in nodes:continue
        color='#a9b6ba';marker='export-arrow'
        if focus:
            if a in ancestors|{focus} and b in ancestors|{focus}:color='#bd8069';marker='export-arrow-up'
            elif a in descendants|{focus} and b in descendants|{focus}:color='#389c88';marker='export-arrow-down'
            else:continue
        curve=dependency_curve(m,a,b);dash=DASH[edge_kind(m['kinds'].get((a,b),['tag']))]
        svg.append(f'<path data-upstream="{e(a)}" data-downstream="{e(b)}" data-link="{e(",".join(m["kinds"].get((a,b),["tag"])))}" d="{curve}" fill="none" stroke="{color}" stroke-width="2" opacity=".8"'+(f' stroke-dasharray="{dash}"' if dash else '')+f' marker-end="url(#{marker})"/>')
    for dag,node in nodes.items():
        x,y=m['positions'][dag];row=node['row'];outline='#dce3e5';fill='#fff'
        if focus==dag:outline='#00bfb5';fill='#edfcf9'
        elif dag in ancestors:outline='#c78779'
        elif dag in descendants:outline='#66b6a4'
        opacity='.22' if focus and dag not in related else '1'
        svg.append(f'<g opacity="{opacity}" data-dag="{e(dag)}" data-run="{e(row["run_id"])}"><title>{e(dag)} · {e(row["run_id"])}</title>'
            f'<rect x="{x}" y="{y}" width="{CARD_WIDTH}" height="{CARD_HEIGHT}" rx="8" fill="{fill}" stroke="{outline}" stroke-width="{2 if dag==focus else 1}"/>')
        name_limit=max(12,(CARD_WIDTH-26)//6)
        name=textwrap.shorten(label(inventory[dag]),width=name_limit,placeholder='…') if ' ' in label(inventory[dag]) else label(inventory[dag])[:name_limit-1]+('…' if len(label(inventory[dag]))>name_limit-1 else '')
        state_color={'failed':'#d76b64','success':'#73965c','running':'#5d9eb9','upstream_failed':'#bf9b76'}.get(row['dag_state'],'#a8b1b7')
        svg.append(f'<circle cx="{x+9}" cy="{y+13}" r="2.5" fill="{state_color}"/><text x="{x+17}" y="{y+17}" font-size="10" fill="#252a36">{e(name)}</text>')
        end=fmt(row['dag_completed_at_bkk'],False)[:5] if row['dag_completed_at_bkk'] else '—'
        svg.append(f'<text x="{x+8}" y="{y+31}" font-size="9" fill="#7d8e96">{node["when"]:%H:%M} → {e(end)}</text>'
            f'<text x="{x+CARD_WIDTH-8}" y="{y+31}" text-anchor="end" font-size="9" fill="#93a0a8">{e(elapsed(row["dag_duration_seconds"]))}</text></g>')
    svg.append('</g></g></svg>')
    return ''.join(svg)
