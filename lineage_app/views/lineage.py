"""Stable lineage interactions using native HTML form controls and CSS only."""
from collections import defaultdict
from html import escape

from lineage_app.data.model import graph_layout, job_groups, trace


def live_lineage(workspace):
    # Import here because Workspace uses this renderer after its module loads.
    from lineage_app.views.render import combined_state, human
    w=workspace
    definitions=list(w.definitions)
    ids={d['task_id'] for d in definitions}
    historical={t['task_id'] for t in w.instances}-ids
    definitions += [dict(task_id=t) for t in sorted(historical)]
    if not definitions:return w.graph_svg()[0],w.render_detail({},{}),''
    ids |= historical
    _,up,down,cyclic=graph_layout(definitions,w.edges)
    groups=job_groups(definitions,w.edges)
    membership={task:prefix for prefix,pair in groups.items() for task in pair}
    expanded=set(w.params.get('expanded','').split(',')) & groups.keys()
    if w.task_id in membership:expanded.add(membership[w.task_id])
    task_keys={task:f'task-{i}' for i,task in enumerate(sorted(ids))}
    group_keys={prefix:f'job-{i}' for i,prefix in enumerate(groups)}
    unit={task:'group:'+membership[task] if task in membership else task for task in ids}
    edges={(unit[a],unit[b]) for a,b in w.edges if a in unit and b in unit and unit[a]!=unit[b]}
    layout,_,_,_=graph_layout([{'task_id':n} for n in set(unit.values())],edges)
    # Reserve the expanded footprint even while closed. All nodes, connectors,
    # canvas bounds and Fit scale keep the same coordinates during interaction.
    sizes={n:(656,158) if n.startswith('group:') else (280,96) for n in layout}
    columns=defaultdict(list)
    for n,(x,y) in layout.items():columns[x].append((y,n))
    positions={};column_x=[];x=48
    for col in sorted(columns):
        column_x.append(x);y=84
        for _,n in sorted(columns[col]):
            positions[n]=(x,y);y+=sizes[n][1]+42
        x+=max(sizes[n][0] for _,n in columns[col])+70
    width=max(x+sizes[n][0] for n,(x,y) in positions.items())+48
    height=max(y+sizes[n][1] for n,(x,y) in positions.items())+48
    styles=[];content=[];paths=[];panes=[];inner_paths=defaultdict(list)
    esc=lambda v:escape(str(v),quote=True)
    active_conditions={}
    for task,key in task_keys.items():
        condition=f':has(#{key}:checked)'
        if task in membership:condition=f':has(#{group_keys[membership[task]]}:checked)'+condition
        active_conditions[task]=condition
    by_task=defaultdict(list)
    for t in w.instances:by_task[t['task_id']].append(t)
    def state(tasks):
        states=[t['task_state'] for task in tasks for t in by_task[task]]
        if any(not by_task[task] for task in tasks):states.append(None)
        value=combined_state(states)
        return value, human(value) or ('Pending' if any(by_task[t] for t in tasks) else 'No run')
    def task_box(task,x,y):
        key=task_keys[task];value,caption=state((task,))
        leaf=task.rsplit('.',1)[-1]
        prefix=task.rsplit('.',1)[0] if '.' in task else w.dag_id
        checked=' checked' if task==w.task_id else ''
        return (f'<div class="live-node {key}" data-task="{esc(task)}" style="left:{x}px;top:{y}px" title="{esc(task)}">'
            f'<input class="task-select" type="radio" form="lineage-state" name="task" id="{key}" value="{esc(task)}" aria-label="Inspect {esc(task)}"{checked}>'
            f'<input class="task-deselect" type="radio" form="lineage-state" name="task" value="" aria-label="Deselect {esc(task)}">'
            f'<div class="live-card"><span class="live-state {esc(value or "pending")}">{esc(caption)}</span>'
            f'<strong>{esc(leaf)}</strong><span class="live-subtitle">{esc(prefix)}</span></div></div>')
    for n,(x,y) in positions.items():
        if not n.startswith('group:'):
            content.append(task_box(n,x,y));continue
        prefix=n[6:];key=group_keys[prefix];pair=groups[prefix]
        value,caption=state(pair);checked=' checked' if prefix in expanded else ''
        content.append(f'<div class="live-job {key}" style="left:{x}px;top:{y}px" data-group="{esc(prefix)}">'
            f'<input type="checkbox" class="job-toggle" form="lineage-state" name="expanded" id="{key}" value="{esc(prefix)}" aria-label="Expand or collapse {esc(prefix)}"{checked}>'
            f'<div class="live-card job-closed"><span class="live-state {esc(value or "pending")}">{esc(caption)}</span>'
            f'<strong>{esc(prefix)}</strong><span class="live-subtitle">2 tasks · Expand +</span></div>'
            f'<div class="job-open"><svg class="job-internal" width="656" height="158" aria-hidden="true">__INNER_{key}__</svg><div class="job-heading"><strong title="{esc(prefix)}">{esc(prefix)}</strong><span>−</span></div>'
            +task_box(pair[0],16,46)+task_box(pair[1],360,46)+'</div></div>')
    # A dependency can have a closed or open endpoint on each side. Render
    # every endpoint combination once, then reveal only the matching paths.
    visible_edges=defaultdict(list)
    def endpoints(task):
        x,y=positions[unit[task]]
        if task not in membership:return [(task,{},(x,y,280,96))]
        prefix=membership[task];key=group_keys[prefix];index=groups[prefix].index(task)
        return [(unit[task],{key:False},(x,y,280,96)),
                (task,{key:True},(x+16+344*index,y+46,280,96))]
    for a,b in w.edges:
        if a not in ids or b not in ids:continue
        for va,ca,ba in endpoints(a):
            for vb,cb,bb in endpoints(b):
                if va==vb or any(k in cb and cb[k]!=v for k,v in ca.items()):continue
                conditions=tuple(sorted({**ca,**cb}.items()))
                visible_edges[(va,vb,conditions,ba,bb)].append((a,b))
    edge_records=[]
    for i,((a,b,conditions,ba,bb),originals) in enumerate(sorted(visible_edges.items())):
        key=f'edge-{i}';ax,ay,aw,ah=ba;bx,by,bw,bh=bb
        ax+=aw;ay+=ah/2;by+=bh/2;mid=(ax+bx)/2
        condition=''.join(f':has(#{k}:checked)' if on else f':not(:has(#{k}:checked))' for k,on in conditions)
        if conditions:styles.append(f'.live-lineage{condition} .{key}{{display:inline}}')
        paths.append(f'<path class="live-edge {key}{" conditional-edge" if conditions else ""}" d="M{ax},{ay} C{mid},{ay} {mid},{by} {bx},{by}" marker-end="url(#live-arrow)"/>')
        if a in membership and membership.get(b)==membership[a]:
            # The group frame covers the scene's connector layer. Draw its
            # internal arrow above that frame, using the same highlight rule.
            inner_paths[group_keys[membership[a]]].append(f'<path class="live-edge {key}" d="M296,94 H360" marker-end="url(#live-arrow)"/>')
        edge_records.append((key,originals))
    for key in group_keys.values():
        content=[box.replace(f'__INNER_{key}__',''.join(inner_paths[key])) for box in content]
    original_task=w.task_id
    try:
        # Panels are already present, so choosing a task cannot trigger a page
        # navigation or change the graph's scroll container.
        w.task_id=''
        panes.append('<div class="live-dag-detail">'+w.render_detail(up,down,native=True)+'</div>')
        for task,key in task_keys.items():
            w.task_id=task
            panes.append(f'<div class="live-task-detail {key}-detail">'+w.render_detail(up,down,native=True)+'</div>')
            condition=active_conditions[task]
            base='.live-lineage'+condition
            styles.append(f'{base} .live-dag-detail{{display:none}}{base} .{key}-detail{{display:block}}')
            styles.append(f'{base} .live-node,{base} .job-closed{{opacity:.3}}{base} .live-edge{{opacity:.25}}')
            styles.append(f'{base} .{key}>.live-card{{background:#e9fbf9;border:2px solid #00bfb5}}')
            for depth,full in (('all',True),('direct',False)):
                related=trace(task,up,full)|trace(task,down,full)|{task}
                scope=base+f':has(#trace-{depth}:checked)'
                selectors=[f'{scope} .{task_keys[t]}' for t in sorted(related)]
                selectors += [f'{scope} .{group_keys[p]}>.job-closed' for p,pair in groups.items() if related.intersection(pair)]
                if selectors:styles.append(','.join(selectors)+'{opacity:1}')
                related_selectors=[f'{scope} .{task_keys[t]}>.live-card' for t in sorted(related-{task})]
                if related_selectors:styles.append(','.join(related_selectors)+'{border-color:#84d8d2}')
                selectors=[f'{scope} .{k}' for k,originals in edge_records if any(a in related and b in related for a,b in originals)]
                if selectors:styles.append(','.join(selectors)+'{opacity:1;stroke:#00a99f;stroke-width:2;marker-end:url(#live-arrow-active)}')
    finally:w.task_id=original_task
    # All controls share this form. Zoom/run/export can still round-trip their
    # actual checked state without JavaScript or stale initial URL parameters.
    fields={k:v for k,v in w.params.items() if k not in ('task','expanded','zoom','depth','native','run','fullscreen')}
    fields.update(view='lineage',dag=w.dag_id,native='1')
    form='<form id="lineage-state" action="/app" method="get">'+''.join(f'<input type="hidden" name="{esc(k)}" value="{esc(v)}">' for k,v in fields.items())+'</form>'
    try:zoom=min(1.8,max(.55,float(w.params.get('zoom','1'))))
    except ValueError:zoom=1
    fit=' fit' if w.params.get('zoom')=='fit' else ''
    headings=''.join(f'<text x="{col}" y="38">STEP {i:02d}</text>' for i,col in enumerate(column_x,1))
    drawing=(f'<svg xmlns="http://www.w3.org/2000/svg" class="graph-svg live-svg{fit}" width="{width*zoom:.0f}" height="{height*zoom:.0f}" viewBox="0 0 {width} {height}" aria-label="Task lineage for {esc(w.dag_id)}" role="group">'
        '<foreignObject x="0" y="0" width="100%" height="100%"><div xmlns="http://www.w3.org/1999/xhtml" class="live-scene" '
        f'style="width:{width}px;height:{height}px"><svg class="live-connections" width="{width}" height="{height}" aria-hidden="true">'
        '<defs><marker id="live-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#b4bec2"/></marker>'
        '<marker id="live-arrow-active" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#00a99f"/></marker></defs>'
        +headings+''.join(paths)+'</svg>'+''.join(content)+'</div></foreignObject></svg>')
    # CSS remains local to this one view. The SVG download uses the portable
    # server-rendered graph instead of browser-specific foreignObject controls.
    return '<style>'+''.join(styles)+'</style>'+drawing,''.join(panes),form
