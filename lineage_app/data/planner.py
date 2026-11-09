"""Read-only schedule scenarios using today's measurements and recorded DAG links (tags + Airflow dependencies)."""
import hashlib
import math
from collections import defaultdict,deque
from datetime import datetime,timedelta

from lineage_app.data.dag_tags import dependency_graph
from lineage_app.data.model import BKK,trace


def _clock(value,field):
    try:
        return datetime.strptime(value,'%H:%M:%S').time() if len(value)==8 else datetime.strptime(value,'%H:%M').time()
    except (ValueError,TypeError):raise ValueError(f'Enter a valid {field} time.') from None


def _cycle(nodes,edges):
    degree={n:0 for n in nodes};children=defaultdict(set)
    for a,b in edges:
        degree[b]+=1;children[a].add(b)
    ready=deque(n for n,v in degree.items() if v==0);count=0
    while ready:
        node=ready.popleft();count+=1
        for child in children[node]:
            degree[child]-=1
            if degree[child]==0:ready.append(child)
    return count!=len(nodes)


def _collected(meta):
    try:value=datetime.fromisoformat(str(meta.get('collected_at')))
    except (TypeError,ValueError):return None
    return value.astimezone(BKK).replace(tzinfo=None) if value.tzinfo else value


def plan_model(w):
    inventory={d['dag_id']:d for d in w.data['inventory']}
    focus=w.params.get('plan_dag') or w.params.get('focus') or w.dag_id
    if focus not in inventory:raise ValueError('Choose a DAG from the saved snapshot.')
    graph=dependency_graph(w.data['inventory'],w.data.get('dag_links',[]));up=graph['up'];down=graph['down']
    ancestors=trace(focus,up);descendants=trace(focus,down)
    family=ancestors|descendants|{focus}
    # Include co-parents of downstream DAGs, so their readiness is not ignored.
    scope=set(family)
    for dag in family:scope.update(trace(dag,up))
    edges=[(a,b) for a,b in graph['edges'] if a in scope and b in scope]
    key=hashlib.sha256('|'.join(str(w.meta.get(k,'')) for k in ('stage','snapshot_date','collected_at')).encode()).hexdigest()[:20]
    params=dict(w.params)
    reset=bool(params.get('plan_snapshot') and params['plan_snapshot']!=key)
    if reset:
        for k in ('plan_start','plan_end','plan_buffer','plan_shift'):params.pop(k,None)
    try:buffer=float(params.get('plan_buffer','0'))
    except (TypeError,ValueError):raise ValueError('Duration buffer must be between 0% and 200%.') from None
    if not math.isfinite(buffer) or not 0<=buffer<=200:raise ValueError('Duration buffer must be between 0% and 200%.')
    baseline={};scenario={};failures=[];collected=_collected(w.meta)
    for dag in sorted(scope):
        row=(w.by_dag.get(dag) or [None])[0]
        start=row.get('dag_started_at_bkk') if row else None
        end=row.get('dag_completed_at_bkk') if row else None
        completed=bool(row and row.get('dag_state') in ('success','failed'))
        duration=(end-start).total_seconds() if completed and start and end and end>=start else None
        if not completed or (start and end and end<start):end=None
        # An unfinished run is drawn up to collection time; it is never a duration.
        running=bool(row and start and not completed and collected and collected>start)
        baseline[dag]=dict(start=start,end=end,duration=duration,state=row.get('dag_state') if row else None,
            run_id=row.get('run_id') if row else None,running_until=collected if running else None)
        scenario[dag]=dict(start=start,end=(start+timedelta(seconds=duration*(1+buffer/100))) if start and duration is not None else end if buffer==0 else None,
            duration=duration*(1+buffer/100) if duration is not None else None)
        if row and row.get('dag_state')=='failed':failures.append(dag)
    selected=scenario[focus]
    if params.get('plan_start'):
        selected['start']=datetime.combine(w.day,_clock(params['plan_start'],'start'))
    if params.get('plan_shift'):
        try:shift=int(params['plan_shift'])
        except (ValueError,TypeError):raise ValueError('Choose a valid time adjustment.') from None
        if shift not in (-30,-15,15,30):raise ValueError('Choose a valid time adjustment.')
        if not selected['start']:raise ValueError('Set a start time first.')
        selected['start']+=timedelta(minutes=shift)
        if selected['start'].date()!=w.day:raise ValueError('Keep the proposed start within the snapshot day.')
    custom_end=params.get('plan_end','')
    if custom_end:
        if not selected['start']:raise ValueError('Set a start time before a custom end.')
        end=datetime.combine(w.day,_clock(custom_end,'end'))
        if end<=selected['start']:end+=timedelta(days=1)
        selected['end']=end;selected['duration']=(end-selected['start']).total_seconds()
    else:
        selected['end']=selected['start']+timedelta(seconds=selected['duration']) if selected['start'] and selected['duration'] is not None else None
    conflicts=[];resolved=[];unassessed=[]
    for a,b in edges:
        base_end=baseline[a]['end'];base_start=baseline[b]['start']
        end=scenario[a]['end'];start=scenario[b]['start']
        if end is None or start is None:
            unassessed.append((a,b));continue
        overlap=max(0,(end-start).total_seconds())
        old=max(0,(base_end-base_start).total_seconds()) if base_end and base_start else None
        if overlap>0:
            change='new' if old==0 else 'worse' if old is not None and overlap>old+1e-6 else 'existing' if old is not None else 'uncompared'
            conflicts.append(dict(upstream=a,downstream=b,end=end,start=start,overlap=overlap,baseline_overlap=old,change=change))
        elif old:
            resolved.append(dict(upstream=a,downstream=b,baseline_overlap=old))
    conflicts.sort(key=lambda r:(r['change']=='existing',-r['overlap'],r['upstream'],r['downstream']))
    gaps=[dag for dag,r in scenario.items() if r['start'] is None or r['end'] is None]
    tag_gaps=[dag for dag,state in graph['coverage'].items() if state!='complete']
    cyclic=_cycle(scope,edges)
    changed=sum(c['change'] in ('new','worse','uncompared') for c in conflicts)
    uncertain=bool(gaps or tag_gaps or cyclic or failures)
    status='Scenario conflicts' if changed else 'Existing overlaps' if conflicts else 'Needs review' if uncertain else 'No timing conflicts'
    level='danger' if changed else 'warning' if conflicts or uncertain else 'good'
    def links(rows):
        # Direct links of the selected DAG only; margin<0 means an overlap.
        out=[]
        for a in sorted(up.get(focus,()),key=lambda d:(rows[d]['end'] is None,rows[d]['end'] or datetime.min,d)):
            end=rows[a]['end'];start=rows[focus]['start']
            out.append(dict(dag=a,relation='upstream',margin=(start-end).total_seconds() if end and start else None))
        for b in sorted(down.get(focus,()),key=lambda d:(rows[d]['start'] is None,rows[d]['start'] or datetime.min,d)):
            end=rows[focus]['end'];start=rows[b]['start']
            out.append(dict(dag=b,relation='downstream',margin=(start-end).total_seconds() if end and start else None))
        return out
    def window(rows):
        ends=[rows[d]['end'] for d in up.get(focus,())];starts=[rows[d]['start'] for d in down.get(focus,()) if rows[d]['start']]
        return (max(ends) if ends and all(ends) else None,min(starts) if starts else None)
    today_links=links(baseline);new_links=links(scenario)
    base_ready,base_deadline=window(baseline)
    moved=(scenario[focus]['start'],scenario[focus]['end'])!=(baseline[focus]['start'],baseline[focus]['end'])
    ready,deadline=window(scenario)
    return dict(focus=focus,inventory=inventory,graph=graph,ancestors=ancestors,descendants=descendants,scope=scope,
        edges=edges,baseline=baseline,scenario=scenario,conflicts=conflicts,resolved=resolved,unassessed=unassessed,
        gaps=gaps,tag_gaps=tag_gaps,failures=failures,cyclic=cyclic,status=status,level=level,changed=changed,
        ready=ready,deadline=deadline,buffer=buffer,custom_end=custom_end,snapshot=key,reset=reset,
        today_links=today_links,new_links=new_links,base_ready=base_ready,base_deadline=base_deadline,moved=moved,
        parents=sorted(up.get(focus,())),children=sorted(down.get(focus,())),
        day=w.day,stage=w.meta['stage'],collected_at=w.meta['collected_at'])
