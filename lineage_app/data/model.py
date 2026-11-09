"""The monitoring contract shared by API collection, Parquet, and HTML views."""
import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from lineage_app.paths import CONFIG_DIR

BKK = ZoneInfo('Asia/Bangkok')
CONTRACT = json.loads((CONFIG_DIR/'monitoring-schema.json').read_text())
DAG_FIELDS = {
    'dag_display_name': 'dag_display_name', 'timetable_summary': 'dag_timetable_summary',
    'timetable_description': 'dag_timetable_description', 'timezone': 'dag_timezone',
    'is_paused': 'dag_is_paused', 'catchup': 'dag_catchup',
    'has_import_errors': 'dag_has_import_errors', 'max_active_runs': 'dag_max_active_runs',
    'max_active_tasks': 'dag_max_active_tasks',
}
DEFINITION_FIELDS = {
    'owner': 'task_definition_owner', 'trigger_rule': 'task_definition_trigger_rule',
    'retries': 'task_definition_retries', 'depends_on_past': 'task_definition_depends_on_past',
    'wait_for_downstream': 'task_definition_wait_for_downstream',
}
INSTANCE_FIELDS = {
    'max_tries': 'task_max_tries', 'pool': 'task_pool', 'pool_slots': 'task_pool_slots',
    'queue': 'task_queue', 'priority_weight': 'task_priority_weight', 'executor': 'task_executor',
}

def timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('API timestamps must include their timezone')
    return result.astimezone(timezone.utc)

def bkk_wall(value):
    value = timestamp(value)
    return value.astimezone(BKK).replace(tzinfo=None) if value else None

def as_date(value):
    return date.fromisoformat(value) if isinstance(value, str) else value

def window(start, end):
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if start > end:
        raise ValueError('Start date must be on or before end date.')
    if (end - start).days > 365:
        raise ValueError('Please collect at most 366 dates at a time.')
    return start, end

def duration(raw):
    if raw.get('duration') is not None:
        return max(0.0, float(raw['duration']))
    start, end = timestamp(raw.get('start_date')), timestamp(raw.get('end_date'))
    return max(0.0, (end-start).total_seconds()) if start and end else None

def map_dag(stage, run, details, now, base):
    start, queued = timestamp(run.get('start_date')), timestamp(run.get('queued_at'))
    anchor = start or queued or timestamp(run.get('run_after'))
    if anchor is None:
        raise ValueError('DAG run has no start, queue, or run_after timestamp')
    from urllib.parse import quote
    row = dict(stage=stage, dag_id=run['dag_id'], run_id=run['dag_run_id'],
               run_date=anchor.astimezone(BKK).date(), state=run.get('state'),
               run_type=run.get('run_type'), logical_date=timestamp(run.get('logical_date')),
               data_interval_start=timestamp(run.get('data_interval_start')),
               data_interval_end=timestamp(run.get('data_interval_end')),
               run_after=timestamp(run.get('run_after')), queued_at=queued, started_at=start,
               completed_at=timestamp(run.get('end_date')), duration_seconds=duration(run),
               dag_run_url=f"{base}/dags/{quote(run['dag_id'], safe='')}/runs/{quote(run['dag_run_id'], safe='')}",
               collected_at=now, dag_details_collected_at_utc=now,
               dag_run_triggered_by=run.get('triggered_by'))
    row.update({target: details.get(source) for source, target in DAG_FIELDS.items()})
    row.update(dag_owners_json=json.dumps(details.get('owners', [])),
               dag_tags_json=json.dumps(details.get('tags', [])))
    return row

def map_task(run, task, definition, now):
    if task.get('dag_id') != run['dag_id'] or task.get('dag_run_id') != run['run_id']:
        raise ValueError('Task response does not match its requested DAG run')
    for key in ('map_index', 'try_number'):
        if type(task.get(key)) is not int:
            raise ValueError(f'Task response needs integer {key}')
    row = {key: run[key] for key in ('stage', 'dag_id', 'run_id', 'run_date')}
    row.update(task_id=task['task_id'], map_index=task['map_index'], try_number=task['try_number'],
               state=task.get('state'), operator=task.get('operator') or definition.get('operator_name'),
               queued_at=timestamp(task.get('queued_when')), started_at=timestamp(task.get('start_date')),
               completed_at=timestamp(task.get('end_date')), duration_seconds=duration(task),
               collected_at=now, task_details_collected_at_utc=now)
    row.update({target: definition.get(source) for source, target in DEFINITION_FIELDS.items()})
    row.update({target: task.get(source) for source, target in INSTANCE_FIELDS.items()})
    return row

def hash_key(**values):
    # Spark to_json(named_struct(...)) omits null fields by default. Field order matters.
    text = json.dumps({k: v for k, v in values.items() if v is not None},
                      ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(text.encode()).hexdigest()

def monitoring_rows(dags, tasks, now):
    """85 target columns; same identity, Bangkok wall times, and 7-day benchmarks as ETL."""
    # A queued run can move to another Bangkok date when its actual start becomes known.
    # Mirror dag_latest/task_latest across all raw daily files before building the model.
    def latest(rows, fields):
        indexed = {}
        for row in rows:
            key = tuple(row[k] for k in fields)
            previous = indexed.get(key)
            order = (row['collected_at'], row.get('try_number') or 0)
            if previous is None or order > (previous['collected_at'], previous.get('try_number') or 0):
                indexed[key] = row
        return list(indexed.values())
    dags = latest(dags, ('stage','dag_id','run_id'))
    tasks = latest(tasks, ('stage','dag_id','run_id','task_id','map_index'))
    instances = defaultdict(list)
    for task in tasks:
        instances[(task['stage'], task['dag_id'], task['run_id'])].append(task)
    dag_history, task_history = defaultdict(list), defaultdict(list)
    dag_lookup = {(r['stage'], r['dag_id'], r['run_id']): r for r in dags}
    for run in dags:
        if run['state'] == 'success' and (run['duration_seconds'] or 0) > 0:
            dag_history[(run['stage'], run['dag_id'], run['run_type'])].append(run)
    for task in tasks:
        run = dag_lookup.get((task['stage'], task['dag_id'], task['run_id']))
        if run and task['state'] == 'success' and (task['duration_seconds'] or 0) > 0:
            task_history[(task['stage'], task['dag_id'], task['task_id'], task['map_index'], run['run_type'])].append(task)
    rows = []
    for run in dags:
        key = (run['stage'], run['dag_id'], run['run_id'])
        for task in [None] + instances[key]:
            r = dict.fromkeys(CONTRACT['columns'])
            r.update({k: run[k] for k in ('stage', 'dag_id', 'run_id', 'run_date', 'run_type', 'dag_run_url')})
            r.update(record_level='task' if task else 'dag', prti_dttm=run['run_date'].strftime('%Y%m'),
                     dag_state=run['state'], dag_duration_seconds=run['duration_seconds'],
                     dag_duration_minutes=run['duration_seconds']/60 if run['duration_seconds'] is not None else None,
                     dag_collected_at_utc=run['collected_at'].replace(tzinfo=None),
                     model_updated_at_utc=now.replace(tzinfo=None), has_job_log=False, has_glue_run=False,
                     job_title='Task' if task else 'DAG', type=task.get('operator') if task else 'dag')
            for source, target in [('logical_date','dag_logical_date_bkk'), ('data_interval_start','data_interval_start_bkk'),
                ('data_interval_end','data_interval_end_bkk'), ('queued_at','dag_queued_at_bkk'),
                ('started_at','dag_started_at_bkk'), ('completed_at','dag_completed_at_bkk')]:
                r[target] = bkk_wall(run.get(source))
            for k in (*DAG_FIELDS.values(), 'dag_owners_json', 'dag_tags_json',
                      'dag_details_collected_at_utc', 'dag_run_triggered_by'):
                r[k] = run.get(k)
            if task:
                r.update({k: task.get(k) for k in ('task_id','map_index','try_number')})
                r.update(task_operator=task['operator'], task_state=task['state'],
                         task_duration_seconds=task['duration_seconds'],
                         task_duration_minutes=task['duration_seconds']/60 if task['duration_seconds'] is not None else None,
                         task_collected_at_utc=task['collected_at'].replace(tzinfo=None))
                for source, target in [('queued_at','task_queued_at_bkk'),('started_at','task_started_at_bkk'),('completed_at','task_completed_at_bkk')]:
                    r[target] = bkk_wall(task.get(source))
                for k in (*DEFINITION_FIELDS.values(), *INSTANCE_FIELDS.values(), 'task_details_collected_at_utc'):
                    r[k] = task.get(k)
            r['dag_run_key'] = hash_key(stage=r['stage'], dag=r['dag_id'], run=r['run_id'])
            r['task_run_key'] = hash_key(stage=r['stage'], dag=r['dag_id'], run=r['run_id'], task=r['task_id'], map=r['map_index']) if task else None
            r['record_id'] = hash_key(stage=r['stage'], dag=r['dag_id'], run=r['run_id'], level=r['record_level'], task=r['task_id'], map=r['map_index'])
            for level, history in [('dag', dag_history[(run['stage'],run['dag_id'],run['run_type'])]),
                    ('task', task_history[(run['stage'],run['dag_id'],task['task_id'],task['map_index'],run['run_type'])] if task else [])]:
                samples = [h['duration_seconds'] for h in history if timedelta(days=1) <= run['run_date']-h['run_date'] <= timedelta(days=7)]
                r[f'{level}_benchmark_samples'] = len(samples) if task or level == 'dag' else None
                avg = sum(samples)/len(samples) if len(samples) >= 3 else None
                r[f'{level}_avg_duration_7d_seconds'] = avg
                current = r[f'{level}_duration_seconds']
                done = r[f'{level}_state'] in ('success','failed')
                r[f'{level}_slowdown_pct'] = (current/avg-1)*100 if avg and current is not None and done else None
                r[f'is_slow_{level}'] = bool(avg and current is not None and done and current > avg*1.2)
            rows.append(r)
    return rows

def job_groups(definitions, edges):
    """Collapse only complete, connected define/trigger pairs with a shared prefix."""
    children = defaultdict(set)
    ids = {d['task_id'] for d in definitions}
    edges = {(a,b) for a,b in edges if a in ids and b in ids}
    for task_id in ids:
        if '.' in task_id:
            prefix, leaf = task_id.rsplit('.', 1)
            children[prefix].add(leaf)
    groups = {}
    for prefix, leaves in sorted(children.items()):
        if leaves != {'define_glue_job', 'trigger_glue_job'}: continue
        pair = (prefix+'.define_glue_job', prefix+'.trigger_glue_job')
        if pair not in edges: continue
        # Contracting a pair with an external path between its children would
        # introduce a cycle and conceal that path. Leave those tasks separate.
        adjacency = defaultdict(set)
        for a,b in edges-{pair}: adjacency[a].add(b)
        if pair[1] in trace(pair[0], adjacency) or pair[0] in trace(pair[1], adjacency): continue
        groups[prefix] = pair
    return groups


def graph_layout(definitions, edges):
    """Stable topological columns, preserving isolated tasks and exposing cycles."""
    ids = {d['task_id'] for d in definitions}
    up, down = {n:set() for n in ids}, {n:set() for n in ids}
    for a,b in edges:
        if a in ids and b in ids:
            up[b].add(a); down[a].add(b)
    rank = {n:0 for n in ids}
    remaining = {n:len(up[n]) for n in ids}
    queue = sorted(n for n in ids if not remaining[n]); visited = set()
    while queue:
        n = queue.pop(0); visited.add(n)
        for target in sorted(down[n]):
            rank[target] = max(rank[target], rank[n]+1)
            remaining[target] -= 1
            if remaining[target] == 0: queue.append(target)
    cyclic = ids-visited
    if cyclic:
        last = max(rank.values(), default=0)+1
        for n in cyclic: rank[n] = last
    columns = defaultdict(list)
    for n in sorted(ids): columns[rank[n]].append(n)
    positions = {}
    for col,nodes in columns.items():
        for i,n in enumerate(nodes): positions[n] = (48+col*338, 84+i*138)
    return positions, up, down, cyclic

def trace(start, adjacency, full=True):
    seen, stack = set(), list(adjacency.get(start, ()))
    while stack:
        node = stack.pop()
        if node == start or node in seen: continue
        seen.add(node)
        if full: stack.extend(adjacency.get(node, ()))
    return seen
