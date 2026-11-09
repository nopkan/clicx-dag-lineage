"""Import dp_dag_lineage_export folders copied to data/<stage>/dag_lineage/<YYYY-MM-DD>/.

One date folder holds both Airflows. Every Parquet file is checked against
manifest.json (size and SHA-256) before anything is saved, so a partial copy is
rejected. Each complete source is rebuilt through the live collector's own
mapping (map_dag/map_task) and saved into data/<stage>/source_id=<id>/ as that
date's snapshot, which Local data can open. An import never replaces a newer
collection of the same source and date, so repeated imports are no-ops.
"""
import hashlib
import io
import json
import threading
from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from lineage_app import connections
from lineage_app.data.model import map_dag, map_task, timestamp
from lineage_app.data.operations import TABLES as OP_TABLES

FOLDER = 'dag_lineage'
FORMAT = 'clicx-dag-lineage'
COMMON = ('source_id', 'source_name', 'airflow_url', 'stage', 'snapshot_date', 'collected_at')
LINK_FIELDS = ('link_kind', 'evidence', 'upstream_dag_id', 'downstream_dag_id', 'upstream_source_id',
               'downstream_source_id', 'resolution', 'declared_by_dag_id', 'task_id', 'detail')
COLUMNS = {
    'airflow_dags': (*COMMON, 'dag_id', 'is_stale', 'raw_json'),
    'airflow_tasks': (*COMMON, 'dag_id', 'task_id', 'operator_name', 'raw_json'),
    'airflow_dag_runs': (*COMMON, 'dag_id', 'run_id', 'state', 'run_type', 'raw_json'),
    'airflow_task_instances': (*COMMON, 'dag_id', 'run_id', 'task_id', 'map_index', 'try_number', 'state', 'raw_json'),
    'airflow_dag_links': (*COMMON, *LINK_FIELDS, 'raw_json'),
    'airflow_dependency_graph': (*COMMON, 'element', 'node_id', 'node_type', 'label', 'source_node_id', 'target_node_id'),
    'airflow_operations': (*COMMON, 'resource', 'record_key', 'raw_json'),
}
LOCK = threading.Lock()
# Exports made before the sources were renamed still carry the old labels.
RENAMED = {'Our Airflow': 'Data Platform', 'Other Airflow': 'Risk Team'}


class ExportError(ValueError):
    pass


def load_manifest(folder):
    try:
        manifest = json.loads((Path(folder) / 'manifest.json').read_text(encoding='utf-8'))
    except FileNotFoundError:
        raise ExportError('manifest.json is missing; copy the whole date folder.') from None
    except (OSError, ValueError, UnicodeError):
        raise ExportError('manifest.json is unreadable.') from None
    if not isinstance(manifest, dict) or manifest.get('format') != FORMAT or manifest.get('version') != 1:
        raise ExportError('This is not a supported dp_dag_lineage_export folder.')
    if manifest.get('snapshot_date') != Path(folder).name:
        raise ExportError('The folder name does not match the manifest snapshot date.')
    try:
        date.fromisoformat(manifest['snapshot_date'])
        if timestamp(manifest.get('collected_at')) is None:
            raise ValueError()
    except (ValueError, TypeError):
        raise ExportError('The manifest has an invalid snapshot date or collection time.') from None
    if not isinstance(manifest.get('sources'), list) or not isinstance(manifest.get('files'), list):
        raise ExportError('The manifest has no source or file list.')
    return manifest


def read_export(folder):
    """Manifest plus every table's rows, after verifying the complete copy."""
    folder = Path(folder)
    manifest = load_manifest(folder)
    files = {f.get('name'): f for f in manifest['files'] if isinstance(f, dict)}
    if set(files) != {name + '.parquet' for name in COLUMNS}:
        raise ExportError('The manifest does not list the expected Parquet files.')
    tables = {}
    for name, columns in COLUMNS.items():
        entry = files[name + '.parquet']
        try:
            body = (folder / (name + '.parquet')).read_bytes()
        except OSError:
            raise ExportError(f'{name}.parquet is missing; copy the whole date folder.') from None
        if len(body) != entry.get('bytes') or hashlib.sha256(body).hexdigest() != entry.get('sha256'):
            raise ExportError(f'{name}.parquet does not match manifest.json; the copy is incomplete or mixes two runs.')
        table = pq.ParquetFile(io.BytesIO(body)).read()
        missing = [c for c in columns if c not in table.column_names]
        if missing:
            raise ExportError(f'{name}.parquet lacks columns: {", ".join(missing)}.')
        tables[name] = table.to_pylist()
    return manifest, tables


def build_result(manifest, source, tables, base):
    """The same result shape AirflowClient.collect() returns, from exported raw records."""
    sid, stage = source['source_id'], source['stage']
    now, today = timestamp(manifest['collected_at']), date.fromisoformat(manifest['snapshot_date'])
    rows = {name: [r for r in items if r['source_id'] == sid] for name, items in tables.items()}
    inventory = [json.loads(r['raw_json']) for r in rows['airflow_dags']]
    details = {d['dag_id']: d for d in inventory}
    definitions = [{**json.loads(r['raw_json']), 'dag_id': r['dag_id']} for r in rows['airflow_tasks']]
    by_dag = defaultdict(list)
    for task in definitions:
        by_dag[task['dag_id']].append(task)
    dependencies = []
    for dag in inventory:
        edges = {(t['task_id'], d) for t in by_dag[dag['dag_id']] for d in (t.get('downstream_task_ids') or [])}
        for a, b in sorted(edges or {(None, None)}, key=lambda e: (e[0] or '', e[1] or '')):
            dependencies.append(dict(stage=stage, dag_id=dag['dag_id'], snapshot_date=today,
                                     upstream_task_id=a, downstream_task_id=b, collected_at=now))
    runs = {}
    for record in rows['airflow_dag_runs']:
        run = json.loads(record['raw_json'])
        if run.get('dag_id') not in details:
            continue
        row = map_dag(stage, run, details[run['dag_id']], now, base)
        if row['run_date'] == today:
            runs[(run['dag_id'], run['dag_run_id'])] = row
    indexed = {(t['dag_id'], t['task_id']): t for t in definitions}
    task_runs = []
    for record in rows['airflow_task_instances']:
        task = json.loads(record['raw_json'])
        run = runs.get((task.get('dag_id'), task.get('dag_run_id')))
        if run:
            task_runs.append(map_task(run, task, indexed.get((run['dag_id'], task['task_id']), {}), now))
    operations = {key: [] for key in OP_TABLES}
    for record in rows['airflow_operations']:
        if record['resource'] in operations:
            operations[record['resource']].append(json.loads(record['raw_json']))
    operations['diagnostics'].sort(key=lambda r: (r['dag_id'], r['run_id'], r['task_id'], r['map_index']))
    links = [dict(stage=stage, snapshot_date=today, collected_at=now, **{k: r[k] for k in LINK_FIELDS})
             for r in rows['airflow_dag_links']]
    return dict(collected_at=now, snapshot_date=today, start=today, end=today, inventory=inventory,
                definitions=definitions, dependencies=dependencies, dag_runs=list(runs.values()),
                task_runs=task_runs, warnings=list(source.get('warnings') or []), operations=operations,
                operational_coverage=source.get('operational_coverage') or {},
                coverage=source.get('coverage') or {}, dag_links=links)


def already_current(meta, manifest):
    """meta is the same source's saved snapshot for the manifest's date, if any."""
    if not meta:
        return False
    try:
        return timestamp(meta['collected_at']) >= timestamp(manifest['collected_at'])
    except (KeyError, ValueError, TypeError):
        return False


def folders(data_root, stages):
    for stage in stages:
        for folder in sorted((Path(data_root) / stage / FOLDER).glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')):
            if folder.is_dir():
                yield stage, folder


def sync(data_root, stages, store_for, claim=lambda key: True, release=lambda key: None):
    """Import new copied exports. Returns notices for the connect page; never raises for a bad folder."""
    notices, newest = [], {}
    with LOCK:
        for stage, folder in folders(data_root, stages):
            newest[stage] = folder
        for stage, folder in folders(data_root, stages):
            label = f'{stage}/{FOLDER}/{folder.name}'
            report = folder == newest[stage]  # Older folders are history; only report the newest.
            try:
                manifest = load_manifest(folder)
            except ExportError as exc:
                if report:
                    notices.append(f'{label}: {exc}')
                continue
            pending = []
            for source in manifest['sources']:
                if not isinstance(source, dict):
                    continue
                source = dict(source, name=RENAMED.get(source.get('name'), source.get('name')))
                name = str(source.get('name') or 'Airflow')
                if source.get('status') != 'complete':
                    if report:
                        notices.append(f'{label}: {name} was not exported ({source.get("error") or "no reason recorded"}).')
                    continue
                try:
                    base = connections.canonical_url(source.get('base_url'))
                    if source.get('stage') != stage:
                        raise ExportError(f'{name} belongs in data/{source.get("stage")}/{FOLDER}/, not data/{stage}/.')
                    if connections.source_id(base) != source.get('source_id'):
                        raise ExportError(f'{name} has a source ID that does not match its URL.')
                except ValueError as exc:
                    if report:
                        notices.append(f'{label}: {exc}')
                    continue
                if not already_current(store_for(stage, base).at(manifest['snapshot_date']).metadata(), manifest):
                    pending.append((source, base))
            if not pending:
                continue
            try:
                manifest, tables = read_export(folder)
            except ExportError as exc:
                notices.append(f'{label}: {exc}')
                continue
            for source, base in pending:
                key = connections.cache_key(stage, base)
                if not claim(key):
                    notices.append(f'{label}: {source["name"]} is being collected; reload this page afterwards to import it.')
                    continue
                try:
                    result = build_result(manifest, source, tables, base)
                    store_for(stage, base).save_collection(
                        result, base, stage,
                        source_info=dict(source_id=source['source_id'], source_name=source['name'],
                                         dependency_mode=source.get('dependency_mode')),
                        extra_metadata=dict(
                            export=dict(folder=f'{FOLDER}/{folder.name}', producer=manifest.get('producer'),
                                        completed_at=manifest.get('completed_at')),
                            dependency_coverage=source.get('dependency_coverage') or {}))
                    notices.append(f'Imported {source["name"]} from {label}.')
                except (ValueError, KeyError, TypeError) as exc:
                    notices.append(f'{label}: {source["name"]} could not be imported ({exc}).')
                finally:
                    release(key)
    return notices
