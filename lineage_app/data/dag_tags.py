"""Cross-DAG declarations from saved Airflow tags; no external mappings."""
import json
import re
from collections import defaultdict

UNKNOWN = {'unknown', 'null', 'n/a', 'na', 'tbc', '-'}
DAG_ID = re.compile(r'[A-Za-z0-9_.-]{1,250}')


def dag_tag_graph(inventory):
    edges = set(); coverage = {}
    for item in inventory:
        dag = item.get('dag_id')
        if not dag:
            continue
        raw = item
        if 'tags' not in raw and isinstance(item.get('raw_json'), str):
            try:
                parsed = json.loads(item['raw_json'])
                raw = parsed if isinstance(parsed, dict) else {}
            except (ValueError, TypeError):
                raw = {}
        tags = raw.get('tags') or []
        if not isinstance(tags, (list, tuple, set)):
            tags = []
        found = set(); present = False; incomplete = False; empty = False
        for tag in tags:
            tag = tag.get('name') if isinstance(tag, dict) else tag
            if not isinstance(tag, str):
                continue
            key, separator, value = tag.partition(':')
            if not separator or key.strip().casefold() != 'upstream':
                continue
            present = True
            for value in value.split(','):
                value = value.strip()
                if not value or value.casefold() in UNKNOWN:
                    incomplete = True
                elif value.casefold() == 'none':
                    empty = True
                elif value == dag or not DAG_ID.fullmatch(value):
                    incomplete = True
                else:
                    found.add(value)
        if empty and found:
            incomplete = True
        coverage[dag] = ('missing' if not present else 'partial' if incomplete and found
                         else 'unknown' if incomplete else 'complete')
        edges.update((upstream, dag) for upstream in found)
    up = defaultdict(set); down = defaultdict(set)
    for a, b in sorted(edges):
        up[b].add(a); down[a].add(b)
    return dict(edges=sorted(edges), up=up, down=down, coverage=coverage)


# Airflow's own dependency evidence from dp_dag_lineage_export (airflow_dag_links).
# Tags are parsed from the inventory above; the export's tag rows are not counted twice.
LINK_LABELS = {'tag': 'Tag', 'asset': 'Asset', 'trigger': 'Trigger', 'sensor': 'Sensor',
               's3_marker': 'S3 marker', 'remote_trigger': 'Remote trigger', 'dag': 'Airflow graph'}


def dependency_graph(inventory, links=()):
    """Tag links plus Airflow-recorded links (assets, TriggerDagRunOperator, sensors, S3 markers).

    Only links whose two ends exist in an Airflow are drawn; an asset without a known
    producer, a templated or an unmatched DAG ID is left out. `kinds` keeps every
    edge's evidence so the page can say how two DAGs are connected.
    """
    graph = dag_tag_graph(inventory)
    kinds = defaultdict(set)
    for edge in graph['edges']:
        kinds[edge].add('tag')
    linked = set()
    for link in links:
        a, b = link.get('upstream_dag_id'), link.get('downstream_dag_id')
        if (not a or not b or a == b or link.get('evidence') == 'dag_tags'
                or link.get('resolution') not in ('same_airflow', 'cross_airflow')):
            continue
        kinds[(a, b)].add(link.get('link_kind') if link.get('link_kind') in LINK_LABELS else 'dag')
        linked.update((a, b))
    up = defaultdict(set); down = defaultdict(set)
    for a, b in sorted(kinds):
        up[b].add(a); down[a].add(b)
    return dict(edges=sorted(kinds), up=up, down=down, coverage=graph['coverage'],
                kinds={edge: sorted(values) for edge, values in kinds.items()}, linked=linked)
