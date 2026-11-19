"""Cross-DAG declarations from saved Airflow tags; no external mappings."""
import json
import re
from collections import defaultdict

UNKNOWN = {'unknown', 'null', 'n/a', 'na', 'tbc', '-'}
DAG_ID = re.compile(r'[A-Za-z0-9_.-]{1,250}')


# Some DAGs pack several tags into one string ('team:risk_de, project:batch, frequency:daily').
# Split only where the next piece starts its own 'key:', so 'upstream:a, b' stays one tag.
PACKED = re.compile(r',\s*(?=[A-Za-z][A-Za-z0-9_.-]*\s*:)')


def split_packed(tag):
    return [part.strip() for part in PACKED.split(tag) if part.strip()]


def tag_names(item):
    """Tag names of one inventory record (or its raw_json); malformed data gives none."""
    raw = item
    if 'tags' not in raw and isinstance(item.get('raw_json'), str):
        try:
            parsed = json.loads(item['raw_json'])
            raw = parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            raw = {}
    tags = raw.get('tags') or []
    if not isinstance(tags, (list, tuple, set)):
        return []
    names = (tag.get('name') if isinstance(tag, dict) else tag for tag in tags)
    return [part for name in names if isinstance(name, str) for part in split_packed(name)]


def dag_tag_graph(inventory):
    edges = set(); coverage = {}
    for item in inventory:
        dag = item.get('dag_id')
        if not dag:
            continue
        found = set(); present = False; incomplete = False; empty = False
        for tag in tag_names(item):
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


def own_projects(item):
    """Real `project:` values on one DAG; empty, none and unknown values are not projects."""
    projects = set()
    for tag in tag_names(item):
        key, separator, value = tag.partition(':')
        value = value.strip()
        if separator and key.strip().casefold() == 'project' and value and value.casefold() not in UNKNOWN | {'none'}:
            projects.add(value)
    return projects


def inherited_projects(inventory, links=()):
    """Projects a DAG feeds: {dag_id: {project: [downstream DAGs that carry it]}}.

    A project tag passes to every upstream DAG at every level (A → B → C: C's project
    reaches B and A), never downstream or sideways, and across both Airflows. Links are
    the same as the Dependency page (tags plus Airflow's own links). A DAG's own
    projects are not repeated as inherited.
    """
    from lineage_app.data.model import trace
    graph = dependency_graph(inventory, links)
    own = {item['dag_id']: own_projects(item) for item in inventory if item.get('dag_id')}
    result = defaultdict(lambda: defaultdict(set))
    for dag, projects in own.items():
        for ancestor in trace(dag, graph['up']) if projects else ():
            for project in projects - own.get(ancestor, set()):
                result[ancestor][project].add(dag)
    return {dag: {project: sorted(sources) for project, sources in sorted(projects.items())}
            for dag, projects in result.items()}
