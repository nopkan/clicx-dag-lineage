# Two-source Airflow storage design

Status: implemented through `dp_dag_lineage_export` and Import data (see [DAG lineage export](dag-lineage-export.md)): per-source URL storage, per-date snapshots and cross-DAG link evidence with Airflow resolution. The union views, source-qualified combined queries and combined dependency visuals below remain proposed.

## Storage

Keep the existing tables, monthly partitions, daily filenames, and Bangkok snapshot date. Add a URL-derived source partition beneath each environment:

```text
data/
  sources.json
  uat/
    source_id=<URL-A-hash>/
      source.json
      metadata.json
      airflow_dag_inventory/prti_dttm=202611/2026-11-05.parquet
      airflow_task_definitions/prti_dttm=202611/2026-11-05.parquet
      airflow_dag_dependencies/prti_dttm=202611/2026-11-05.parquet
      airflow_dag_runs/prti_dttm=202611/2026-11-05.parquet
      airflow_task_runs/prti_dttm=202611/2026-11-05.parquet
      airflow_monitoring_logs/prti_dttm=202611/2026-11-05.parquet
      # Other collected tables use the same layout.
    source_id=<URL-B-hash>/
      source.json
      metadata.json
      airflow_dag_inventory/prti_dttm=202611/2026-11-05.parquet
      # Same layout, independently collected/imported.
```

`<URL-A-hash>` and `<URL-B-hash>` are placeholders for the full SHA-256 hex digest of each canonical Airflow base URL. Raw URLs do not become directory names. The source registry maps each ID to its canonical URL, a short display label, and dependency mode (`tags` for our source; `api` for the other source, subject to schema inspection). Credentials never enter this registry or Parquet metadata.

Canonicalize the base URL by lowercasing the scheme/hostname, removing a default port and trailing slash, and retaining any deployment path and its case. Reject credentials, query strings and fragments in base URLs. Different schemes, non-default ports, or deployment paths remain distinct sources. A changed hostname is a new source unless an explicit alias migration is configured; do not silently combine hosts.

The legacy single-source path has only `data/<stage>/<table>/...` and rejects a second URL for the same environment. Bundle collection removes that limitation by selecting stores and collection locks using `(stage, source_id)`.

## Writes and snapshot selection

- Refresh only the selected source; another source's files and token are unaffected.
- Retain current per-table behavior inside that source/date: run and task identities are upserted; current definitions, inventory, task edges and rebuilt monitoring files are replaced. The overwrite boundary is one source and one Bangkok date, never the whole environment.
- Keep collection metadata, interruption markers and freshness per source. An incomplete collection stays visibly incomplete.
- Union only explicitly selected sources from the same environment and requested Bangkok date. A missing partition is unavailable, not an empty successful collection. Never silently substitute yesterday's data.
- Show each source's collection time and coverage separately because the two collections may finish at different times.
- For the first imported files, preserve the original exports separately and write validated, normalized tables into the registered URL partition. Do not guess a URL or schema from filenames.

## Union and identities

Keep the existing 85-column `airflow_monitoring_logs` contract unchanged on disk. At read time, attach `source_id` and `airflow_url` from the selected store to every loaded table, then use `UNION ALL` after validating compatible column names and types. The second source needs an adapter if its Parquet schema differs.

All application joins, keys, caches, AI queries, exports and navigation must carry source identity:

| Entity | Combined identity |
| --- | --- |
| DAG | `stage, source_id, dag_id` |
| DAG run | `stage, source_id, dag_id, run_id` |
| Task definition | `stage, source_id, dag_id, task_id` |
| Task instance | `stage, source_id, dag_id, run_id, task_id, map_index` |
| Attempt/log | Task instance identity plus `try_number` |

Existing `record_id`, `dag_run_key` and `task_run_key` values remain source-local; qualify them with the source when using them globally. Never merge runtime/state records merely because DAG names or run IDs match. Airflow links and log requests always use the owning source URL and token. AI conversation snapshot identity must include every selected source and its collection revision.

## Dependency stitching

1. Our source derives DAG edges from its saved `upstream:<dag_id>` tags, as today. Layer grouping also uses its own saved tags.
2. The other source derives DAG edges from its dependency API/export. Confirm that these are cross-DAG edges; task edges within a DAG cannot be treated as cross-DAG dependencies.
3. Normalize both into a separate DAG-edge model with `snapshot_date`, upstream/downstream source and DAG IDs, `edge_kind`, evidence/source reference, and collection time. Preserve the original API fields in raw data. Do not repurpose the existing `airflow_dag_dependencies` table: that table contains task-to-task edges within a DAG.
4. Index exact `dag_id` matches across the two selected sources in the same environment. Use DAG IDs, not display names, and do not strip suffixes or change case. These matches are the requested bridge between the two graphs. Keep their original source-qualified nodes, with a shared logical DAG group for combined tracing; do not invent a directional scheduling edge between equal names.
5. When an upstream declaration names a DAG absent locally, resolve it to the other selected source only if the exact match is unique. Keep unresolved or ambiguous references visible. If the DAG exists in both sources, retain the local edge and record the cross-source name match separately.
6. Derive downstream by reversing the resolved directed dependency edges. Traverse through logical name-match groups to connect both graphs, guarding against cycles and duplicate display edges. Preserve evidence when two sources declare the same logical edge.

For example, source A declares `raw_orders → daily_orders` and source B reports `daily_orders → sales_report`. Exact matching of `daily_orders` allows the combined view to trace `raw_orders → daily_orders → sales_report`. Expanding `daily_orders` still exposes two independent source records/runs. Matching names is a configured business mapping, not proof that Airflow enforces an execution dependency between the two deployments. Conflicting schedules, states, tags or project values must remain source-specific rather than taking an arbitrary winner.

## URL filter and page behavior

Use a shared source selector: **All Airflows**, **Data Platform**, **Risk Team**, with labels mapped to URLs. Tables and metrics follow that selection and show source labels when combined. The count of DAGs means source-qualified DAGs; logical name groups are a separate count if shown.

In Dependency, selecting a source makes its DAGs the focus while retaining related nodes from the other loaded source as labelled context. This avoids a URL filter breaking the very cross-source paths the user wants to monitor. A source that was never loaded remains unavailable and cannot supply hidden edges.

Planner must select a concrete source-qualified run for measured timing. It cannot take the runtime from one copy of a matching DAG and the status from the other. Until cross-source execution semantics are confirmed, mark stitched-name timing checks as inferred and retain the original edge evidence.

## Migration and implementation order

1. Inspect the incoming Parquet schema, metadata, timestamp timezone and dependency evidence, and associate it with its actual Airflow base URL.
2. Add source registration and source-scoped storage. Copy the old environment cache into the source partition identified by its existing `metadata.json.base_url`; verify row counts and schemas before switching reads. Retain the old files until verification succeeds.
3. Add source-qualified union/keys and source-aware links, logs, refresh, raw downloads and AI snapshot context.
4. Add the second dependency adapter and exact-name stitching, then the URL selector and combined graph.
5. Verify two same-stage/same-date sources cannot overwrite or merge each other's identities, refreshing one leaves the other unchanged, unavailable partitions remain explicit, old monitoring schemas are preserved, and graph/export/AI results retain provenance.

This design is local-filesystem first and can use the same source/date key structure under a future S3 prefix.
