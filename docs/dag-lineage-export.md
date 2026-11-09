# DAG lineage export (S3 → dashboard)

The other Airflow is reachable only inside AWS, so `dp_dag_lineage_export` (Clicx Config repo, `dags/dag_lineage_export_dag.py`) collects both Airflows there. It replaces `dp_dashboard_access_bundle`. The dashboard imports the copied folder; it never connects to S3 itself.

## Flow

```text
dp_dag_lineage_export (manual)  → s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/<YYYY-MM-DD>/
first page → Import data        → aws sso login (browser, only if expired) + aws s3 sync → data/uat/dag_lineage/
                                → imported into data/uat/source_id=<id>/ → opens the newest date
top-bar picker                  → any saved source and date
```

The date is the Bangkok date when the DAG task started. Import needs the AWS CLI on the machine running the dashboard and an AWS profile with `s3:ListBucket` and `s3:GetObject` on that prefix. The first page has fields for the S3 folder and environment; the AWS profile comes from `LINEAGE_AWS_PROFILE`. The last values used are remembered in a browser cookie (no credentials) and reused by Refresh. Their defaults come from environment variables:

| Variable | Default |
| --- | --- |
| `LINEAGE_AWS_PROFILE` | `nonprod_DP_DataEngineerOS` (the console role; `clicx-uat` is denied) |
| `LINEAGE_S3_URI` | `s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/` |
| `LINEAGE_S3_STAGE` | `uat` |

For a role-chained profile, sign-in uses the SSO profile at the end of its `source_profile` chain. The dashboard never stores AWS credentials; the AWS CLI keeps its own SSO cache. The manual equivalent is:

```sh
aws sso login --profile <your-sso-profile>   # the SSO profile behind nonprod_DP_DataEngineerOS
aws s3 sync s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/ data/uat/dag_lineage/ --profile nonprod_DP_DataEngineerOS
```

## Folder

All files contain both Airflows. Every row has `source_id` (SHA-256 of the canonical base URL), `source_name`, `airflow_url`, `stage`, `snapshot_date` and `collected_at`.

| File | Contents |
| --- | --- |
| `airflow_dags.parquet` | DAG list merged with DAG details (`raw_json`), including stale DAGs |
| `airflow_tasks.parquet` | Task definitions (`raw_json`) |
| `airflow_dag_runs.parquet` | Runs started or queued that Bangkok day (`conf` removed) |
| `airflow_task_instances.parquet` | Task instances of those runs (`rendered_fields` removed) |
| `airflow_dag_links.parquet` | Cross-DAG links with evidence and Airflow resolution |
| `airflow_dependency_graph.parquet` | Airflow's own dependency graph (`/ui/dependencies` nodes and edges) |
| `airflow_operations.parquet` | Health, import errors, failed-task tries and logs (token redacted) |
| `manifest.json` | Written last: per-source status, coverage and warnings; rows, bytes and SHA-256 per file |

## Cross-DAG links

| `link_kind` | `evidence` | Source |
| --- | --- | --- |
| `tag` | `dag_tags` | `upstream:<dag_id>` tags |
| `trigger`, `sensor`, `asset`, … | `dependency_graph` | The scheduler's dependency graph: `TriggerDagRunOperator`, `ExternalTaskSensor`, asset schedules |
| `asset` | `assets_api` | `/api/v2/assets` producing tasks and scheduled DAGs |
| `trigger`, `sensor`, `s3_marker`, `remote_trigger` | `rendered_fields` | Latest run's rendered `trigger_dag_id`, `external_dag_id(s)`, `dependency_dags` (the S3 marker sensor `wait_for_s3_dependency_markers`) and `…/dags/<id>/dagRuns` URLs |

`upstream_source_id` and `downstream_source_id` name the Airflow where each end exists. `resolution` is `same_airflow`, `cross_airflow`, `not_found`, `templated` or `unknown` (for example, an asset without a known producer). The same link can appear once per evidence type. Rendered fields come from today's run, otherwise from the latest run of sensor- or trigger-like tasks (at most 300 lookups per Airflow). Tasks that have never run cannot provide rendered evidence. Each source's `dependency_coverage` in the manifest records what was unavailable, such as a 403 or 404 from `/ui/dependencies`.

## Import rules

- Import data imports each new folder under `data/<stage>/dag_lineage/`, including folders copied there by hand. Problems are shown before the workspace opens. If the S3 download fails (no AWS CLI, sign-in, access, network), Import still offers the data downloaded earlier and says so.
- Every file must match the manifest's size and SHA-256. A partial copy, or files mixed from two runs, is rejected before anything is saved.
- A source's `stage` must match the folder (`data/uat/…`), and its `source_id` must match its URL.
- Each complete source is rebuilt with the live collector's own mapping and saved like a live collection, including the 85-column monitoring table. Links go to `airflow_dag_links` (Raw tab: *Cross-DAG links*; also queryable by AI). A source the DAG could not collect is reported and skipped.
- Every date is kept as its own snapshot. Re-importing an unchanged folder is a no-op; a newer export of the same date replaces it. Importing an older date never changes which date is newest.

The existing visuals still derive dependencies from tags. They do not yet draw `airflow_dag_links`.

## Docker

The image includes AWS CLI v2 (`AWS_CLI_VERSION` build argument) and sets `LINEAGE_AWS_DEVICE_CODE=1`: a container cannot open your Mac's browser, so an expired sign-in shows a link and code on the progress page instead.

Your Mac's `~/.aws` is **not** mounted. Put the SSO settings in `.env` (see `.env.example`): start URL, SSO region, account, and `LINEAGE_AWS_ROLES`, the roles to try in order (`ssoRole>roleArnToAssume,…`). After signing in, each person uses the first route their account has that can read the S3 folder: the role it assumes, then the SSO role directly. Someone with none of the roles is told which roles to request. They say where to sign in; they are not credentials, and `.env` is not committed.

Every browser session gets its **own** private AWS folder in the container's temporary storage. When someone clicks Import data, the page shows them an **Open AWS sign-in** button and a code, and they approve with their own AWS account. One person's sign-in is never used for another. Restarting the container clears all sign-ins.

Without the `LINEAGE_AWS_SSO_*` settings, Import reports that the profile is not configured and opens data already in the volume. You can also download on the host and copy the folder in, then click Import data:

```sh
docker compose -p clicx-lineage-test cp data/uat/dag_lineage lineage:/app/data/uat/
```
