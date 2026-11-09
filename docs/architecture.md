# CLICX Lineage

Airflow monitoring for two Airflows, using **HTML, CSS, Python, SVG, and Parquet. No JavaScript.**

## Start

Python 3.10 or newer is required. Dependencies have already been installed in this workspace's `.venv`.

```sh
.venv/bin/python -B app.py
```

Open <http://127.0.0.1:8765>. **Import data** signs in to AWS (browser approval when the SSO session has expired), downloads the `dp_dag_lineage_export` folders from S3 and imports them. The top-bar picker opens any saved source and date. See [DAG lineage export](dag-lineage-export.md). This HTML is a Python template; run the server rather than double-clicking it.

On a new machine:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -B app.py
```

Use `--port 8766` for another port or `--data-dir /path/to/cache` for another local cache. The server binds only to `127.0.0.1`.

## Data flow

```text
dp_dag_lineage_export (inside AWS) reads both Airflow API v2s
  → s3://…/dag_lineage/<date>/ Parquet + manifest
  → Import data: aws sso login (if needed) + aws s3 sync → data/uat/dag_lineage/
  → Verified, mapped with the live collector's own code → per-source daily Parquet files
  → Build the 85-column monitoring model from saved raw files
  → Read saved Parquet
  → Python renders HTML and SVG
```

The browser sends ordinary HTML forms. Collection runs in a Python background thread; an HTML meta-refresh reports progress. Filters, run selection, and graph zoom are server requests. Task selection, dependency highlighting, group expansion, and full-window mode use native HTML controls and CSS without reloading the page. The browser neither calls Airflow nor reads binary Parquet directly.

The sidebar can collapse to icons. Its preference is saved in an HttpOnly browser cookie and applies across tabs, item navigation, reloads and local server restarts. The toggle uses an ordinary HTML form; graph selection, expanded groups, dependency depth and zoom are submitted with it. Changing this preference makes no Airflow requests or Parquet writes.

The permanent Settings gear opens display preferences: visible sidebar tabs, landing page, table spacing, text size and separate Data Platform/Risk Team tag colors. The native color pickers apply on Save; pale badge backgrounds and readable text colors are derived automatically and shared across all pages. Save changes uses a CSRF-protected HTML form and a separate validated, year-long HttpOnly browser cookie. Preferences survive disconnects and server/container restarts in that browser; they do not change data collection or restrict direct links to hidden tabs. At least one tab must remain visible. The default landing page follows the first visible tab; an explicitly chosen landing page must be visible. Restore defaults resets display preferences and preserves the independent sidebar collapse choice. Graph coordinates and scale are unaffected by table spacing or UI text size.

Dependency opens at 100% zoom. Fit remains available to show the entire map. Compact 170 × 42 cards, at most two side by side per layer column (busy half-hours grow taller), keep the timetable narrow for a desktop monitoring view. All half-hour slots remain in the daily model; empty rows compress, and empty half-hour labels are omitted visually. CSS size-container units fit both scene width and height to the available canvas, including fullscreen and sidebar changes. Selected DAG details overlay the map and close via a native radio control; they never change map dimensions, scale or positions. Numeric zoom and Reset allow closer reading with scrolling. SVG exports use the same compact coordinates and dimensions.

Planner is a read-only schedule check for one DAG, answering two questions: does it overlap its tagged (`upstream:<dag_id>`) dependencies today, and would it with a new schedule? The gear (Schedule) button opens the proposed start, duration basis and custom end in a popup, leaving the page to the timeline; the toolbar shows the values applied and −15m/+15m steps. The timeline runs left to right in Bangkok time with bars at true scale: every upstream level (farthest at the top), the selected DAG as Today and New schedule rows, then every downstream level, each labelled direct or N levels up/down. The green safe window runs from the last direct-upstream finish to the first direct-downstream start; any part of the selected bar outside it is red and is the overlap, and each selected row states ✓ No overlap or ✗ Overlaps N. DAGs involved in an overlap are red with a short reason. The “Only overlaps” tick box switches, without a reload, to a chart of just those DAGs plus the selected rows; it is kept when settings are applied and by the SVG export. A DAG still running at collection time is drawn striped up to the collection time; it is never treated as a completed duration, so links to it are reported as not checkable. Only the selected DAG moves; other start times stay fixed, and the duration buffer applies to every measured run. The collapsed “All dependency checks” table covers the whole dependency family, and the JSON export uses the same model.

Lineage initially combines matching `define_glue_job` → `trigger_glue_job` siblings into one job box. Click to expand both tasks; click the group header to collapse. Groups show the children's combined run status. Deep links to a child open its group automatically. Each group reserves its expanded footprint, keeping every other node, the canvas bounds, Fit scale, and scroll position stable. Click the selected task again to deselect it. The corner icon expands Lineage across the browser window and restores it on the next click. Grouping changes only the view; saved task records and dependencies keep their original identities.

Direct and All levels control both graph highlighting and the selected task’s Upstream/Downstream lists. Direct shows immediate neighbours; All levels follows every upstream/downstream task in the same DAG, excluding unrelated branches. The lists and task counts switch with native CSS controls; the graph is not reloaded. Long lists scroll within the detail panel.

## Local storage

Each environment has a separate root, and each Airflow source a `source_id=<SHA-256 of canonical base URL>` folder beneath it with the table/month/date layout. `snapshots/<date>.json` records each saved date (source name, URL, ID, coverage, warnings, collection time; no credentials); `metadata.json` is the newest one. Copied exports stay in `data/<stage>/dag_lineage/`.

```text
data/uat/source_id=<id>/
  airflow_dag_runs/prti_dttm=202610/2026-10-29.parquet
  airflow_task_runs/prti_dttm=202610/2026-10-29.parquet
  airflow_dag_dependencies/prti_dttm=202610/2026-10-29.parquet
  airflow_dag_inventory/prti_dttm=202610/2026-10-29.parquet
  airflow_task_definitions/prti_dttm=202610/2026-10-29.parquet
  airflow_monitoring_logs/prti_dttm=202610/2026-10-29.parquet
  airflow_system_health/prti_dttm=202610/2026-10-29.parquet
  airflow_import_errors/prti_dttm=202610/2026-10-29.parquet
  airflow_task_diagnostics/prti_dttm=202610/2026-10-29.parquet
  airflow_dag_links/prti_dttm=202610/2026-10-29.parquet
  snapshots/2026-10-29.json
  metadata.json
```

- One Parquet file per dataset per Bangkok date, with a `YYYYMM` partition folder. No filename hashes.
- DAG and task files merge newly collected records with that day's saved records. Identity: `stage, dag_id, run_id`; tasks additionally use `task_id, map_index`. A newer attempt updates the task rather than adding another task identity.
- Current definitions, inventory, and dependency files replace their complete collection date. Older snapshot dates remain untouched. Every export or collection uses one Bangkok date, captured when it starts.
- The monitoring model is rebuilt from that day’s saved raw files. The workspace reads and exports only the opened date.
- Each file is written to a temporary file, then atomically replaced. Multiple file replacements are not one database transaction. An interrupted multi-file write leaves a marker and displays a cache warning until a subsequent collection completes. Only one collection or import per source runs at a time.
- Raw timestamps are UTC. Monitoring `*_bkk` columns use Bangkok wall time, matching the ETL `TIMESTAMP_NTZ` contract. Model UTC audit fields are timezone-free UTC wall times.
- `metadata.json` records source URL, coverage, window, warnings, and collection time. **Tokens are never stored in Parquet, metadata, exports, URLs, or logs.**

## API collection and coverage

Collection uses read-only requests:

```text
GET /api/v2/dags
GET /api/v2/dags/{dag_id}/details
GET /api/v2/dags/{dag_id}/tasks
GET /api/v2/dags/~/dagRuns
GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances
```

Connect/Refresh also collects optional `/monitor/health` and `/importErrors` through `/api/v2`. Only task instances whose current collected state is `failed` receive extra attempt-history and log requests. Successful, running, waiting, skipped and upstream-failed instances make no diagnostic requests; their execution metadata and all graph dependencies remain collected. Every failed instance's known attempts are included, with no 100-task cap or saved-text excerpt limit. Logs use `full_content=true` and follow continuation tokens until completion or a non-advancing/empty cursor. Incomplete and unavailable logs remain explicit in coverage, with `scope=failed_tasks`. Each mapped task uses its own map index. The access token is redacted from saved log text. Attempt history is scoped to selected runs, including their attempts across midnight; active import errors are snapshots of current state and may reference earlier timestamps.

Lineage displays a saved-log pane only when the selected task has a failed instance in the selected run. Its native HTML controls select failed map indexes and their attempts, page through 200 lines at a time, and download the entire saved attempt as text. Successful instances' logs are hidden even if an older snapshot contains them. The pane navigates independently, keeping the graph's selection, expansion and scroll position intact. It loads lazily from local Parquet rather than embedding every task's log in the main page. New diagnostic record keys allow task-scoped Parquet reads; older numeric keys and excerpt logs remain readable. Normal workspace rendering skips loading full diagnostic payloads. The nine physical schemas and the 85-column monitoring model are unchanged; failed-task logs live in the diagnostic table's `raw_json`. Fresh collection is required to populate logs absent from an older snapshot.

Optional API permission, version, and resource failures are saved as resource coverage statuses and do not discard the core DAG/task snapshot. The three optional datasets preserve API records as JSON strings within daily Parquet files, with stage, snapshot date, record key and collection time. Their complete snapshot partitions replace on refresh. They remain separate from the unchanged 85-column monitoring model. Old caches open normally and show “Not collected” until Connect/Refresh gathers these resources. Opening any tab, selecting a task, and downloading data read local files only.

DAG, run, and task-instance lists are paginated, with count and repeated-record checks. Task definitions use the non-paginated task-list endpoint and validate `total_entries`. Requests use four workers, TLS verification, timeouts, limited retries, and no redirects carrying the bearer token. A 401/403 stops collection. Stale DAGs are listed as an informational notice; current task-definition collection is skipped for them. Missing definitions for current DAGs remain collection warnings.

Each snapshot covers **one Bangkok date**, from 00:00 inclusive to the next midnight exclusive. Choose the source and date with the top-bar picker; there are no baseline controls. Query-string or submitted legacy dates cannot widen collection, rendering, run selection, or exports. DAG/task definitions and dependency snapshots use the same captured date, including when collection crosses midnight.

Every view is limited to the snapshot day. Dependency (formerly Timeline) follows the legacy timetable: 00:00–24:00 down the left in half-hour rows, with DAG cards across data-layer columns. Each DAG shows its latest observed run today, positioned by actual start time or queued time when it has not started. The detail panel retains all its runs today and links each to the correct run in Lineage. Click a card to trace upstream/downstream DAGs; click it again to deselect. All levels/Direct and the full-window icon use native HTML controls and CSS. Selection keeps the scene coordinates and scroll container fixed. Zoom, Fit, Reset, and SVG download submit the current selected DAG, dependency depth, and full-window setting using an HTML form. Fit responds to the canvas width and height; SVG download produces a standalone vector timetable with the selected relationships and source metadata. Headers and the time column remain pinned while scrolling. Seven-day comparisons are removed; their nullable columns remain reserved in the 85-column schema. Running executions have no completed duration. Switching source or date reads saved files only, without credentials, Airflow requests, or Parquet writes. Refresh runs Import data again and opens the newest date of the same source.

Dependency's **Find DAG** searches DAG IDs and display names, selects a unique match, and navigates to its card using a stable fragment anchor. It keeps the full map, all coordinates, and available dependency arrows; it clears state/tag restrictions for the jump. Multiple matches become choices above the unchanged map, and DAGs without a run today are labelled explicitly. State/tag filters remain separate controls. Native form submission preserves the currently selected depth, zoom and full-window state. A temporary jump nonce ensures repeated finds reset the selected radio button; it is excluded from subsequent links/forms. Other pages retain their normal search filtering.

Dependency columns use only Airflow layer tags from the saved inventory. Explicit `layer:` tags take priority over recognized plain tags such as `persist`, `curate`, `consumption`, and `outbound`; canonical names and aliases are normalized. Missing, unknown or conflicting layer tags go under Other. Project/team/stakeholder tags and DAG names never imply a layer. There is no Excel fallback. Card frequency comes from Airflow tags or the observed run type. Grouping works immediately with existing saved snapshots.

Dependency links come only from `upstream:<dag_id>` tags in the saved Airflow DAG inventory. Multiple tags and comma-separated values are supported; links are deduplicated and reversed to derive downstream. The graph uses the full collected inventory so filters and missing runs do not erase relationships; arrows still require visible cards at both ends. Missing, empty, invalid, self-referencing and `unknown` values never become nodes. Coverage distinguishes missing/unknown/partial tags from an explicit `upstream:none`. An old snapshot with no upstream tags shows a refresh notice instead of loading Excel links. `layer:unknown` groups under Other. Refresh after syncing DAG tags to collect the new metadata; viewing the map makes no API calls or Parquet writes. Existing Timeline bookmarks open Dependency, and older task-timetable links remain supported.

Only resources visible to the supplied token can be collected. Lineage shows current **task dependencies within each DAG**, overlaid with the selected run's task states. Dependency separately shows DAG relationships declared by saved Airflow tags; these metadata declarations do not prove scheduling enforcement or run-to-run pairing. Arrows are drawn only between DAGs with observed runs today; related DAGs without a run, outside the snapshot, or hidden by filters are explicitly listed in the detail panel. No relationships are inferred from timing or names. Neither view reconstructs historical DAG versions or asset/table lineage. Historical task instances absent from the current definition remain visible as unconnected nodes.

Transient network failures during read-only Airflow requests retry up to three attempts with cancellable backoff. DNS, timeout, refused/interrupted connection and TLS failures produce distinct messages without exposing raw exception text or credentials. TLS certificate verification failures stop immediately; verification remains enabled. HTTP 401 retains its separate expired/invalid-token behavior, while network failures preserve the session token.

## Schema and future S3

`lineage_app/config/monitoring-schema.json` contains the exact 85-column `mockup_db.airflow_monitoring_logs` target recovered from Notification Project, plus the dependency schema and join identities. `lineage_app/data/model.py` maps raw API records into it. `lineage_app/data/parquet.py` owns all file access, so an S3-backed store can later supply the same view inputs.

The additional inventory and definition snapshots preserve DAGs without runs, isolated tasks, owners, schedules, and current dependency definitions. They are separate from the 85-column model.

The API-only source cannot supply callback `job_logs` details such as Glue execution IDs, worker metrics, target table names, or errors stored by `logs_builder`. Those fields remain null, and `has_job_log`/`has_glue_run` are false. Future job-log enrichment should LEFT JOIN on `stage, dag_id, run_id, task_id, map_index, try_number` without multiplying task rows.

## Views

- Overview is the landing page for Airflow and local data. It lists all DAGs whose latest collected run today failed, with started/ended times, duration, run type, failed/total task counts for that run, links to each failed Airflow run, a final arrow to the selected run’s lineage, and a scrollable table. DAGs whose latest run recovered are excluded.
- Lineage explorer: task graph, selected run, direct/full dependency tracing, click-again deselection, zoom, SVG download, mapped instances, and monitoring-field inspection.
- Dependency: latest run per DAG today, tag-declared DAG links, upstream/downstream tracing, and full-window mode.
- DAG inventory: all collected DAGs, full tags/ownership, pause/stale/import health, task/edge counts, today's run counts and success rate, latest-run failure counts, exact run ID and timestamps, interval dates, scheduler limits, source details, and pinned Airflow DAG/run links. The DAG identity and navigation remain fixed while scrolling; native Columns checkboxes hide fields and Show all restores them. Timestamps are displayed in Bangkok; each schedule retains its configured timezone. Unavailable definitions/counts remain unknown rather than zero.
- Performance: longest completed DAG/task executions, starts by Bangkok hour, median runtime and queue wait, and the 50 longest saved queue waits. The two runtime rankings cross-filter on this page: selecting a DAG run shows its longest tasks; selecting a task shows its exact parent run on the left. Task identity includes map index. Clicking a selection again or Clear removes the selection; task deselection retains an existing DAG-run filter. Ranking clicks stay on Performance and return to the ranking section, using saved data only. A running parent can appear for a completed task with runtime shown as unavailable. Summary metrics, the hour chart and queue table keep the current page's search/tag scope. Task measurements use the latest saved attempt, exclude upstream-failed/skipped tasks from queue timing, and omit negative or missing waits. There are no historical baselines or CPU measurements.
- Troubleshooting: failed, upstream-failed, retrying and waiting task instances. Select a task to inspect the exact run/map identity, upstream failed tasks, downstream upstream-failed tasks, saved attempts, logs and scheduling blockers. Graph relationships use current definitions within the same run and do not establish a sole cause of failure. Diagnostic coverage and per-resource failures remain visible.
- Health: saved database/scheduler/triggerer/DAG-processor status and heartbeats, active import errors with expandable tracebacks, and snapshot coverage. Unknown data never displays as healthy or as zero errors.
- Raws: all ten table schemas (including Cross-DAG links), searchable row previews with paging, and original saved data. Monitoring logs also show the 85-column model types. Download a complete table as its original Parquet file, CSV, or JSON, or download its schema as JSON. Download all packages the snapshot’s original Parquet files, all schemas, metadata, and a manifest into one ZIP. Optional collection status is included in schemas/manifests. Preview filters never limit downloads. Raw values, JSON strings, and timestamp timezones are preserved; older partitions are excluded. Browsing and downloading make no Airflow requests or Parquet writes. Old Insights and Schema bookmarks open Raws.

Refresh reuses the Airflow token in the current Python session. When viewing local data, Refresh opens a token popup over the current page; submitting a token collects today's Bangkok snapshot and returns to the same view. An HTTP 401 at any collection endpoint clears the rejected token and asks for a replacement, while retaining the saved snapshot. Permission and network failures keep their own error messages. The sidebar has one **Change connection** button: it clears the token, ends the current session, and returns to setup. Saved Parquet and AI chat history remain available when the saved environment is reopened. Tokens leave application memory after 24 hours of inactivity or process exit; actual token validity is determined by Airflow. Do not expose this local app as a multiuser service.

## AI chat

Open **AI** in the sidebar after loading a snapshot. Add the backend configuration to `.env` and restart the server:

```dotenv
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=gpt-5-mini
```

The environment variable overrides `.env`. The key stays on the Python backend; `.env` is ignored by version control and is not served to the browser.

The UI has conversation history on the left, chat on the right, a pinned composer, and expandable evidence under answers. Conversation title, snapshot date, Context and Export chat sit in the page-heading row alongside AI and Updated, keeping the conversation panel clear. Context shows the environment, Bangkok snapshot date, collection time, and available tables. Suggested questions help start a conversation. Chats can be exported as JSON with their answers, queries, evidence and model usage.

The compact composer is one rounded text field with an arrow submit button. Enter submits the same native HTML form; there is no JavaScript keyboard handler. Its reduced height leaves more space for the conversation.

Conversation history is stored separately from Parquet in `data/ai/conversations.sqlite3`. An HttpOnly browser cookie identifies its owner, so history survives server restarts in the same browser. Each conversation is pinned to its exact environment and collection. After refresh, earlier conversations remain readable but cannot query overwritten data; start a new chat for the new snapshot. Chat history does not widen the app's one-day execution-data scope.

For follow-ups, Python sends the latest 12 completed messages plus compact notes of older questions. A history-search tool retrieves older messages on demand. Prior answers are context, not a source of fresh facts. The model receives a small table manifest first, then requests relevant schemas, read-only SQLite queries, or task/DAG traces. All nine saved tables are available, plus an in-memory `declared_dag_links` table derived solely from saved Airflow upstream tags, with downstream inferred by reversing them. No Airflow requests or Parquet writes occur during chat. SQL runs only against the loaded snapshot, with allowed tables/functions, time limits and bounded results. Evidence records the actual query, returned rows, truncation and snapshot identity. Missing coverage remains unknown; DAG link evidence identifies Airflow upstream tags as its source and includes missing/unknown tag coverage.

The OpenAI Responses API uses `store=False` and application-managed history. Relevant conversation context and retrieved data are sent to OpenAI; this setting does not imply zero provider retention. Only final text, evidence and usage are saved locally, not reasoning output. Strict function schemas constrain tool arguments. Duplicate submissions are deduplicated, and page navigation never repeats model requests. Failed answers show a recoverable status. Python background work plus a held HTTP request delivers completed answers without JavaScript.

The exact column-name index is supplied upfront, avoiding separate schema-discovery calls. Independent read-only tool calls can share one OpenAI response, and list queries can include window counts for totals. Answers default to compact tables. Completed answers show elapsed time; hover to see model-request and local-load timings. Structured events in the chat database's `ai_events` table record model request durations, request IDs, token counts and tool successes/errors, including failed answers. Events exclude prompts, SQL, raw records and credentials. The message `usage` JSON records total/API/tool/load timing, reasoning tokens and cached input tokens. Earlier answers created before timing instrumentation have no measured completion duration.

While an answer is pending, its page immediately navigates once to `/ai/wait`. Python holds that request on a condition variable. Saving the completed answer or error wakes it immediately, then redirects to the finished chat with the latest answer in view. There is no two-second polling interval or repeated model request. A pending request reconnects after 55 seconds if the model is still working. The temporary result query value guarantees a fresh document instead of an anchor-only navigation and is excluded from navigation/forms. Browsing away leaves the background answer running; reopening the chat reads its saved result. This delivers the completed answer, not token-by-token streaming.

See the [OpenAI function-calling guide](https://developers.openai.com/api/docs/guides/function-calling) and [conversation-state guide](https://developers.openai.com/api/docs/guides/conversation-state).

## Validation

```sh
.venv/bin/python -B -m unittest discover -s tests -v
```

HTTP integration tests start temporary loopback servers and a fake Airflow API; environments that restrict socket binding need permission to run those tests. No live Airflow credentials are required. Live authentication and production data coverage still need to be checked with a user-provided token.

Visual reference: [CLICX Bank](https://clicxbank.com/th/personal). The local theme uses turquoise `#00DDD1`, yellow `#DFFF00`, charcoal `#252A36`, and white. The wordmark is a local copy of the public site SVG; the interface uses system fonts.
