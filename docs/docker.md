# Docker setup and validation

The image `clicx-lineage:local` has been built and tested on Linux ARM64 with Python 3.13. The test container runs at <http://localhost:8766>.

- `Dockerfile`: Python 3.13, a non-root application user, and a health check.
- `compose.yaml`: localhost access on `${LINEAGE_PORT:-8765}`, optional runtime `.env`, and a persistent data volume.
- `.dockerignore`: excludes credentials, local data, the virtual environment, caches, tests, and archived files.

The container listens on `0.0.0.0` internally. Compose publishes it only on the host's `127.0.0.1`. Normal local Python startup still binds to `127.0.0.1`.

## Build and run

Requires Docker with Compose 2.24 or newer:

```sh
docker compose build
docker compose up -d
```

Open <http://127.0.0.1:8765>. Stop the local Python server before using the same host port.

To run beside the local Python app:

```sh
LINEAGE_PORT=8766 docker compose -p clicx-lineage-test up -d --no-build
docker compose -p clicx-lineage-test ps
```

Open <http://localhost:8766>. Click Import data to open the copied `dag_lineage` folders (see [DAG lineage export](dag-lineage-export.md#docker)). The app accepts localhost requests on the published port even when it differs from the container's internal port.

To stop the test container while retaining its data:

```sh
docker compose -p clicx-lineage-test down
```

The default `lineage-data` named volume stores Parquet snapshots and the AI chat database at `/app/data`. It starts independently of the existing host `data/` folder. Reusing the host snapshots/chat database needs an explicit data migration or a bind-mount configuration with suitable write permissions during deployment. Local data is excluded from the image.

The current test project uses `clicx-lineage-test_lineage-data`. A copy of host `data/uat/` was placed there for validation; no host directory is mounted into the running application. Its AI conversations are separate from the host's chat database.

OpenAI configuration is supplied at runtime from the optional root `.env`. The image contains no API key. The Airflow access token is still entered in the UI and held in application memory.

## Checks performed

- 109 regression tests cover tag-only layer grouping, Performance cross-filtering, Dependency find/jump behavior, failed-task-only logs, saved-log paging/downloads, token reuse, local refresh prompts, and authentication recovery. The container test run has external networking disabled and uses fake Airflow/OpenAI clients.
- All nine workspace tabs, SVG exports and raw-table/ZIP downloads passed against the running container.
- A live OpenAI question used the runtime key, returned the correct raw-data count with evidence, and completed through the held-request UI flow.
- The container runs as UID 10001 and can write to its own data volume. Its Docker health check reports healthy.
- Parquet snapshots and AI chat messages survived a test-container restart; all nine pages passed again afterward.
- The built image contains no `.env`, OpenAI key, or host snapshots. Environment files are not served over HTTP.

Repeat the HTTP checks against a running container with today's saved data:

```sh
.venv/bin/python -B scripts/smoke_container.py --url http://localhost:8766
```

Add `--ask-ai` to send one small live OpenAI question using the container's runtime key. Live Airflow collection still requires entering a valid Airflow token in the UI; automated collector tests use a fake API.
