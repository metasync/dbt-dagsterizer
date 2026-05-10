# Local development (template and rendered project)

This guide is for developing the source template and verifying the rendered project locally.

## Render the template

Use the embedded template renderer:

```bash
dbt-dagsterizer project init \
  --output-dir /tmp \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --include-docker \
  --include-sample-dbt-project \
  --author-name "You" \
  --author-email "you@example.com"
```

## Setup and run the rendered project

```bash
cd /tmp/orders_analytics
make setup
```

`make setup` installs Python dependencies and installs the dbt macros required by this code location.

## Start StarRocks (Docker Compose)

```bash
make docker-up
```

This step requires rendering with `--include-docker`.

Wait until the `starrocks-init` container finishes. It registers the BE into FE.

StarRocks ports exposed locally:

- FE HTTP: `8030`
- FE MySQL: `9030`
- BE HTTP: `8040`

## Configure environment

`make setup` creates `.env` if missing. Update it with your StarRocks connection values.

For local StarRocks via Docker Compose, these defaults typically work:

- `STARROCKS_HOST=localhost`
- `STARROCKS_PORT=9030`
- `STARROCKS_USER=root`
- `STARROCKS_PASSWORD=`

Validate connectivity:

```bash
make check-db
```

## Observability (OpenTelemetry)

The rendered code location initializes OpenTelemetry at process startup. Export is disabled unless you enable it via environment variables.

Common variables:

- `OTEL_TRACES_EXPORTER=otlp` (or `none`)
- `OTEL_METRICS_EXPORTER=otlp` (or `none`)
- `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` or `http/protobuf`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_EXPORTER_OTLP_HEADERS` (optional, for authentication), for example `Authorization=Bearer <token>`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`

Endpoint formatting depends on protocol:

- `grpc`: use `host:port` (no scheme), for example `localhost:4317`
- `http/protobuf`: use `http://host:port` or `https://host:port`, for example `http://localhost:4318`

### Test OTEL export with Elastic APM (local k8s)

This section is a step-by-step smoke test for verifying that dbt-dagsterizer emits OTEL traces to Elastic APM (and that Kibana can display them).

Assumptions:

- Elastic Stack (Elasticsearch + Kibana + Fleet + APM integration) is running in your local k8s cluster.
- StarRocks is reachable from your laptop (for example `starrocks.local:9030`).

#### 1) Configure OTEL env vars

In the rendered repo, set these values in `.env` (or export them in your shell before `make dev`):

```bash
OTEL_TRACES_EXPORTER=otlp
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_EXPORTER_OTLP_ENDPOINT=https://apm.luban.metasync.cc
OTEL_SERVICE_NAME=metasync/orders_analytics
OTEL_RESOURCE_ATTRIBUTES=service.namespace=metasync,deployment.environment.name=development
```

Notes:

- `OTEL_METRICS_EXPORTER` can be left as `none`. This project currently focuses on spans/traces; APM UI also provides service KPIs derived from traces.
- When using `http/protobuf`, the exporter sends data to OTLP HTTP paths (`/v1/traces`, `/v1/metrics`). dbt-dagsterizer auto-appends the correct path if you provide a base URL with no path.

#### 2) Optional: configure token auth (if your APM endpoint requires it)

If the APM endpoint enforces bearer token authentication, set:

```bash
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer <apm_secret_token>
```

If your token is stored as a k8s Secret, export it at runtime (avoid committing it into `.env`):

```bash
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $(
  kubectl -n elastic-stack get secret apm-server-apm-token \
    -o jsonpath='{.data.secret-token}' | base64 --decode
)"
```

Adjust Secret name / key if your cluster uses a different convention.

#### 3) Start the rendered project locally

```bash
make dev
```

Dagster Webserver should be available at `http://localhost:3000`.

#### 4) Generate a trace (dbt span)

In Dagster UI:

- Navigate to the asset graph
- Materialize `dbt/orders` (pick any partition if prompted)

Expected:

- The run succeeds
- Elastic receives spans including `dbt.cli` (and sometimes `dbt.manifest.prepare` during load/parse)

#### 5) Verify in Kibana

In Kibana:

- Go to Observability → APM → Services
- Time range: “Last 15 minutes”
- Find the service named `dagster/<your_app_name>`
- Open a trace and confirm spans:
  - `dbt.cli`
  - `dbt.manifest.prepare` (may or may not appear depending on whether manifest refresh happened)

## ODS demo data (optional)

If you plan to use the built-in ODS observation loop and automation, create the demo ODS tables before starting Dagster.

Bootstrap:

```bash
make ods-test-bootstrap
```

Append new rows (simulate incremental arrival):

```bash
make ods-test-append
```

## Run Dagster

```bash
make dev
```

Dagster Webserver should be available at `http://localhost:3000`.

## Partitioning notes

The template currently ships with daily partition support only.
