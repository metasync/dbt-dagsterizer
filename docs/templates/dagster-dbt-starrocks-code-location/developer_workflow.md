# Developer workflow (Dagster + dbt + StarRocks)

This template is designed so most orchestration configuration is declared in dbt and discovered via `dbt_project/target/manifest.json`.

## Philosophy (dbt-first, Dagster-orchestrated)

This template follows a division of responsibilities that scales well for warehouse-style transformations:

- dbt is the transformation layer: SQL models, tests, sources, and selection primitives (tags/groups) live in dbt.
- Dagster is the orchestration and observability layer: schedules, sensors, retries, backfills, run history, and dependency-aware execution live in Dagster.

In practice, this means:

- You declare orchestration intent close to the models (dbt `meta` and tags), not in ad-hoc Python lists.
- Dagster reads the dbt manifest and turns that intent into jobs/schedules/sensors.
- Python config exists as an escape hatch for cases where dbt metadata is not expressive enough.

### Why declare orchestration intent in dbt meta?

- Single source of truth for orchestration intent (`dbt_project/dagsterization.yml`).
- Reviewability: changes to orchestration intent are part of dbt model reviews.
- Portability: the manifest is a stable interface between dbt and Dagster.

### What stays out of dbt meta

Keep model execution details in dbt config (materialization strategy, incremental keys), and keep runtime infrastructure in GitOps (resources, env vars, secrets).

## Source of truth

- dbt source metadata (observation): `meta.luban.observe` in `sources.yml`
- dbt model metadata (dbt-native): `schema.yml` in `dbt_project/models/**`
- Orchestration intent: `dbt_project/dagsterization.yml` (written/updated by `dbt-dagsterizer`)

## Quick start

1. Ensure `dbt_project/target/manifest.json` exists.
   - Local: keep `LUBAN_DBT_PREPARE_ON_LOAD=1` (default).
   - CI or explicit: run `dbt deps` and `dbt parse`.
   - Or: use `dbt-dagsterizer meta init --parse` to create the orchestration file and refresh the manifest.
2. Start the code location and verify `Definitions` load.

## Editing orchestration intent with the CLI

Instead of writing orchestration intent in model SQL, the recommended workflow is:

1. Run `dbt-dagsterizer meta init`.
2. Use `dbt-dagsterizer meta job|schedule|partition|asset-job|partition-change ...` to update `dbt_project/dagsterization.yml`.
3. Use `--parse` (or run `dbt parse`) so Dagster picks up the changes.

CLI reference: `../../concepts/cli.md`.

## Creating jobs

### One model per asset job (recommended for `dwd`)

Enable a per-model asset job in `dagsterization.yml`.

```yaml
asset_jobs:
  - orders

partitions:
  daily:
    - orders
```

CLI equivalent:

```bash
dbt-dagsterizer meta asset-job --models orders
dbt-dagsterizer meta partition --models orders --type daily
```

The generated job name is `dbt_<model>_asset_job`.

### Grouped job (recommended for `dws`)

Define a grouped job in `dagsterization.yml`.

```yaml
jobs:
  daily_facts_job:
    models:
      - fact_orders_daily
      - fact_customer_orders_daily
    include_upstream: false
    partitions: daily
```

CLI equivalent:

```bash
dbt-dagsterizer meta job \
  --models fact_orders_daily,fact_customer_orders_daily \
  --name daily_facts_job \
  --partitions daily
```

All models listed in the same job are grouped into the same asset job.

Defaults:

- `include_upstream`: `false`
- `partitions`: can be set per job (`daily|unpartitioned`)

## Creating schedules

Schedules are declared on the model that represents the job’s anchor.

Define a schedule in `dagsterization.yml`:

```yaml
schedules:
  orders_anchor_daily_schedule:
    type: daily_at
    job_name: dbt_orders_asset_job
    hour: 1
    minute: 0
    lookback_days: 0
    enabled: true
```

CLI equivalent:

```bash
dbt-dagsterizer meta schedule \
  --models orders \
  --name orders_anchor_daily_schedule \
  --hour 1 \
  --minute 0 \
  --lookback-days 0 \
  --enabled
```

## Partition-change sensors (late arrivals)

### Detector

Declare a partition-change detector entry in `dagsterization.yml`.

```yaml
partition_change:
  detectors:
    - model: orders
      enabled: true
      lookback_days: 7
      offset_days: 1
      detect_source:
        source: ods
        table: orders
      partition_date_expr: order_datetime
      updated_at_expr: updated_at
```

CLI equivalent:

```bash
dbt-dagsterizer meta partition-change detector \
  --model orders \
  --enabled \
  --detect-source ods.orders \
  --partition-date-expr order_datetime \
  --updated-at-expr updated_at \
  --lookback-days 7 \
  --offset-days 1
```

Defaults:

- `job_name`: derived from the model’s job config (same rule as schedules)
- `minimum_interval_seconds`: `60`
- `name`: `<model>_partition_change_sensor`

### Propagation

Declare propagation on the upstream model.

Declare propagation in `dbt_project/dagsterization.yml`:

```yaml
partition_change:
  propagators:
    - upstream_model: orders
      enabled: true
      name: facts_from_orders_partitions_sensor
      minimum_interval_seconds: 30
      targets:
        - job_name: daily_facts_job
```

CLI equivalent:

```bash
dbt-dagsterizer meta partition-change propagator \
  --model orders \
  --enabled \
  --name facts_from_orders_partitions_sensor \
  --minimum-interval-seconds 30 \
  --targets daily_facts_job
```

Notes:

- `enabled: false` still registers the sensor definition but defaults it to STOPPED.
- The template enables propagation by default on the `orders` model; set `enabled: false` if you want to onboard gradually.
- `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE=eager` disables propagation sensors entirely.
- Propagation sensors only react to new upstream materializations by default. To replay recent upstream partitions after enabling a sensor, set `LUBAN_PARTITION_CHANGE_PROPAGATOR_CATCHUP_DAYS` (for example `7`) before the first time the sensor runs (or after resetting its cursor).

## Source observation (DataVersion)

Define a watermark column for the dbt source table:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_column: ods_updated_at
```

This drives observable source assets and the observation job/schedule.

## Optional overrides (escape hatch)

Rendered projects depend on the `dbt-dagsterizer` Python package. The recommended customization path is via `dagsterization.yml`.

If you need a custom escape hatch, the intended long-term approach is to expose explicit override hooks in `dbt_dagsterizer`. Until then, treat library forks as temporary.

## Troubleshooting

- Missing manifest: ensure `LUBAN_DBT_PREPARE_ON_LOAD=1` or run `dbt deps` + `dbt parse`.
- Duplicate names: job and schedule names must be unique after grouping.
- dbt fails with "already exists": Dagster retries the dbt invocation once (after a short delay); if it persists, treat it as a real failure.
- Partition-change sensor finds no partitions: verify `updated_at_expr` and StarRocks connectivity.
