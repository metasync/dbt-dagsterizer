# Dagster + dbt (StarRocks) Code Location Template

For the end-to-end developer workflow (template + local validation), start with:

- `../templates/dagster-dbt-starrocks-code-location/README.md`

Dagster deployments commonly use two layers:

- **Dagster Platform**: Webserver + Daemon + Dagster instance storage.
- **Dagster Code Location**: User code served by `dagster code-server` (gRPC).

This document describes the standard **Dagster + dbt (StarRocks) code location** source template intended for data transformation teams.

## Goal

Provide a runnable, opinionated skeleton that:

- Keeps the Dagster platform and code locations decoupled.
- Uses **dbt** as the transformation engine (SQL/models/tests).
- Uses **Dagster** for orchestration, scheduling, observability, and dependency management.
- Standardizes folder structure and wiring so teams focus on datasets and models.

## When to use this template

Use this template when:

- You want a new Dagster code location that primarily materializes dbt assets.
- You want consistent conventions across teams (layout, naming, local run, CI expectations).

Do not use this template when:

- You need a Dagster platform.
- Your pipeline is mostly non-dbt (pure Python ops, Spark jobs, etc.).

## Boundary: Dagster vs dbt

### dbt owns

- Model definitions (SQL in `models/`).
- Data tests (schema tests, custom tests).
- Sources/exposures/macros/packages.
- Model selection semantics (`--select`, tags, state).

### Dagster owns

- Asset orchestration (when/what to run).
- Observability (asset-level logs, lineage view, run history).
- Scheduling, sensors, retries, alerting.
- Coordination with non-dbt assets (ingestion, external tables, ML training).

## Repository layout

The template is a single Git repo representing one code location.

- `src/<package_name>/` Dagster code location module
  - `definitions.py` exports `defs` (Dagster entrypoint) and delegates most wiring to `dbt_dagsterizer`
- `dbt_project/` dbt project
  - `dbt_project.yml`
  - `models/` (standard dbt structure)
  - `profiles.yml` (local-safe default)

## Runtime contract (Luban CI + Dagster)

Luban CI deploys code locations as a Kubernetes Deployment that runs:

`dagster code-server start -h 0.0.0.0 -p <port> -m <package_name>`

This implies:

- The Python module `<package_name>` must import successfully.
- The module must expose `defs` (via `__init__.py`).

## Local development

For the end-to-end local workflow (render template, start StarRocks, run Dagster), see:

- `../templates/dagster-dbt-starrocks-code-location/local_development.md`

## Production configuration guidance

- Keep secrets out of Git.
- Prefer configuring warehouse credentials via environment variables and/or mounted Secrets.
- For production adapters (for example Snowflake, BigQuery, Postgres, StarRocks), update `dbt_project/profiles.yml` to reference credentials via `env_var()`.

## Environments

This template supports configuring the default dbt environment via Cookiecutter (`default_env`).

Common mapping:

- `sandbox`: developer environment and Luban CI `snd` deployment
- `production`: Luban CI `prd` deployment

Set `DBT_TARGET` to one of the configured environments. In GitOps deployments, you typically keep env var names the same and just provide different values per environment.

## Layers as separate databases (StarRocks)

In many StarRocks deployments, `ods`, `dwd`, and `dws` are separate databases on the same cluster.

The template centralizes layer mapping in `dbt_project.yml` using env-var-driven dbt `vars`.

Convention: ODS source table `name` matches the physical table name. If you need a different physical name, set `identifier` in `dwd/sources.yml`.

## Recommended conventions

- One code location per domain.
- Shared dimensions as a producer.
- dbt-first transformations.
- Stable selection semantics (tags/groups) declared in dbt and consumed by Dagster.

For the supported orchestration config schema (`dbt_project/dagsterization.yml`) and the intended developer workflow, see:

- `../templates/dagster-dbt-starrocks-code-location/developer_workflow.md`

## Extending the skeleton

Common extensions that keep the boundary clean:

- Add ingestion assets in Dagster that produce dbt sources, then make dbt depend on them.
- Add partitioning in Dagster for incremental models, passing partition ranges into dbt via `--vars`.
- Add asset checks for dbt tests to show failures at the asset level.
