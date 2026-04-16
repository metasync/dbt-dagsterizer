# dbt-dagsterizer

`dbt-dagsterizer` is the home for reusable Dagster automation driven by dbt metadata.

## Goals

- Keep dbt projects and Dagster code locations decoupled (BYO dbt project).
- Generate Dagster assets/jobs/schedules/sensors/resources from dbt `manifest.json` plus a small orchestration intent file.
- Provide a CLI to author and validate orchestration intent with a low learning curve.

## Core concepts

### Manifest-driven

The dbt manifest (`dbt_project/target/manifest.json`) is the stable interface between dbt and Dagster.

### Orchestration intent

Orchestration intent is stored in a single, reviewable YAML file:

- `dbt_project/dagsterization.yml`

This file is not a dbt schema YAML and is intentionally kept outside `dbt_project/models/` so dbt will not parse it.

### Always-loadable definitions

Code locations often need to import successfully even before the dbt project is “real”.

`dbt_dagsterizer.api.build_definitions()` returns a minimal Dagster `Definitions` when there are no dbt models yet, so skeleton repos remain runnable.

## See also

- CLI reference: `cli.md`
- Code location template concept: `dagster-dbt-code-location-template.md`
