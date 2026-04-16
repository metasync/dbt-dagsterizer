# Codebase tour

This is a high-level guide to the `dbt-dagsterizer` codebase: what’s public, what’s internal, and where to look when you need to change behavior.

## Public entry points

- Python API: `dbt_dagsterizer.api.build_definitions()`
- CLI: `dbt_dagsterizer.cli:cli` (exposed as the `dbt-dagsterizer` console script)
- Templates: `src/dbt_dagsterizer/project_templates/` and `src/dbt_dagsterizer/macro_templates/`

## Core idea

At runtime (Dagster import time), the code location calls `build_definitions()`.

- If the dbt project is empty (no `models/**/*.sql`), it returns a minimal `Definitions` that still loads.
- Otherwise it assembles assets, jobs, schedules, sensors, and resources by reading the dbt manifest and the orchestration intent file (`dagsterization.yml`).

## Package layout

- `api.py`: stable API surface for code locations
- `cli.py` + `cli_parts/`: Click-based CLI grouped by domain (`meta`, `macros`, `project`)
- `dbt/`: manifest reading and “prepare” logic (refreshing `manifest.json` when requested)
- `assets/`: Dagster asset factories (dbt assets and observable source assets)
- `jobs/`, `schedules/`, `sensors/`: generated orchestration constructs
- `resources/`: external resources (for example warehouse connectivity)
- `project_templates/`: embedded cookiecutter template for a runnable code location repo
- `macro_templates/`: dbt macros that the CLI can install into a dbt project

## When you need to change X

- Add a new CLI command: start in `src/dbt_dagsterizer/cli_parts/`
- Change the orchestration schema: look for YAML read/write in `cli_parts/meta.py` and validation in `cli_parts/validation.py`
- Change Dagster `Definitions` composition: `src/dbt_dagsterizer/api.py`
- Change dbt manifest refresh rules: `src/dbt_dagsterizer/dbt/`
