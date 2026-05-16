# Dagster + dbt + StarRocks code location template

This documentation describes the source template embedded in `dbt-dagsterizer`.

- Template source: `dbt_dagsterizer/project_templates/`
- Rendered output: a Dagster code location repo containing `dbt_project/` and `src/<package_name>/`

Notes:

- Examples may contain cookiecutter variables (for example `{{cookiecutter.package_name}}`). In rendered projects, those are replaced with real values.
- Rendered projects do not ship template documentation; their `README.md` links back to this directory.
- Rendered projects include basic OpenTelemetry bootstrap; export is controlled by `OTEL_*` environment variables.

## GitOps (Luban CI)

In Luban CI, Dagster code location GitOps templates typically inject OTEL settings via a `dagster-observability` ConfigMap. Export is disabled unless you set:

- `OTEL_TRACES_EXPORTER=otlp` (and optionally `OTEL_METRICS_EXPORTER=otlp`)

## Template options

- `include_sample_dbt_project` (default: `false`): when `true`, the template includes sample dbt models (useful for troubleshooting and demos).
- `include_docker` (default: `false`): when `true`, the template includes a local StarRocks `docker-compose.yml` file and docker-related `make` targets.

## dbt-dagsterizer

Rendered projects depend on the published `dbt-dagsterizer` Python package and use it to build Dagster `Definitions`.

The rendered `pyproject.toml` pins `dbt-dagsterizer` by default to the installed CLI version (when available) so generated code locations align with the generator. Override with `dbt-dagsterizer project init --dbt-dagsterizer-version ...` or leave it unpinned with `--no-pin-dbt-dagsterizer` (mutually exclusive).

Use the CLI to write orchestration intent into `dbt_project/dagsterization.yml` and refresh the dbt manifest:

```bash
dbt-dagsterizer meta init --parse
dbt-dagsterizer meta validate --prepare
```

CLI reference: `../../concepts/cli.md`.

## Guides

- Usage and architecture: `template_usage.md`
- Developer workflow (jobs/schedules/sensors via `dagsterization.yml`): `developer_workflow.md`
- Local development (render + run): `local_development.md`
