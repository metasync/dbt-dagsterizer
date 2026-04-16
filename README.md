# dbt-dagsterizer

`dbt-dagsterizer` is a Python package for building Dagster automation from dbt metadata.

It is designed to keep Dagster code locations mostly static, while letting developers declare orchestration intent in a small, reviewable YAML file alongside the dbt project.

## Two common ways to use it

1. As a CLI tool (bootstrap + maintain `dagsterization.yml`)

- Install once:

```bash
uv tool install dbt-dagsterizer
```

- Upgrade later:

```bash
uv tool upgrade dbt-dagsterizer
```

This is the recommended workflow for running `dbt-dagsterizer project ...`, `dbt-dagsterizer meta ...`, and `dbt-dagsterizer macros ...` from any repo.

2. As a Python dependency (Dagster runtime imports it)

Dagster code locations typically import `dbt_dagsterizer` at runtime (for example `build_definitions()`), so the Dagster project itself must depend on `dbt-dagsterizer` (for example in its own `pyproject.toml`). Installing the CLI as a tool does not automatically make it importable inside that project’s runtime environment/container.

## Documentation

- Docs index: [docs/README.md](docs/README.md)
- Getting started: [docs/getting-started.md](docs/getting-started.md)
- CLI reference: [docs/concepts/cli.md](docs/concepts/cli.md)
- Template docs (Dagster + dbt + StarRocks): [docs/templates/.../README.md](docs/templates/dagster-dbt-starrocks-code-location/README.md)

## Quick start

### CLI

```bash
dbt-dagsterizer --help
```

Initialize orchestration intent and refresh the dbt manifest:

```bash
dbt-dagsterizer meta init --parse
dbt-dagsterizer meta validate --prepare
```

### Python

```python
from dbt_dagsterizer.api import build_definitions

defs = build_definitions(dbt_project_dir="./dbt_project")
```

If the project has no dbt models yet (no `models/**/*.sql`), `build_definitions()` still returns a minimal, always-loadable `Definitions`.

## Development

This section is for developing `dbt-dagsterizer` itself. If you are using it in another repo, start with the CLI install instructions or add it as a dependency in your Dagster code location.

Setup:

```bash
uv sync --dev
```

Run tests:

```bash
uv run pytest
```

Lint:

```bash
uv run ruff check .
```
