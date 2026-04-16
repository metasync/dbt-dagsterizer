# Package development

This document is for contributors working on `dbt-dagsterizer` in this repository.

## Setup

Create a local environment with dev dependencies:

```bash
uv sync --dev
```

## Run tests

```bash
uv run pytest
```

## Lint

```bash
uv run ruff check .
```

## Build artifacts

Wheel and sdist:

```bash
uv run hatch build
```

## Template development

The embedded cookiecutter template lives under `src/dbt_dagsterizer/project_templates/`.

To render a local throwaway project for validation:

```bash
uv run dbt-dagsterizer project init --help
```

Then follow the rendered project’s `README.md`.
