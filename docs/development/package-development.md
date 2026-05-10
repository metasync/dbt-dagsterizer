# Package development

This document is for contributors working on `dbt-dagsterizer` in this repository, and for local workflows that require running with a working-tree version of the package.

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

## Render and run with your working tree

This section is for iterating on dbt-dagsterizer and validating changes in a rendered code location (for example, OTEL/Elastic integration).

### Render with your working tree

From the dbt-dagsterizer repo root:

```bash
cd /Users/chi/Workspace/projects/luban/dbt-dagsterizer
```

Run the CLI via `uv` so it uses the package from this repo:

```bash
uv run dbt-dagsterizer project init \
  --output-dir /tmp \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --include-sample-dbt-project \
  --include-docker
```

If your environment is not picking up the local checkout, install the current repo in editable mode and retry:

```bash
uv pip install -e .
uv run dbt-dagsterizer project init ...
```

Sanity check: the rendered project should include your latest template changes (for example, recent `.env.example` edits).

### Run a rendered project with your working tree

After rendering, update the rendered project so it depends on your local checkout of dbt-dagsterizer.

#### Option A: Use a file URL dependency (recommended)

In the rendered project `pyproject.toml`, set:

```toml
dependencies = [
  # ...
  "dbt-dagsterizer @ file:///Users/chi/Workspace/projects/luban/dbt-dagsterizer",
]
```

Then re-sync:

```bash
uv sync --reinstall-package dbt-dagsterizer
```

Now `make dev` runs with your local dbt-dagsterizer code.

#### Option B: Install editable from the rendered project

From inside the rendered project directory:

```bash
uv pip install -e /Users/chi/Workspace/projects/luban/dbt-dagsterizer
```

Then run:

```bash
make dev
```

### Notes

- If you change dbt-dagsterizer source code, the rendered project sees updates immediately when using a file URL or editable install.
- If you change template files in dbt-dagsterizer, you must re-render the project to see template changes.
