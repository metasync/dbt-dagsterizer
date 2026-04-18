# Changelog

All notable changes to `dbt-dagsterizer` will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [0.1.13] - 2026-04-18

### Added

- Added a template fingerprint file (`dbt_project/.dbt_dagsterizer_template`) to allow `macros sync` to select the correct embedded template without CLI flags.

### Changed

- Managed dbt macros now live inside the embedded template under `dbt_project/macros/dbt_dagsterizer/` so freshly rendered projects include required macros by default.
- Replaced the `macros install` workflow with `macros sync`, which syncs the packaged managed macros into `macros/dbt_dagsterizer/` in a dbt project.

### Removed

- Removed the example `row_count_greater_than` test macro and its references from the sample dbt project.
- Removed the legacy `macro_templates/` directory; managed macros are now sourced from the embedded template.

## [0.1.12] - 2026-04-16

### Fixed

- Fixed Dagster runs without a partition key against partitioned dbt assets by providing a safe default daily dbt vars window instead of failing with missing `min_datetime`/`max_datetime`.

## [0.1.11] - 2026-04-16

### Fixed

- Fixed unpartitioned execution of partitioned dbt assets by handling Dagster partition context access safely.
- Improved template CLI behavior and docs:
  - Narrowed exception handling for project template helpers.
  - Clarified when `dbt deps` runs during manifest preparation.
- Disabled Dagster telemetry by default in the rendered template Dagster instance config.

## [0.1.10] - 2026-04-16

### Added

- Added CLI equivalents to template developer workflow docs to make it easy to manage `dagsterization.yml` via CLI or direct edits.
- Added tests for daily partitions env var enforcement and dbt retry decision logic.

### Changed

- Improved template runtime ergonomics:
  - Rendered projects now get an absolute `DAGSTER_HOME` written into `.env.example` at generation time.
  - `make setup` copies `.env.example` to `.env` before installing dependencies/macros.
- Hardened partition-change propagation sensor:
  - Resets invalid cursors safely instead of scanning history unexpectedly.
  - Emits stable upstream asset key tags.
- Aligned template schedule spec tests with supported schedule type(s).
- Clarified docs about when `dbt deps` runs during manifest preparation.
- Improved CLI error handling:
  - Narrowed Dagster version fallback to only “package not installed”.
  - `project list-templates` now surfaces errors instead of silently printing nothing.
- Refined docs for the two primary usage modes:
  - Install/use as a CLI tool (via `uv tool install/upgrade`).
  - Use as a runtime Python dependency inside a Dagster code location.
- Simplified doc examples by removing redundant `--dbt-project-dir dbt_project` where the default applies.

## [0.1.9] - 2026-04-16

### Added

- Centralized daily partition start-date handling into a shared helper.

### Changed

- Daily partitions now require `DAGSTER_DAILY_PARTITIONS_START_DATE` when daily partitions are used.
- Replaced manual dbt retry (sleep + rerun) with a Dagster retry request.
- Narrowed broad exception capture in orchestration validation to expected error types.
- Updated template docs to reflect current behavior and defaults.

## [0.1.8] - 2026-04-15

### Added

- Initial published version.
