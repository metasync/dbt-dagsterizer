from __future__ import annotations

import importlib.resources
import os
from pathlib import Path

import click

from .common import resolve_dir_arg


_DEFAULT_TEMPLATE_NAME = "luban-dagster-dbt-starrocks-code-location-source-template"
_TEMPLATE_MARKER_NAME = ".dbt_dagsterizer_template"


def _template_macros_dir(*, template_name: str) -> importlib.resources.abc.Traversable:
    base = importlib.resources.files("dbt_dagsterizer").joinpath("project_templates")
    template_root = base.joinpath(template_name)
    if not template_root.is_dir():
        raise click.ClickException(f"Template not found: {template_name}")
    return template_root.joinpath(
        "{{cookiecutter.app_name}}",
        "dbt_project",
        "macros",
        "dbt_dagsterizer",
    )


def _sync_macros(*, dbt_project_path: Path, template_name: str, force: bool) -> tuple[int, Path]:
    macros_dir = dbt_project_path / "macros" / "dbt_dagsterizer"
    macros_dir.mkdir(parents=True, exist_ok=True)

    template_dir = _template_macros_dir(template_name=template_name)
    if not template_dir.is_dir():
        raise click.ClickException(f"Macro templates not found: {template_dir}")

    synced = 0
    with importlib.resources.as_file(template_dir) as template_path:
        for src in template_path.rglob("*.sql"):
            rel = src.relative_to(template_path)
            dst = macros_dir / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists() and not force:
                continue
            try:
                dst.write_bytes(src.read_bytes())
            except Exception as e:
                raise click.ClickException(f"Failed to sync macro file {rel}: {e}") from e
            synced += 1

    return synced, macros_dir


def _resolve_template_name(*, dbt_project_path: Path) -> str:
    override = (os.getenv("DBT_DAGSTERIZER_TEMPLATE") or "").strip()
    if override:
        return override

    marker = dbt_project_path / _TEMPLATE_MARKER_NAME
    if marker.exists():
        name = marker.read_text(encoding="utf-8").strip()
        if name:
            return name

    return _DEFAULT_TEMPLATE_NAME


def build_macros_group() -> click.Group:
    @click.group()
    def macros() -> None:
        pass

    @macros.command("sync")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--force/--no-force", default=False, show_default=True)
    def macros_sync(dbt_project_dir: str, force: bool) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        template_name = _resolve_template_name(dbt_project_path=dbt_project_path)
        synced, macros_dir = _sync_macros(
            dbt_project_path=dbt_project_path, template_name=template_name, force=force
        )
        click.echo(f"Synced {synced} macro file(s) into {macros_dir}")

    return macros
