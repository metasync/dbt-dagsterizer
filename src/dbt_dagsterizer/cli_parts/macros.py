from __future__ import annotations

import shutil
from pathlib import Path

import click

from .common import resolve_dir_arg


def _install_macros(*, dbt_project_path: Path, force: bool) -> tuple[int, Path]:
    macros_dir = dbt_project_path / "macros"
    macros_dir.mkdir(parents=True, exist_ok=True)

    template_dir = Path(__file__).resolve().parent.parent / "macro_templates"
    if not template_dir.exists():
        raise click.ClickException(f"Macro templates not found: {template_dir}")

    installed = 0
    for src in template_dir.rglob("*.sql"):
        rel = src.relative_to(template_dir)
        dst = macros_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not force:
            continue
        shutil.copy2(src, dst)
        installed += 1

    return installed, macros_dir


def build_macros_group() -> click.Group:
    @click.group()
    def macros() -> None:
        pass

    @macros.command("install")
    @click.option("--dbt-project-dir", default="./dbt_project", show_default=True)
    @click.option("--force/--no-force", default=False, show_default=True)
    def macros_install(dbt_project_dir: str, force: bool) -> None:
        dbt_project_path = resolve_dir_arg(dbt_project_dir)
        installed, macros_dir = _install_macros(dbt_project_path=dbt_project_path, force=force)
        click.echo(f"Installed {installed} macro file(s) into {macros_dir}")

    return macros
