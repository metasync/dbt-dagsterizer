from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from ..orchestration_config import resolve_orchestration_path


def resolve_dir_arg(value: str) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()


def split_csv(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def orchestration_path(*, dbt_project_dir: Path, path_: str) -> Path:
    rel = Path(path_)
    if rel.is_absolute():
        return rel
    return resolve_orchestration_path(dbt_project_dir=dbt_project_dir, path_=rel)


def existing_model_names(manifest: dict[str, Any]) -> set[str]:
    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        return set()
    names: set[str] = set()
    for props in nodes.values():
        if not isinstance(props, dict) or props.get("resource_type") != "model":
            continue
        name = props.get("name")
        if isinstance(name, str) and name.strip():
            names.add(name.strip())
    return names


def select_models(
    *,
    dbt_project_dir: Path,
    manifest: dict[str, Any] | None,
    models_csv: str,
    tag_: str,
) -> list[str]:
    selected = split_csv(models_csv)
    if tag_:
        if manifest is None:
            raise click.ClickException("--tag selection requires manifest")
        nodes = manifest.get("nodes") or {}
        for props in nodes.values():
            if not isinstance(props, dict) or props.get("resource_type") != "model":
                continue
            tags = set(props.get("tags") or [])
            name = props.get("name")
            if isinstance(name, str) and name.strip() and tag_ in tags:
                selected.append(name.strip())
    selected = sorted(set([m for m in selected if m]))
    return selected

