from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path


def parse_dotenv_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            quote = value[0]
            value = value[1:-1]
            if quote == '"':
                value = (
                    value.replace("\\n", "\n")
                    .replace("\\r", "\r")
                    .replace("\\t", "\t")
                    .replace('\\"', '"')
                    .replace("\\\\", "\\")
                )
        out[key] = value
    return out


def dotenv_paths_for_dbt_project(dbt_project_dir: Path) -> list[Path]:
    return [dbt_project_dir.parent / ".env", dbt_project_dir / ".env"]


def dotenv_overrides_for_dbt_project(*, dbt_project_dir: Path) -> dict[str, str | None]:
    parsed: dict[str, str] = {}
    for p in dotenv_paths_for_dbt_project(dbt_project_dir):
        parsed.update(parse_dotenv_file(p))
    return {k: v for k, v in parsed.items() if os.environ.get(k) is None}


def max_mtime(paths: list[Path]) -> float | None:
    mt: float | None = None
    for p in paths:
        if not p.exists():
            continue
        m = p.stat().st_mtime
        mt = m if mt is None else max(mt, m)
    return mt


@contextmanager
def temporary_env(overrides: dict[str, str | None]):
    old = {k: os.environ.get(k) for k in overrides.keys()}
    try:
        for k, v in overrides.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

