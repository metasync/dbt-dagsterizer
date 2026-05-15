from __future__ import annotations

import json
import os
from collections.abc import Mapping

DAGSTER_K8S_CONFIG_TAG = "dagster-k8s/config"


def build_luban_run_k8s_config_tag_value() -> str | None:
    configmap = (os.getenv("LUBAN_RUN_ENV_CONFIGMAP") or "").strip()
    secret = (os.getenv("LUBAN_RUN_ENV_SECRET") or "").strip()

    env_from: list[dict] = []
    if configmap:
        env_from.append({"configMapRef": {"name": configmap}})
    if secret:
        env_from.append({"secretRef": {"name": secret}})

    if not env_from:
        return None

    return json.dumps({"container_config": {"envFrom": env_from}}, separators=(",", ":"), sort_keys=True)


def with_luban_run_k8s_config_tag(tags: Mapping[str, str] | None) -> dict[str, str] | None:
    merged = dict(tags) if tags else {}
    if DAGSTER_K8S_CONFIG_TAG in merged:
        return merged

    value = build_luban_run_k8s_config_tag_value()
    if value is None:
        return merged or None

    merged[DAGSTER_K8S_CONFIG_TAG] = value
    return merged
