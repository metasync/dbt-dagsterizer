import json
import os
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dbt.errors import DagsterDbtCliRuntimeError

from ...dbt.run_results import add_run_results_telemetry
from ...orchestration_config import (
    default_orchestration_path,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...otel import (
    otel_dagster_transaction_info,
    otel_record_exception,
    otel_span,
    otel_transaction_span,
)
from ...resources.dbt import get_dbt_project_dir
from ..sources.automation import load_automation_observable_sources
from .prepare import prepare_manifest_if_missing
from .translator import LubanDagsterDbtTranslator
from .vars import _get_dbt_vars_for_context

_dbt_assets_def = None


def _should_retry_dbt_cli_error(*, message: str, retry_number: int) -> bool:
    message = (message or "").lower()
    return "already exists" in message and retry_number < 1


def get_dbt_assets():
    global _dbt_assets_def
    if _dbt_assets_def is not None:
        return _dbt_assets_def

    dbt_project_dir = get_dbt_project_dir()
    dbt_target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"
    dbt_project = DbtProject(
        project_dir=dbt_project_dir,
        target=dbt_target,
        packaged_project_dir=dbt_project_dir,
    )

    prepare_manifest_if_missing()
    automation_observable_tables = {
        spec["table"] for spec in load_automation_observable_sources() if spec.get("table")
    }

    orch_cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    orch_cfg = load_orch(orch_cfg_path)
    orch_index = index_orch(orch_cfg)

    translator = LubanDagsterDbtTranslator(
        daily_partitions_def=None,
        automation_observable_tables=automation_observable_tables,
        partitions_by_model=orch_index.partitions_by_model,
    )

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        dagster_dbt_translator=translator,
    )
    def _dbt_assets(context, dbt: DbtCliResource):
        retry_number = int(getattr(context, "retry_number", 0) or 0)
        tx_span_name, _, tx_attrs = otel_dagster_transaction_info(context)
        tx_attrs["dagster.retry_number"] = retry_number
        with otel_transaction_span(tx_span_name, attributes=tx_attrs):
            with otel_span("dbt.vars"):
                dbt_vars = _get_dbt_vars_for_context(context)
            dbt_args = ["build"]
            if dbt_vars:
                dbt_args += ["--vars", json.dumps(dbt_vars)]
            with otel_span(
                "dbt.cli",
                attributes={
                    "dbt.command": dbt_args[0] if dbt_args else "",
                    "dbt.target": dbt_target,
                },
            ) as span:
                try:
                    yield from dbt.cli(dbt_args, context=context).stream()
                except DagsterDbtCliRuntimeError as e:
                    otel_record_exception(span, e)
                    if _should_retry_dbt_cli_error(message=str(e), retry_number=retry_number):
                        context.log.warning(
                            "dbt invocation failed with 'already exists'; requesting a retry"
                        )
                        raise dg.RetryRequested(seconds_to_wait=2) from e
                    raise
                finally:
                    add_run_results_telemetry(
                        run_results_path=dbt_project_dir / "target" / "run_results.json",
                        manifest_path=dbt_project_dir / "target" / "manifest.json",
                        parent_span_attributes={
                            "dbt.target": dbt_target,
                        },
                    )

    _dbt_assets_def = _dbt_assets
    return _dbt_assets_def
