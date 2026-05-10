from __future__ import annotations

import atexit
import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

_log = logging.getLogger(__name__)
_configured_key: tuple[Any, ...] | None = None
_configured_result: OTelConfigureResult | None = None


@dataclass(frozen=True)
class OTelConfigureResult:
    traces_configured: bool
    metrics_configured: bool


def _normalize_exporter(value: str | None) -> str:
    v = (value or "").strip().lower()
    return v


def _parse_resource_attributes(value: str | None) -> dict[str, str]:
    raw = (value or "").strip()
    if not raw:
        return {}

    out: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k or not v:
            continue
        out[k] = v
    return out


def _normalize_otlp_endpoint(*, endpoint: str, protocol: str) -> str:
    ep = endpoint.strip()
    if not ep:
        return ep

    if protocol == "grpc":
        parsed = urlparse(ep)
        if parsed.scheme and parsed.netloc:
            return parsed.netloc
        return ep

    parsed = urlparse(ep)
    if parsed.scheme and parsed.netloc:
        return ep
    return f"http://{ep}"


def _http_otlp_signal_endpoint(*, endpoint: str, signal: str) -> str:
    parsed = urlparse(endpoint)
    if not (parsed.scheme and parsed.netloc):
        return endpoint

    if parsed.path and parsed.path != "/":
        return endpoint

    base = endpoint.rstrip("/")
    if signal == "traces":
        return f"{base}/v1/traces"
    if signal == "metrics":
        return f"{base}/v1/metrics"
    return endpoint


def configure_otel() -> OTelConfigureResult:
    global _configured_key, _configured_result

    traces_exporter = _normalize_exporter(os.getenv("OTEL_TRACES_EXPORTER"))
    metrics_exporter = _normalize_exporter(os.getenv("OTEL_METRICS_EXPORTER"))

    traces_enabled = traces_exporter not in {"", "none"}
    metrics_enabled = metrics_exporter not in {"", "none"}
    if not (traces_enabled or metrics_enabled):
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    endpoint = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "").strip()
    protocol = (os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL") or "").strip().lower() or "http/protobuf"
    if protocol and protocol not in {"grpc", "http/protobuf"}:
        _log.warning("Invalid OTEL_EXPORTER_OTLP_PROTOCOL=%r; disabling OTLP export", protocol)
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    if not endpoint:
        _log.warning("OTEL_EXPORTER_OTLP_ENDPOINT is missing; disabling OTLP export")
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    endpoint = _normalize_otlp_endpoint(endpoint=endpoint, protocol=protocol)

    service_name = (os.getenv("OTEL_SERVICE_NAME") or "").strip()
    resource_attrs_raw = os.getenv("OTEL_RESOURCE_ATTRIBUTES")
    resource_attrs = _parse_resource_attributes(resource_attrs_raw)
    if service_name:
        resource_attrs.setdefault("service.name", service_name)

    headers_raw = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")

    configured_key = (
        traces_exporter,
        metrics_exporter,
        endpoint,
        protocol,
        service_name,
        resource_attrs_raw,
        headers_raw,
    )
    if _configured_key == configured_key and _configured_result is not None:
        return _configured_result

    try:
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except Exception as e:
        _log.warning("OpenTelemetry packages are unavailable; OTEL disabled (%s)", e)
        return OTelConfigureResult(traces_configured=False, metrics_configured=False)

    resource = Resource.create(resource_attrs)

    traces_configured = False
    if traces_enabled and traces_exporter in {"otlp"}:
        try:
            if protocol == "grpc":
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            else:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            trace_endpoint = endpoint
            if protocol == "http/protobuf":
                trace_endpoint = _http_otlp_signal_endpoint(endpoint=endpoint, signal="traces")

            provider = TracerProvider(resource=resource)
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=trace_endpoint)))
            trace.set_tracer_provider(provider)
            atexit.register(provider.shutdown)
            traces_configured = True
        except Exception as e:
            _log.warning("Failed configuring OTLP trace exporter; traces disabled (%s)", e)

    metrics_configured = False
    if metrics_enabled and metrics_exporter in {"otlp"}:
        try:
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

            if protocol == "grpc":
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                    OTLPMetricExporter,
                )
            else:
                from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                    OTLPMetricExporter,
                )

            metric_endpoint = endpoint
            if protocol == "http/protobuf":
                metric_endpoint = _http_otlp_signal_endpoint(endpoint=endpoint, signal="metrics")

            reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=metric_endpoint))
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            metrics.set_meter_provider(provider)
            atexit.register(provider.shutdown)
            metrics_configured = True
        except Exception as e:
            _log.warning("Failed configuring OTLP metric exporter; metrics disabled (%s)", e)

    _configured_key = configured_key
    _configured_result = OTelConfigureResult(
        traces_configured=traces_configured, metrics_configured=metrics_configured
    )
    return _configured_result

