import logging
from typing import Optional

from opentelemetry import trace, metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry._logs import set_logger_provider
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

from miner.settings.settings import Settings


_requests_instrumented = False
_pymysql_instrumented = False
_logging_instrumented = False
_telemetry_initialized = False


def _build_resource() -> Resource:
    return Resource.create(
        {
            "service.name": Settings.OTEL_SERVICE_NAME,
            "service.version": Settings.OTEL_SERVICE_VERSION,
            "deployment.environment": Settings.OTEL_ENV,
        }
    )


def _normalize_endpoint(base_or_signal_endpoint: Optional[str], signal_path: str) -> str:
    """
    Aceita tanto endpoint completo quanto base OTLP.

    Exemplos válidos de entrada:
    - http://otel-collector:4318
    - http://otel-collector:4318/
    - http://otel-collector:4318/v1/traces
    - http://otel-collector:4318/v1/logs
    """
    if not base_or_signal_endpoint:
        raise ValueError(f"Missing OTLP endpoint for {signal_path}")

    endpoint = base_or_signal_endpoint.rstrip("/")

    if endpoint.endswith("/v1/traces") or endpoint.endswith("/v1/logs") or endpoint.endswith("/v1/metrics"):
        return endpoint

    return f"{endpoint}/{signal_path.lstrip('/')}"


def setup_telemetry() -> None:
    global _requests_instrumented
    global _pymysql_instrumented
    global _logging_instrumented
    global _telemetry_initialized

    if _telemetry_initialized:
        return

    resource = _build_resource()

    # TRACE EXPORT
    traces_endpoint = _normalize_endpoint(
        getattr(Settings, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None),
        "/v1/traces",
    )

    tracer_provider = TracerProvider(resource=resource)
    trace_exporter = OTLPSpanExporter(endpoint=traces_endpoint)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(tracer_provider)

    # METRICS EXPORT
    metrics_endpoint_setting = getattr(
        Settings,
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        getattr(Settings, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None),
    )
    metrics_endpoint = _normalize_endpoint(metrics_endpoint_setting, "/v1/metrics")

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=metrics_endpoint),
        export_interval_millis=10000,
    )

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(meter_provider)

    # LOG EXPORT
    #
    # Se você tiver Settings.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, ele será usado.
    # Caso não tenha, reaproveita o endpoint de traces trocando para /v1/logs.
    logs_endpoint_setting = getattr(
        Settings,
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        getattr(Settings, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None),
    )
    logs_endpoint = _normalize_endpoint(logs_endpoint_setting, "/v1/logs")

    logger_provider = LoggerProvider(resource=resource)
    log_exporter = OTLPLogExporter(endpoint=logs_endpoint)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)

    # Encaminha logging padrão do Python para OTel
    otel_handler = LoggingHandler(
        level=logging.NOTSET,
        logger_provider=logger_provider,
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(otel_handler)

    # Injeta trace_id/span_id no logging
    if not _logging_instrumented:
        LoggingInstrumentor().instrument(set_logging_format=True)
        _logging_instrumented = True

    # Auto-instrumentation opcional
    if not _pymysql_instrumented:
        PyMySQLInstrumentor().instrument()
        _pymysql_instrumented = True

    # Ative só se você realmente usa requests e quiser spans automáticos HTTP cliente
    if not _requests_instrumented:
        RequestsInstrumentor().instrument()
        _requests_instrumented = True

    _telemetry_initialized = True