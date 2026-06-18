"""OpenTelemetry setup for traces, metrics, and logs."""

import logging
from typing import Optional

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from miner.settings.settings import Settings

_REQUESTS_INSTRUMENTED = False
_PYMYSQL_INSTRUMENTED = False
_LOGGING_INSTRUMENTED = False
_TELEMETRY_INITIALIZED = False


def _build_resource() -> Resource:
    """Build the common OpenTelemetry resource."""
    return Resource.create(
        {
            'service.name': Settings.OTEL_SERVICE_NAME,
            'service.version': Settings.OTEL_SERVICE_VERSION,
            'deployment.environment': Settings.OTEL_ENV,
        }
    )


def _normalize_endpoint(base_or_signal_endpoint: Optional[str], signal_path: str) -> str:
    """Normalize an OTLP endpoint to the requested signal path."""
    if not base_or_signal_endpoint:
        raise ValueError(f'Missing OTLP endpoint for {signal_path}')

    endpoint = base_or_signal_endpoint.rstrip('/')

    if (
        endpoint.endswith('/v1/traces')
        or endpoint.endswith('/v1/logs')
        or endpoint.endswith('/v1/metrics')
    ):
        return endpoint

    return f'{endpoint}/{signal_path.lstrip("/")}'


def setup_telemetry() -> None:  # pylint: disable=global-statement
    """Initialize OpenTelemetry exporters and instrumentations once."""
    # pylint: disable=global-statement
    global _REQUESTS_INSTRUMENTED
    global _PYMYSQL_INSTRUMENTED
    global _LOGGING_INSTRUMENTED
    global _TELEMETRY_INITIALIZED

    if _TELEMETRY_INITIALIZED:
        return

    if not Settings.MINER_TELEMETRY_ENABLED:
        _TELEMETRY_INITIALIZED = True
        logging.getLogger(__name__).info('Miner telemetry disabled by MINER_TELEMETRY_ENABLED')
        return

    resource = _build_resource()

    traces_endpoint = _normalize_endpoint(
        getattr(Settings, 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', None),
        '/v1/traces',
    )

    tracer_provider = TracerProvider(resource=resource)
    trace_exporter = OTLPSpanExporter(endpoint=traces_endpoint)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(tracer_provider)

    metrics_endpoint_setting = getattr(
        Settings,
        'OTEL_EXPORTER_OTLP_METRICS_ENDPOINT',
        getattr(Settings, 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', None),
    )
    metrics_endpoint = _normalize_endpoint(metrics_endpoint_setting, '/v1/metrics')

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=metrics_endpoint),
        export_interval_millis=10000,
    )

    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(meter_provider)

    logs_endpoint_setting = getattr(
        Settings,
        'OTEL_EXPORTER_OTLP_LOGS_ENDPOINT',
        getattr(Settings, 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', None),
    )
    logs_endpoint = _normalize_endpoint(logs_endpoint_setting, '/v1/logs')

    logger_provider = LoggerProvider(resource=resource)
    log_exporter = OTLPLogExporter(endpoint=logs_endpoint)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)

    otel_handler = LoggingHandler(
        level=logging.NOTSET,
        logger_provider=logger_provider,
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(otel_handler)

    if not _LOGGING_INSTRUMENTED:
        LoggingInstrumentor().instrument(set_logging_format=True)
        _LOGGING_INSTRUMENTED = True

    if not _PYMYSQL_INSTRUMENTED:
        PyMySQLInstrumentor().instrument()
        _PYMYSQL_INSTRUMENTED = True

    if not _REQUESTS_INSTRUMENTED:
        RequestsInstrumentor().instrument()
        _REQUESTS_INSTRUMENTED = True

    _TELEMETRY_INITIALIZED = True
