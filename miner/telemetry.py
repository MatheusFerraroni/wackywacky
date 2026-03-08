import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor

from miner.settings.settings import Settings

def setup_telemetry() -> None:
    resource = Resource.create({
        "service.name": Settings.OTEL_SERVICE_NAME,
        "service.version": Settings.OTEL_SERVICE_VERSION,
        "deployment.environment": Settings.OTEL_ENV,
    })

    endpoint = Settings.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # RequestsInstrumentor().instrument()
    PyMySQLInstrumentor().instrument()
