import logging
import os
import platform

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_metrics(resource: Resource) -> None:
    metric_exporter = OTLPMetricExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    metric_reader = PeriodicExportingMetricReader(metric_exporter)
    metric_provider = MeterProvider(metric_readers=[metric_reader], resource=resource)
    set_meter_provider(metric_provider)

def setup_tracer(resource: Resource) -> None:
    trace.set_tracer_provider(TracerProvider(resource=resource))
    trace_exporter = OTLPSpanExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    trace_processor = BatchSpanProcessor(trace_exporter)
    trace.get_tracer_provider().add_span_processor(trace_processor)

def setup_logger(resource: Resource) -> LoggingHandler:
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    log_exporter = OTLPLogExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    return LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

def enable_otel(handlers, service_name="jobbers") -> None:
    resource=Resource.create(
        {
            "service.name": service_name,
            "service.instance.id": platform.uname().node,
        }
    )
    otel_handler = setup_logger(resource)
    setup_metrics(resource)
    setup_tracer(resource)

    handlers.append(otel_handler)
