# Run the server using uvicorn
import logging
import os
import sys

import uvicorn


def enable_otel(handlers, app):
    from opentelemetry import trace
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
        OTLPLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.metrics import set_meter_provider
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource=Resource.create(
        {
            "service.name": "jobbers",
            "service.instance.id": os.uname().nodename,
        }
    )
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    metric_exporter = OTLPMetricExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    metric_reader = PeriodicExportingMetricReader(metric_exporter)
    metric_provider = MeterProvider(metric_readers=[metric_reader])
    set_meter_provider(metric_provider)

    trace.set_tracer_provider(TracerProvider())
    trace_exporter = OTLPSpanExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    trace_processor = BatchSpanProcessor(trace_exporter)
    trace.get_tracer_provider().add_span_processor(trace_processor)

    exporter = OTLPLogExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4317"),
        insecure=True
    )
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    handlers.append(handler)

    FastAPIInstrumentor.instrument_app(app)


ENABLE_OTEL = True
def run():
    from . import db
    from .task_routes import app

    handlers = [logging.StreamHandler(stream=sys.stdout)]

    if ENABLE_OTEL:
        enable_otel(handlers, app)

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    # Initialize the database client
    db.get_client()

    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run()
