# Run the server using uvicorn
import importlib
import logging
import sys

import uvicorn

ENABLE_OTEL = True
def run() -> None:
    from jobbers import db
    from jobbers.task_routes import app

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]

    if ENABLE_OTEL:
        from jobbers.utils.otel import enable_otel
        enable_otel(handlers, service_name="jobbers-manager")

        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)


    # Initialize the database client
    db.get_client()

    # register tasks so we can validate submitted task signatures
    task_module = sys.argv[1]
    importlib.import_module(task_module)

    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run()
