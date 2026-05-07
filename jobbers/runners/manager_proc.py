# Run the server using uvicorn
import argparse
import importlib
import importlib.util
import logging
import os
import sys

import uvicorn

ENABLE_OTEL = True


def _load_task_module(arg: str) -> None:
    if os.path.isabs(arg) or arg.endswith(".py"):
        spec = importlib.util.spec_from_file_location("_user_tasks", arg)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load task module from path: {arg}")
        module = importlib.util.module_from_spec(spec)
        sys.modules["_user_tasks"] = module
        spec.loader.exec_module(module)
    else:
        importlib.import_module(arg)


def run() -> None:
    from jobbers.task_routes import app

    parser = argparse.ArgumentParser(description="Jobbers Manager")
    parser.add_argument("task_module", help="Task module to load (dotted name or file path)")
    parser.add_argument(
        "--static-config",
        metavar="FILE",
        default=None,
        help="Path to a JSON/YAML static routing config file. Implies ROUTING_BACKEND=static.",
    )
    args = parser.parse_args()

    if args.static_config:
        from jobbers import db
        from jobbers.adapters.static_routing import StaticRoutingBackend

        db.register_routing_backend(StaticRoutingBackend.from_file(args.static_config))

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]

    if ENABLE_OTEL:
        from jobbers.utils.otel import enable_otel

        enable_otel(handlers, service_name="jobbers-manager")

        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    _load_task_module(args.task_module)

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()
