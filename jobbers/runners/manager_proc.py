# Run the server using uvicorn
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

    handlers: list[logging.Handler] = [logging.StreamHandler(stream=sys.stdout)]

    if ENABLE_OTEL:
        from jobbers.utils.otel import enable_otel

        enable_otel(handlers, service_name="jobbers-manager")

        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)

    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("jobbers").setLevel(logging.DEBUG)

    # register tasks so we can validate submitted task signatures
    _load_task_module(sys.argv[1])

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()
