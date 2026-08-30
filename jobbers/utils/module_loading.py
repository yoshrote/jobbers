import importlib
import importlib.util
import os
import sys


def load_task_module(arg: str) -> None:
    """Import a task module by dotted name or file path (the latter is registered as `_user_tasks`)."""
    if os.path.isabs(arg) or arg.endswith(".py"):
        spec = importlib.util.spec_from_file_location("_user_tasks", arg)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load task module from path: {arg}")
        module = importlib.util.module_from_spec(spec)
        sys.modules["_user_tasks"] = module
        spec.loader.exec_module(module)
    else:
        importlib.import_module(arg)
