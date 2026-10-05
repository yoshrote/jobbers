import datetime as dt

TIME_ZERO = dt.datetime.fromtimestamp(0, dt.UTC)

# Reserved prefix for tasks jobbers ships and registers itself. User tasks may not claim
# it (enforced in registry.register_task). A double underscore rather than a dot because
# mermaid node labels constrain task names to [a-zA-Z_][a-zA-Z0-9_]* -- a dotted name
# would be unparseable as a label and would break diagram round-tripping.
SYSTEM_TASK_PREFIX = "jobbers__"

# Stands in for a router node whose selection failed. Re-runs the router against the
# parent's stored results when resumed; see jobbers/system_tasks.py.
RERUN_ROUTER_TASK = f"{SYSTEM_TASK_PREFIX}rerun_router"
