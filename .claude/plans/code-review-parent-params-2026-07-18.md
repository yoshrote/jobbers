# Code Review — `parent-params` branch (2026-07-18)

Scope: uncommitted working-tree diff (`git diff HEAD`) replacing the old `inject_parent_results` boolean-flag mechanism with a new `Annotated[T, FromParent(key, many=...)]` parameter-injection mechanism. Changed files: `jobbers/models/dag.py`, `jobbers/task_processor.py`, `jobbers/models/task.py`, `jobbers/adapters/sql/task_state.py`, `jobbers/migrations/schema.py`, `openapi.json`, docs, and tests.

Review method: 8 finder angles (line-by-line diff scan, removed-behavior audit, cross-file tracer, reuse/simplification/efficiency/altitude/conventions) run in parallel, findings deduplicated and verified by direct code inspection (including live reproduction of the top two bugs).

---

## Critical

### 1. `jobbers/task_routes.py:412` — stale 4-tuple unpack crashes any DAG with an edge

`_validate_dag_against_registry` still does:

```python
for successor, _, error_node, _ in node._successors:
```

but `DAGNode._successors` was shortened from a 4-tuple `(node, fan_in_key, error_node, inject_parent_results)` to a 3-tuple `(node, fan_in_key, error_node)` everywhere else in the diff. This file wasn't touched.

**Failure scenario:** `POST /submit-dag` (and `POST`/`PUT /cron-dags`) with any mermaid diagram containing at least one edge (any `.then()`/`.merge()`) raises `ValueError: not enough values to unpack (expected 4, got 3)` — an unhandled 500 for virtually every real DAG submission. Reproduced directly: `a=DAGNode('a'); b=DAGNode('b'); a.then(b)` then iterating `a._successors` as a 4-tuple fails immediately. Existing tests only submit single-node (edge-free) diagrams, so this is untested and currently masked.

### 2. `jobbers/utils/mermaid_dag.py:560,577` — mermaid DAGs bypass the new fan-in cardinality check

`DAGNode.merge()` is called once per incoming edge in a loop instead of once with the full predecessor group:

```python
for edge in success_edges:
    ...
    if edge.dst in fan_in_collectors:
        DAGNode.merge(dag_nodes[edge.src], into=dag_nodes[edge.dst], on_error=on_error)
```

`validate_fan_in_cardinality`'s `if len(predecessors) < 2: return` guard fires every time since each call only ever passes one predecessor, so the check never actually runs for mermaid-parsed fan-ins.

**Failure scenario:** A mermaid diagram with `A --> C` and `B --> C`, where `C`'s registered task has a singular (`many=False`) `FromParent` param, does **not** raise `FanInCardinalityError` at parse time despite the documented "raises at graph-construction time" guarantee. `jobbers/task_processor.py`'s `_spec_to_dag_node` hit the identical pattern and was explicitly patched with a pre-loop `validate_fan_in_cardinality` call over the full predecessor group; `mermaid_dag.py` has no equivalent fix. This is the primary DAG-authoring path (per the docs), so this defeats the safety net for most real users.

### 3. `jobbers/task_processor.py:199` — FromParent resolution errors escape task failure handling entirely

The loop calling `_resolve_from_parent` sits between two `try` blocks — after the one guarding `get_type_hints()`, before the one guarding the actual task-function call — so it is not covered by either:

```python
for param_name, spec in from_parent_specs.items():
    value = _resolve_from_parent(task, param_name, spec, parent_results_map)  # unguarded
    ...
```

**Failure scenario:** When a fan-in collector reaches execution with the wrong parent count (reachable via bug #2 above), `_resolve_from_parent` raises `ValueError` in this unguarded region. The exception propagates uncaught out of `process()` — skipping `remove_task_heartbeat` and `_maybe_cleanup` — through `run()`'s `TaskGroup`, up to `worker_proc.py`'s `run_task()`, whose only handler is `finally: semaphore.release()`. Verified `worker_proc.py` never awaits/inspects each task's exception outside of shutdown, so it's dropped as an "exception was never retrieved" asyncio warning. The task is left stuck in `STARTED` forever with an orphaned heartbeat entry, and its DAG run's pending counter never decrements — silently hanging the run instead of surfacing a clean `FAILED` status.

---

## High

### 4. `jobbers/task_routes.py:550,604,664` — `except MermaidParseError` doesn't catch `FanInCardinalityError`

`FanInCardinalityError` subclasses `ValueError` directly — it's a sibling of `MermaidParseError`, not a child of it.

**Failure scenario:** Once bug #2 is fixed, submitting a mermaid DAG whose fan-in collector has a singular `FromParent` param raises `FanInCardinalityError` from inside `parse_mermaid_dag()`; the route's `except MermaidParseError as exc:` doesn't catch it, producing an unhandled 500 instead of the clean 400 that structurally-identical validation errors (e.g. duplicate node names) already get.

### 5. `jobbers/migrations/schema.py:79` — dropped column has no corresponding migration

`run_migrations` (`jobbers/migrations/runner.py:37`) only calls `metadata.create_all(...)`, which never alters existing tables.

**Failure scenario:** A production deployment with `TASK_BACKEND=sql` or `ROUTING_BACKEND=sql` upgrading in place keeps its existing `tasks` table with `inject_parent_results BOOLEAN NOT NULL` (the old `Column` used Python-side `default=`, not `server_default`, so there's no DB-level default). The new `_task_to_row` insert no longer supplies that column, so every task insert after upgrade raises `IntegrityError: NOT NULL constraint failed: tasks.inject_parent_results` — reproduced live during review.

---

## Medium

### 6. `jobbers/task_processor.py:552` — fan-out wiring failures during `post_process` are silently swallowed

A `FanInCardinalityError` (or any exception) raised while wiring a dynamic/declarative fan-out inside `post_process` is caught only by `process()`'s generic `except Exception as exc: await self._handle_post_process_failure(task, exc)`, which just logs and appends to `task.errors` without changing status.

**Failure scenario:** A task returns `TaskResult(fanout=DynamicFanOut(...))` targeting a collector with a singular `FromParent` param. `DAGNode.merge(*terminals, into=fanout.collector)` raises `FanInCardinalityError`; the dispatcher then falls through to `finalize_dag_run_task` as if it closed normally — decrementing the DAG run's pending counter with no arms submitted and no fan-in set initialized, silently completing or hanging the run with only a buried log entry as evidence.

---

## Low / Cleanup

### 7. `jobbers/task_processor.py:186` — duplicated Annotated-unwrapping logic

`task_processor.py` re-implements the exact `Annotated`/`get_args`/`isinstance(meta, FromParent)` loop that already exists as `_extract_from_parent` in `jobbers/models/dag.py`, instead of importing and calling it. Both implementations are identical today, but the leading underscore on `_extract_from_parent` signals it was never meant to be duplicated — a future change to marker detection has to be made in both places and can silently drift out of sync.

### 8. `jobbers/task_processor.py:187` — `hints.items()` walked twice

`hints.items()` is iterated once to extract `FromParent` specs and again inside the resolver block to extract `_Depends` specs. A single combined loop collecting both per parameter would halve the traversal with no behavior change.

---

## Priority for fixing

Start with **#1 and #3** — they look like they'd break ordinary DAG submission/execution outright; worth a smoke test (submit a `.then()`-chained DAG through `/submit-dag`) before merging regardless. **#2 and #4** compound each other since mermaid is the primary DAG-authoring path. **#5** only affects SQL-backend deployments upgrading an existing database in place.
