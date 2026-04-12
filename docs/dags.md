# DAG Patterns in Jobbers

Jobbers supports two complementary approaches to building task dependency graphs:

- **Static DAGs** — the full graph is described before any task runs, using the `DAGNode` builder and `StateManager.submit_dag`. All task IDs are pre-assigned at construction time.
- **Dynamic DAGs** — a task function determines the shape of the next stage at runtime and returns a `TaskResult` with a `DynamicFanOut`. The framework wires the fan-in and submits children automatically.

Both approaches can be mixed freely: a static graph node may return a `TaskResult` with a `DynamicFanOut`, and a dynamic collector may carry static `dag_callbacks` to continue an outer graph.

---

## Imports used in all examples

```python
from jobbers.context import get_current_task
from jobbers.models.dag import DAGNode, DynamicFanOut, TaskResult
from jobbers.models.task_config import BackoffStrategy, DeadLetterPolicy
from jobbers.registry import register_task
from jobbers.state_manager import StateManager
```

---

## Node construction styles

Two equivalent approaches exist for creating `DAGNode` objects:

**Direct constructor** — explicit; useful when the task name is dynamic or the registered wrapper isn't in scope:

```python
from jobbers.models.dag import DAGNode

a = DAGNode("extract",   version=1, parameters={"source_url": url})
b = DAGNode("transform", version=1)
c = DAGNode("load",      version=1)
a.then(b).then(c)
```

**Wrapper `.node()`** — concise call-site style when the registered `TaskWrapper` is imported; name and version are inferred automatically:

```python
# extract, transform, load are the TaskWrapper objects returned by @register_task
extract.node(source_url=url).then(transform.node()).then(load.node())
```

Both produce identical `DAGNode` objects. The wrapper style reads like Celery's `.s()` signatures and is convenient when building workflows inline. The constructor style is necessary when the task name is determined at runtime (e.g., in dynamic fan-out children).

---

## 1. Static Chain

Each task runs sequentially: `extract` → `transform` → `load`.

```
[extract] → [transform] → [load]
```

### Task definitions

```python
@register_task(name="extract", version=1)
async def extract(**kwargs):
    data = await fetch_from_source(kwargs["source_url"])
    return TaskResult(results={"rows": data})


@register_task(name="transform", version=1)
async def transform(**kwargs):
    upstream = await get_current_task().parent_results()
    rows = upstream["rows"]
    return TaskResult(results={"rows": [normalize(r) for r in rows]})


@register_task(name="load", version=1)
async def load(**kwargs):
    upstream = await get_current_task().parent_results()
    await write_to_warehouse(upstream["rows"])
    return TaskResult(results={"written": len(upstream["rows"])})
```

### DAG construction and submission

```python
async def run_etl_pipeline(state_manager: StateManager, source_url: str) -> None:
    extract   = DAGNode("extract",   version=1, parameters={"source_url": source_url})
    transform = DAGNode("transform", version=1)
    load      = DAGNode("load",      version=1)

    extract.then(transform)
    transform.then(load)

    await state_manager.submit_dag(extract)
```

Note: `then()` returns `self` (the node it was called on), not the child. Use separate calls to build a chain.

Equivalently, using the registered task wrappers directly:

```python
async def run_etl_pipeline(state_manager: StateManager, source_url: str) -> None:
    e = extract.node(source_url=source_url)
    t = transform.node()
    e.then(t)
    t.then(load.node())
    await state_manager.submit_dag(e)
```

### Alternative: automatic parent result injection

Pass `inject_parent_results=True` to `then()` and declare a `parent_results` parameter on the task function instead of calling `parent_results()` manually:

```python
@register_task(name="transform", version=1)
async def transform(parent_results: dict | None = None, **kwargs):
    rows = parent_results["rows"]   # injected automatically by the worker
    return TaskResult(results={"rows": [normalize(r) for r in rows]})


@register_task(name="load", version=1)
async def load(parent_results: dict | None = None, **kwargs):
    await write_to_warehouse(parent_results["rows"])
    return TaskResult(results={"written": len(parent_results["rows"])})
```

```python
async def run_etl_pipeline(state_manager: StateManager, source_url: str) -> None:
    extract   = DAGNode("extract",   version=1, parameters={"source_url": source_url})
    transform = DAGNode("transform", version=1)
    load      = DAGNode("load",      version=1)

    extract.then(transform, inject_parent_results=True)
    transform.then(load,    inject_parent_results=True)

    await state_manager.submit_dag(extract)
```

The worker fetches the parent task's results from Redis via `parent_ids` and injects them as the `parent_results` kwarg just before calling the function. The injected value is always a `dict` for a single parent and a `list[dict]` for a fan-in collector (same semantics as `parent_results()`).

---

## 2. Static Fan-Out

One root task triggers multiple independent branches that run in parallel.

```
           ┌→ [process_region_a]
[split] ───┼→ [process_region_b]
           └→ [process_region_c]
```

### Task definitions

```python
@register_task(name="split_dataset", version=1)
async def split_dataset(**kwargs):
    return TaskResult(results={"dataset_id": kwargs["dataset_id"]})


@register_task(name="process_region", version=1)
async def process_region(**kwargs):
    upstream = await get_current_task().parent_results()
    result = await run_regional_analysis(upstream["dataset_id"], kwargs["region"])
    return TaskResult(results={"region": kwargs["region"], "summary": result})
```

### DAG construction and submission

```python
async def run_regional_analysis_pipeline(
    state_manager: StateManager, dataset_id: str
) -> None:
    split      = DAGNode("split_dataset", version=1, parameters={"dataset_id": dataset_id})
    region_a   = DAGNode("process_region", version=1, parameters={"region": "A"})
    region_b   = DAGNode("process_region", version=1, parameters={"region": "B"})
    region_c   = DAGNode("process_region", version=1, parameters={"region": "C"})

    split.then(region_a, region_b, region_c)

    await state_manager.submit_dag(split)
```

`then()` accepts multiple nodes, so all three branches are registered in one call.

---

## 3. Static Fan-In

Multiple branches converge into a single collector that runs only after all predecessors complete.

```
[branch_a] ─┐
[branch_b] ─┼→ [aggregate]
[branch_c] ─┘
```

### Task definitions

```python
@register_task(name="score_model", version=1)
async def score_model(**kwargs):
    score = await evaluate_model(kwargs["model_id"], kwargs["dataset"])
    return TaskResult(results={"model_id": kwargs["model_id"], "score": score})


@register_task(name="pick_best_model", version=1)
async def pick_best_model(**kwargs):
    all_results = await get_current_task().parent_results()
    best = max(all_results, key=lambda r: r["score"])
    return TaskResult(results={"winner": best["model_id"], "score": best["score"]})
```

### DAG construction and submission

```python
async def run_model_selection(
    state_manager: StateManager, dataset: str
) -> None:
    model_a  = DAGNode("score_model", version=1, parameters={"model_id": "gpt",   "dataset": dataset})
    model_b  = DAGNode("score_model", version=1, parameters={"model_id": "llama", "dataset": dataset})
    model_c  = DAGNode("score_model", version=1, parameters={"model_id": "mistral","dataset": dataset})
    selector = DAGNode("pick_best_model", version=1)

    DAGNode.merge(model_a, model_b, model_c, into=selector)

    await state_manager.submit_dag(model_a, model_b, model_c)
```

`submit_dag` accepts multiple roots and pre-populates the fan-in tracking set before any root is enqueued.

---

## 4. Static Fan-Out + Fan-In (Diamond)

The canonical pattern: one root fans out to parallel branches which then reconverge.

```
              ┌→ [branch_a] ─┐
[dispatch] ───┤               ├→ [summarize]
              └→ [branch_b] ─┘
```

### Task definitions

```python
@register_task(name="dispatch_chunks", version=1)
async def dispatch_chunks(**kwargs):
    return TaskResult(results={"job_id": kwargs["job_id"]})


@register_task(name="process_chunk", version=1)
async def process_chunk(**kwargs):
    upstream = await get_current_task().parent_results()
    result = await heavy_computation(upstream["job_id"], kwargs["chunk"])
    return TaskResult(results={"chunk": kwargs["chunk"], "output": result})


@register_task(name="summarize_job", version=1)
async def summarize_job(**kwargs):
    chunk_results = await get_current_task().parent_results()
    combined = combine_outputs(chunk_results)
    await store_final_result(kwargs["job_id"], combined)
    return TaskResult(results={"job_id": kwargs["job_id"], "chunks_processed": len(chunk_results)})
```

### DAG construction and submission

```python
async def run_chunked_job(state_manager: StateManager, job_id: str) -> None:
    dispatch  = DAGNode("dispatch_chunks", version=1, parameters={"job_id": job_id})
    chunk_a   = DAGNode("process_chunk",   version=1, parameters={"chunk": "A"})
    chunk_b   = DAGNode("process_chunk",   version=1, parameters={"chunk": "B"})
    summarize = DAGNode("summarize_job",   version=1, parameters={"job_id": job_id})

    dispatch.then(chunk_a, chunk_b)
    DAGNode.merge(chunk_a, chunk_b, into=summarize)

    await state_manager.submit_dag(dispatch)
```

---

## 5. Dynamic Chain

A task decides at runtime which single follow-up task to run, based on its output. Use `TaskResult` with a `DynamicFanOut` containing a single child and a pass-through collector, or simply use a static chain with parameters computed inside the function.

The simplest dynamic chain is a task that chooses the next task name or parameters based on its result. Because `dag_callbacks` are embedded at submission time, truly dynamic routing (where the *name* of the next task is unknown upfront) requires a `DynamicFanOut` with one child.

```
[classify] → [<chosen handler>]
```

### Task definitions

```python
@register_task(name="classify_document", version=1)
async def classify_document(**kwargs):
    doc_type = await detect_type(kwargs["doc_id"])

    # Pick the next task dynamically based on classification.
    handler = DAGNode(
        f"handle_{doc_type}",   # e.g. "handle_invoice" or "handle_receipt"
        version=1,
        parameters={"doc_id": kwargs["doc_id"]},
    )
    passthrough = DAGNode("classification_done", version=1)

    return TaskResult(
        results={"doc_type": doc_type},
        fanout=DynamicFanOut(children=[handler], collector=passthrough),
    )


@register_task(name="handle_invoice", version=1)
async def handle_invoice(**kwargs):
    upstream = await get_current_task().parent_results()
    await route_to_accounts_payable(kwargs["doc_id"])
    return TaskResult(results={"doc_id": kwargs["doc_id"], "routed_to": "accounts_payable"})


@register_task(name="handle_receipt", version=1)
async def handle_receipt(**kwargs):
    upstream = await get_current_task().parent_results()
    await route_to_expense_system(kwargs["doc_id"])
    return TaskResult(results={"doc_id": kwargs["doc_id"], "routed_to": "expense_system"})


@register_task(name="classification_done", version=1)
async def classification_done(**kwargs):
    # Runs after the chosen handler completes.
    results = await get_current_task().parent_results()
    return TaskResult(results=results[0] if results else {})
```

### Submission

```python
async def process_document(state_manager: StateManager, doc_id: str) -> None:
    classify = DAGNode("classify_document", version=1, parameters={"doc_id": doc_id})
    await state_manager.submit_dag(classify)
```

---

## 6. Dynamic Fan-Out

A task fetches a variable-length collection at runtime and spawns one child per item.

```text
[fetch_records] → [process_record] × N   (N known only at runtime)
```

### Task definitions (dynamic fan-out)

```python
@register_task(name="fetch_records", version=1, timeout=60)
async def fetch_records(**kwargs):
    records = await db.query("SELECT id FROM items WHERE status = 'pending'")

    children = [
        DAGNode("process_record", version=1, parameters={"record_id": r["id"]})
        for r in records
    ]
    collector = DAGNode("records_done", version=1)

    return TaskResult(
        results={"total": len(records)},
        fanout=DynamicFanOut(children=children, collector=collector),
    )


@register_task(name="process_record", version=1, max_retries=3, retry_delay=5)
async def process_record(**kwargs):
    upstream = await get_current_task().parent_results()
    await transform_and_store(kwargs["record_id"])
    return TaskResult(results={"record_id": kwargs["record_id"], "status": "processed"})


@register_task(name="records_done", version=1)
async def records_done(**kwargs):
    # Called once, after all process_record tasks finish.
    all_results = await get_current_task().parent_results()
    processed_ids = [r["record_id"] for r in all_results]
    await mark_batch_complete(processed_ids)
    return TaskResult(results={"processed": len(processed_ids)})
```

### Submission (dynamic fan-out)

```python
async def run_batch_processing(state_manager: StateManager) -> None:
    root = DAGNode("fetch_records", version=1)
    await state_manager.submit_dag(root)
```

---

## 7. Dynamic Fan-In

A dynamic fan-in is the collector side of a `DynamicFanOut` — the `collector` node receives results from all children once they all complete. This is already shown in examples 5 and 6 above. The key pattern in the collector function is:

```python
@register_task(name="my_collector", version=1)
async def my_collector(**kwargs):
    all_results = await get_current_task().parent_results()
    # all_results is a list[dict] — one entry per completed predecessor
    ...
```

`parent_results()` detects that this task is a fan-in collector (by checking the `dag:fan-in:{id}:members` key written at submission time) and returns all predecessors' results automatically. No key construction needed in the task body.

---

## 8. Composed Example: Static Outer Graph + Dynamic Inner Fan-Out

A static pipeline triggers a dynamic fan-out stage whose collector continues the outer static graph.

```text
[prepare] → [dispatch_rows] → [process_row] × N → [aggregate] → [notify]
              (dynamic)                              (dynamic)     (static)
```

### Task definitions (composed example)

```python
@register_task(name="prepare_batch", version=1)
async def prepare_batch(**kwargs):
    batch_id = await create_batch(kwargs["source"])
    return TaskResult(results={"batch_id": batch_id})


@register_task(name="dispatch_rows", version=1)
async def dispatch_rows(**kwargs):
    upstream = await get_current_task().parent_results()
    rows = await load_batch(upstream["batch_id"])

    children = [
        DAGNode("process_row", version=1, parameters={"row_id": r["id"]})
        for r in rows
    ]
    # The collector node is pre-wired into the outer static graph,
    # so its dag_callbacks already contain the SimpleCallback to "notify".
    # We receive it as a parameter via the static DAG construction below.
    collector = DAGNode("aggregate_rows", version=1,
                        task_id=kwargs["_collector_id"])

    return TaskResult(
        results={"batch_id": upstream["batch_id"], "row_count": len(rows)},
        fanout=DynamicFanOut(children=children, collector=collector),
    )


@register_task(name="process_row", version=1, max_retries=2)
async def process_row(**kwargs):
    upstream = await get_current_task().parent_results()
    await process(kwargs["row_id"])
    return TaskResult(results={"row_id": kwargs["row_id"], "ok": True})


@register_task(name="aggregate_rows", version=1)
async def aggregate_rows(**kwargs):
    results = await get_current_task().parent_results()
    failed = [r["row_id"] for r in results if not r.get("ok")]
    return TaskResult(results={"failed": failed, "total": len(results)})


@register_task(name="notify_completion", version=1)
async def notify_completion(**kwargs):
    upstream = await get_current_task().parent_results()
    await send_notification(failed=upstream["failed"], total=upstream["total"])
    return TaskResult()
```

### DAG construction and submission

```python
from ulid import ULID

async def run_full_pipeline(state_manager: StateManager, source: str) -> None:
    collector_id = ULID()   # pre-assign so we can pass it as a parameter

    prepare  = DAGNode("prepare_batch",  version=1, parameters={"source": source})
    dispatch = DAGNode("dispatch_rows",  version=1,
                       parameters={"_collector_id": str(collector_id)})
    # aggregate is the dynamic collector — its ID is pre-assigned above.
    aggregate = DAGNode("aggregate_rows",   version=1, task_id=collector_id)
    notify    = DAGNode("notify_completion", version=1)

    # Static outer graph:
    prepare.then(dispatch)
    aggregate.then(notify)   # collector continues into static notify

    await state_manager.submit_dag(prepare)
```

> **Note:** The `_collector_id` parameter trick lets `dispatch_rows` reconstruct the pre-assigned `DAGNode` with the correct ULID so its `dag_callbacks` (carrying the `SimpleCallback` to `notify`) are preserved correctly when the framework serialises it.

---

## Quick Reference

| Pattern | Construction | Submission |
|---|---|---|
| Static chain | `a.then(b).then(c)` | `submit_dag(a)` |
| Static fan-out | `root.then(b, c, d)` | `submit_dag(root)` |
| Static fan-in | `DAGNode.merge(b, c, d, into=collector)` | `submit_dag(b, c, d)` |
| Static diamond | `root.then(b, c)` + `merge(b, c, into=collector)` | `submit_dag(root)` |
| Dynamic chain/fan-out | `return TaskResult(fanout=DynamicFanOut(children=[...], collector=...))` | `submit_dag(root)` |
| Dynamic fan-in | `await get_current_task().parent_results()` in collector | — |

### Accessing parent results

Two approaches are available and can be freely mixed across nodes in the same DAG:

**Manual** — call `parent_results()` inside the task body:

```python
upstream = await get_current_task().parent_results()
```

**Automatic injection** — pass `inject_parent_results=True` on `then()` or `merge()` and declare a `parent_results` parameter on the function:

```python
# Chain
a.then(b, inject_parent_results=True)

@register_task(name="b")
async def b_fn(parent_results: dict | None = None, **kwargs): ...

# Fan-in
DAGNode.merge(branch_a, branch_b, into=collector, inject_parent_results=True)

@register_task(name="collector")
async def collector_fn(parent_results: list | None = None, **kwargs): ...
```

Either way the resolved type is:

| Context | Return type |
| --- | --- |
| Simple chain child (one parent) | `dict` |
| Fan-in collector (static or dynamic, any number of parents) | `list[dict]` |
| Root task (no parents) | `{}` |
