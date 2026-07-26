import asyncio
import contextlib
import importlib

import pytest
import pytest_asyncio
from ulid import ULID

import end2end
from jobbers.adapters.redis import (
    RedisCancellationBus,
    RedisCronDAGScheduler,
    RedisDeadQueue,
    RedisRoutingNotifications,
    RedisTaskScheduler,
    RedisTaskState,
    RedisTaskSubmit,
)
from jobbers.adapters.sql import SQLQueueConfigAdapter, SQLRoutingBackend
from jobbers.models.dag import DagRunStatus
from jobbers.models.queue_config import QueueConfig
from jobbers.models.task_status import TaskStatus
from jobbers.registry import clear_registry
from jobbers.state_manager import StateManager
from jobbers.task_processor import TaskProcessor
from jobbers.utils.mermaid_dag import parse_mermaid_dag

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def register_e2e_tasks():
    clear_registry()
    importlib.reload(end2end)
    yield
    clear_registry()


@pytest_asyncio.fixture
async def sm(redis, session_factory):
    await SQLQueueConfigAdapter(session_factory).save_queue_config(QueueConfig(name="default"))
    routing_backend = SQLRoutingBackend(session_factory)
    task_state = RedisTaskState(redis)
    task_submit = RedisTaskSubmit(redis, task_state)
    return StateManager(
        routing_backend,
        task_state=task_state,
        task_submit=task_submit,
        dead_queue=RedisDeadQueue(redis, task_state),
        task_scheduler=RedisTaskScheduler(redis, task_state, routing_backend.get_all_queues),
        cron_dag_scheduler=RedisCronDAGScheduler(redis),
        cancellation_bus=RedisCancellationBus(redis),
        routing_notifications=RedisRoutingNotifications(redis),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


async def drain(sm: StateManager) -> int:
    """Pop and process tasks until all queues are empty. Returns count processed."""
    queues = {"default"}
    ta = sm.task_state
    count = 0
    while True:
        task = None
        for q in queues:
            results = await ta.data_store.zpopmin(ta.TASKS_BY_QUEUE(queue=q), count=1)
            if results:
                task_id_bytes, _ = results[0]
                task = await ta.get_task(ULID.from_bytes(task_id_bytes))
                break
        if task is None:
            break
        await TaskProcessor(sm).run(task)
        count += 1
    return count


async def pop_and_process_one(sm: StateManager, queue: str = "default") -> None:
    """Pop and process exactly one task from *queue* (no draining loop)."""
    ta = sm.task_state
    results = await ta.data_store.zpopmin(ta.TASKS_BY_QUEUE(queue=queue), count=1)
    assert results, f"expected a queued task in {queue!r}"
    task_id_bytes, _ = results[0]
    task = await ta.get_task(ULID.from_bytes(task_id_bytes))
    assert task is not None
    await TaskProcessor(sm).run(task)


async def tick_scheduler(sm: StateManager) -> int:
    """Dispatch all currently-due scheduled tasks. Returns count dispatched."""
    entries = await sm.task_scheduler.next_due_bulk(100)
    for task, _ in entries:
        await sm.dispatch_scheduled_task(task)
    return len(entries)


async def run_until_done(sm: StateManager, max_rounds: int = 20) -> None:
    """Alternate drain/tick until neither produces work."""
    for _ in range(max_rounds):
        processed = await drain(sm)
        dispatched = await tick_scheduler(sm)
        if not processed and not dispatched:
            break


# ── Scenario 1: Single node, happy path ──────────────────────────────────────


@pytest.mark.asyncio
async def test_single_node_happy_path(sm: StateManager) -> None:
    """A single-node DAG completes with the correct results and all timestamps set."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    assert len(submitted) == 1
    task = await sm.task_state.get_task(submitted[0].id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.results == {"value": "a"}
    assert task.submitted_at is not None
    assert task.started_at is not None
    assert task.completed_at is not None
    assert task.dag_run_id is not None


# ── Scenario 2: Linear chain ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_linear_chain(sm: StateManager) -> None:
    """A→B→C chain: each node completes in order and shares the same dag_run_id."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"] --> B["echo_task@1(value=b)"] --> C["echo_task@1(value=c)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]
    c_node = b_node._successors[0][0]

    task_a = await sm.task_state.get_task(a_node.id)
    task_b = await sm.task_state.get_task(b_node.id)
    task_c = await sm.task_state.get_task(c_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.COMPLETED
    assert task_b is not None
    assert task_b.status == TaskStatus.COMPLETED
    assert task_c is not None
    assert task_c.status == TaskStatus.COMPLETED

    assert task_a.results == {"value": "a"}
    assert task_b.results == {"value": "b"}
    assert task_c.results == {"value": "c"}

    assert task_a.dag_run_id == task_b.dag_run_id == task_c.dag_run_id
    assert a_node.id in task_b.parent_ids
    assert b_node.id in task_c.parent_ids


# ── Scenario 3: Diamond fan-out / fan-in ──────────────────────────────────────


@pytest.mark.asyncio
async def test_diamond_fan_out_fan_in(sm: StateManager) -> None:
    """A→{B,C}→D: D waits for both B and C, then completes with both as parents."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"] --> B["echo_task@1(value=b)"]
      A["echo_task@1(value=a)"] --> C["echo_task@1(value=c)"]
      B["echo_task@1(value=b)"] --> D["echo_task@1(value=d)"]
      C["echo_task@1(value=c)"] --> D["echo_task@1(value=d)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]
    c_node = a_node._successors[1][0]
    d_node = b_node._successors[0][0]

    task_a = await sm.task_state.get_task(a_node.id)
    task_b = await sm.task_state.get_task(b_node.id)
    task_c = await sm.task_state.get_task(c_node.id)
    task_d = await sm.task_state.get_task(d_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.COMPLETED
    assert task_b is not None
    assert task_b.status == TaskStatus.COMPLETED
    assert task_c is not None
    assert task_c.status == TaskStatus.COMPLETED
    assert task_d is not None
    assert task_d.status == TaskStatus.COMPLETED

    assert task_a.results == {"value": "a"}
    assert task_b.results == {"value": "b"}
    assert task_c.results == {"value": "c"}
    assert task_d.results == {"value": "d"}

    assert len(task_d.parent_ids) == 2
    assert {b_node.id, c_node.id} == set(task_d.parent_ids)


# ── Scenario 4: Error callback fires on permanent failure ─────────────────────


@pytest.mark.asyncio
async def test_error_callback_fires_on_failure(sm: StateManager) -> None:
    """When A fails permanently its error callback (-.-> edge) is submitted; the success callback is not."""
    # A fails permanently; B is the (never-reached) success callback;
    # C is the error callback that should fire when A fails.
    diagram = """
    flowchart TD
      A["always_fail_task@1"] --> B["echo_task@1(value=success_cb)"]
      A -.-> C["echo_task@1(value=error_cb)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = roots[0]
    # _successors entry: (successor, fan_in_key, error_node)
    c_node = a_node._successors[0][2]

    task_a = await sm.task_state.get_task(a_node.id)
    task_c = await sm.task_state.get_task(c_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.FAILED
    assert task_a.retry_attempt == 2
    assert len(task_a.errors) == 3

    assert task_c is not None
    assert task_c.status == TaskStatus.COMPLETED
    assert task_c.results == {"value": "error_cb"}

    dlq = await sm.dead_queue.get_by_ids([str(a_node.id)])
    assert len(dlq) == 1


# ── Scenario 5: Downstream task never submitted when parent fails ──────────────


@pytest.mark.asyncio
async def test_downstream_not_submitted_on_parent_failure(sm: StateManager) -> None:
    """A successor linked by --> is never submitted when its parent fails permanently."""
    diagram = """
    flowchart TD
      A["always_fail_task@1"] --> B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]

    task_a = await sm.task_state.get_task(a_node.id)
    task_b = await sm.task_state.get_task(b_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.FAILED
    assert task_b is None  # B was never submitted


# ── Scenario 6: Multi-root DAG ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_multi_root_dag(sm: StateManager) -> None:
    """Two independent root nodes share a dag_run_id and both complete."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
      B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    # roots order is non-deterministic (set iteration); look up by parameter
    a_root = next(r for r in roots if r._parameters.get("value") == "a")
    b_root = next(r for r in roots if r._parameters.get("value") == "b")
    task_a = await sm.task_state.get_task(a_root.id)
    task_b = await sm.task_state.get_task(b_root.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.COMPLETED
    assert task_b is not None
    assert task_b.status == TaskStatus.COMPLETED
    assert task_a.results == {"value": "a"}
    assert task_b.results == {"value": "b"}
    assert task_a.dag_run_id == task_b.dag_run_id


# ── Scenario 7: Immediate retry exhaustion lands in DLQ ───────────────────────


@pytest.mark.asyncio
async def test_retry_exhaustion_dlq(sm: StateManager) -> None:
    """A task that fails on every attempt exhausts its retries and lands in the DLQ."""
    diagram = """
    flowchart TD
      A["always_fail_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    task = await sm.task_state.get_task(submitted[0].id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_attempt == 2
    assert len(task.errors) == 3

    dlq = await sm.dead_queue.get_by_ids([str(submitted[0].id)])
    assert len(dlq) == 1


# ── Scenario 8: Scheduled retry path (zero-delay, exercises scheduler) ────────


@pytest.mark.asyncio
async def test_scheduled_retry_path(sm: StateManager) -> None:
    """A task configured with a retry delay exercises the scheduler path before landing in the DLQ."""
    diagram = """
    flowchart TD
      A["scheduled_fail_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots, name="test-run")
    task_id = submitted[0].id

    await run_until_done(sm)

    task = await sm.task_state.get_task(task_id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_attempt == 2
    assert len(task.errors) == 3

    dlq = await sm.dead_queue.get_by_ids([str(task_id)])
    assert len(dlq) == 1


# ── Scenario 9: Parameters passed correctly and reflected in results ───────────


@pytest.mark.asyncio
async def test_parameters_passed_and_results(sm: StateManager) -> None:
    """Task parameters are forwarded to the function and reflected in the stored results."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=hello)"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    task = await sm.task_state.get_task(submitted[0].id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.results == {"value": "hello"}
    assert task.parameters == {"value": "hello"}


# ── Scenario 10: Cancellation ─────────────────────────────────────────────────


async def _wait_for_started(sm: StateManager, task_id: ULID, *, max_wait: float = 2.0) -> None:
    """Poll until the task reaches STARTED status or the deadline expires."""
    deadline = asyncio.get_event_loop().time() + max_wait
    while asyncio.get_event_loop().time() < deadline:
        t = await sm.task_state.get_task(task_id)
        if t is not None and t.status == TaskStatus.STARTED:
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"Task {task_id} did not reach STARTED within {max_wait}s")


@pytest.mark.asyncio
async def test_cancel_running_task(sm: StateManager) -> None:
    """A STARTED task transitions to CANCELLED when a cancellation is requested."""
    diagram = """
    flowchart TD
      A["slow_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots, name="test-run")
    task_id = submitted[0].id

    cancel_listener = asyncio.create_task(sm.run_cancel_listener())
    drain_bg = asyncio.create_task(drain(sm))
    await _wait_for_started(sm, task_id)
    await asyncio.sleep(0.05)  # let the cancel listener subscribe

    await sm.request_task_cancellation(task_id)
    await drain_bg

    cancel_listener.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await cancel_listener

    result = await sm.task_state.get_task(task_id)
    assert result is not None
    assert result.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_cancel_dag_root_leaves_downstream_unsubmitted(sm: StateManager) -> None:
    """When the root of a DAG chain is cancelled, successor tasks are never submitted."""
    diagram = """
    flowchart TD
      A["slow_task@1"] --> B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")

    a_node = roots[0]
    b_node = a_node._successors[0][0]

    cancel_listener = asyncio.create_task(sm.run_cancel_listener())
    drain_bg = asyncio.create_task(drain(sm))
    await _wait_for_started(sm, a_node.id)
    await asyncio.sleep(0.05)  # let the cancel listener subscribe

    await sm.request_task_cancellation(a_node.id)
    await drain_bg

    cancel_listener.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await cancel_listener

    task_a = await sm.task_state.get_task(a_node.id)
    task_b = await sm.task_state.get_task(b_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.CANCELLED
    assert task_b is None  # successor was never submitted


# ── Scenario 11: DAG run aggregate status ──────────────────────────────────────
#
# These exercise the real RedisTaskState/RedisTaskSubmit adapters (via the `sm`
# fixture, not DummyTaskAdapter) end-to-end through TaskProcessor.run(), since the
# ordering/atomicity of the aggregate-status write path is exactly the kind of
# interaction Dummy-backed orchestration tests can't prove is race-free.


@pytest.mark.asyncio
async def test_dag_run_reaches_complete_status_only_after_last_task(sm: StateManager) -> None:
    """A 2-root DAG's aggregate status becomes 'complete' only once every task has completed."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
      B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    dag_run_id, submitted = await sm.submit_dag(*roots, name="two-root-run")
    assert len(submitted) == 2

    await pop_and_process_one(sm)

    run = await sm.get_dag_run(dag_run_id)
    assert run is not None
    assert run.name == "two-root-run"
    assert run.status == DagRunStatus.RUNNING  # one task done, one still pending

    await pop_and_process_one(sm)

    run = await sm.get_dag_run(dag_run_id)
    assert run is not None
    assert run.status == DagRunStatus.COMPLETE


@pytest.mark.asyncio
async def test_dag_run_with_one_stuck_task_latches_to_partial_failure_and_never_completes(
    sm: StateManager,
) -> None:
    """A run with a mix of a completed and a permanently-failed task reaches 'partial_failure' and stays there."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
      B["always_fail_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    dag_run_id, submitted = await sm.submit_dag(*roots, name="mixed-run")
    assert len(submitted) == 2

    await run_until_done(sm)

    run = await sm.get_dag_run(dag_run_id)
    assert run is not None
    assert run.status == DagRunStatus.PARTIAL_FAILURE

    # The stuck (FAILED) task never lets the pending count reach zero, so the
    # complete-detection path (close_dag_run_task_and_sweep) never fires for this
    # run -- status must stay latched at partial_failure, never flip to complete.
    b_task = await sm.task_state.get_task(
        submitted[1].id if submitted[0].name == "echo_task" else submitted[0].id
    )
    assert b_task is not None
    assert b_task.status == TaskStatus.FAILED


@pytest.mark.asyncio
async def test_dag_run_concurrent_last_completers_both_reach_complete_idempotently(sm: StateManager) -> None:
    """Two siblings finishing concurrently and both hitting the 'last completer' path is race-free."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
      B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    dag_run_id, submitted = await sm.submit_dag(*roots, name="concurrent-run")
    assert len(submitted) == 2

    ta = sm.task_state
    results = await ta.data_store.zpopmin(ta.TASKS_BY_QUEUE(queue="default"), count=2)
    assert len(results) == 2
    tasks = [await ta.get_task(ULID.from_bytes(task_id_bytes)) for task_id_bytes, _ in results]
    assert all(t is not None for t in tasks)

    await asyncio.gather(*(TaskProcessor(sm).run(t) for t in tasks))

    run = await sm.get_dag_run(dag_run_id)
    assert run is not None
    assert run.status == DagRunStatus.COMPLETE


# ── Scenario 12: FromParent resolution through real dispatch ──────────────────


@pytest.mark.asyncio
async def test_from_parent_chain_resolution(sm: StateManager) -> None:
    """A downstream FromParent param is resolved from the real parent's stored results through actual dispatch."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=hello)"] --> B["from_parent_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]
    task_b = await sm.task_state.get_task(b_node.id)

    assert task_b is not None
    assert task_b.status == TaskStatus.COMPLETED
    assert task_b.results == {"seen": "hello"}


@pytest.mark.asyncio
async def test_from_parent_many_fan_in_resolution(sm: StateManager) -> None:
    """A many=True FromParent collector resolves a real list across real fan-in predecessors."""
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"] --> C["from_parent_many_task@1"]
      B["echo_task@1(value=b)"] --> C["from_parent_many_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    a_node = next(r for r in roots if r._name == "echo_task")
    c_node = a_node._successors[0][0]
    task_c = await sm.task_state.get_task(c_node.id)

    assert task_c is not None
    assert task_c.status == TaskStatus.COMPLETED
    assert sorted(task_c.results["seen"]) == ["a", "b"]


@pytest.mark.asyncio
async def test_from_parent_task_used_as_root_with_direct_override(sm: StateManager) -> None:
    """
    A FromParent-annotated task can be submitted as a root node; the submitted parameter is used directly.

    Before the root-node fix, FromParent(many=False) raised unconditionally whenever
    parent_ids != 1 -- including 0 -- so this would have failed rather than complete.
    """
    diagram = """
    flowchart TD
      A["from_parent_task@1(value=direct)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    task = await sm.task_state.get_task(roots[0].id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.results == {"seen": "direct"}


@pytest.mark.asyncio
async def test_from_parent_task_used_as_root_falls_back_to_default(sm: StateManager) -> None:
    """A FromParent-annotated task used as a root node with no submitted value uses its Python default."""
    diagram = """
    flowchart TD
      A["from_parent_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots, name="test-run")
    await run_until_done(sm)

    task = await sm.task_state.get_task(roots[0].id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.results == {"seen": "no-parent-value"}
