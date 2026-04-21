import importlib

import pytest
import pytest_asyncio
from ulid import ULID

from jobbers.adapters.raw_redis import MsgpackTaskAdapter
from jobbers.models.queue_config import QueueConfig, QueueConfigAdapter
from jobbers.models.task_status import TaskStatus
from jobbers.registry import clear_registry
from jobbers.state_manager import StateManager
from jobbers.task_processor import TaskProcessor
from jobbers.utils.mermaid_dag import parse_mermaid_dag

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def register_e2e_tasks():
    import end2end

    clear_registry()
    importlib.reload(end2end)
    yield
    clear_registry()


@pytest_asyncio.fixture
async def sm(redis, session_factory):
    await QueueConfigAdapter(session_factory).save_queue_config(QueueConfig(name="default"))
    return StateManager(redis, session_factory, task_adapter=MsgpackTaskAdapter(redis))


# ── Helpers ───────────────────────────────────────────────────────────────────


async def drain(sm: StateManager) -> int:
    """Pop and process tasks until all queues are empty. Returns count processed."""
    queues = {"default"}
    ta = sm.ta
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
        await TaskProcessor(sm).process(task)
        count += 1
    return count


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
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots)
    await run_until_done(sm)

    assert len(submitted) == 1
    task = await sm.ta.get_task(submitted[0].id)
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
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"] --> B["echo_task@1(value=b)"] --> C["echo_task@1(value=c)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots)
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]  # type: ignore[attr-defined]
    c_node = b_node._successors[0][0]  # type: ignore[attr-defined]

    task_a = await sm.ta.get_task(a_node.id)
    task_b = await sm.ta.get_task(b_node.id)
    task_c = await sm.ta.get_task(c_node.id)

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
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"] --> B["echo_task@1(value=b)"]
      A["echo_task@1(value=a)"] --> C["echo_task@1(value=c)"]
      B["echo_task@1(value=b)"] --> D["echo_task@1(value=d)"]
      C["echo_task@1(value=c)"] --> D["echo_task@1(value=d)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots)
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]  # type: ignore[attr-defined]
    c_node = a_node._successors[1][0]  # type: ignore[attr-defined]
    d_node = b_node._successors[0][0]  # type: ignore[attr-defined]

    task_a = await sm.ta.get_task(a_node.id)
    task_b = await sm.ta.get_task(b_node.id)
    task_c = await sm.ta.get_task(c_node.id)
    task_d = await sm.ta.get_task(d_node.id)

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
    # A fails permanently; B is the (never-reached) success callback;
    # C is the error callback that should fire when A fails.
    diagram = """
    flowchart TD
      A["always_fail_task@1"] --> B["echo_task@1(value=success_cb)"]
      A -.-> C["echo_task@1(value=error_cb)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots)
    await run_until_done(sm)

    a_node = roots[0]
    # _successors entry: (successor, fan_in_key, error_node, inject_parent_results)
    c_node = a_node._successors[0][2]  # type: ignore[attr-defined]

    task_a = await sm.ta.get_task(a_node.id)
    task_c = await sm.ta.get_task(c_node.id)

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
    diagram = """
    flowchart TD
      A["always_fail_task@1"] --> B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots)
    await run_until_done(sm)

    a_node = roots[0]
    b_node = a_node._successors[0][0]  # type: ignore[attr-defined]

    task_a = await sm.ta.get_task(a_node.id)
    task_b = await sm.ta.get_task(b_node.id)

    assert task_a is not None
    assert task_a.status == TaskStatus.FAILED
    assert task_b is None  # B was never submitted


# ── Scenario 6: Multi-root DAG ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_multi_root_dag(sm: StateManager) -> None:
    diagram = """
    flowchart TD
      A["echo_task@1(value=a)"]
      B["echo_task@1(value=b)"]
    """
    roots = parse_mermaid_dag(diagram)
    await sm.submit_dag(*roots)
    await run_until_done(sm)

    # roots order is non-deterministic (set iteration); look up by parameter
    a_root = next(r for r in roots if r._parameters.get("value") == "a")  # type: ignore[attr-defined]
    b_root = next(r for r in roots if r._parameters.get("value") == "b")  # type: ignore[attr-defined]
    task_a = await sm.ta.get_task(a_root.id)
    task_b = await sm.ta.get_task(b_root.id)

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
    diagram = """
    flowchart TD
      A["always_fail_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots)
    await run_until_done(sm)

    task = await sm.ta.get_task(submitted[0].id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_attempt == 2
    assert len(task.errors) == 3

    dlq = await sm.dead_queue.get_by_ids([str(submitted[0].id)])
    assert len(dlq) == 1


# ── Scenario 8: Scheduled retry path (zero-delay, exercises scheduler) ────────


@pytest.mark.asyncio
async def test_scheduled_retry_path(sm: StateManager) -> None:
    diagram = """
    flowchart TD
      A["scheduled_fail_task@1"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots)
    task_id = submitted[0].id

    await run_until_done(sm)

    task = await sm.ta.get_task(task_id)
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.retry_attempt == 2
    assert len(task.errors) == 3

    dlq = await sm.dead_queue.get_by_ids([str(task_id)])
    assert len(dlq) == 1


# ── Scenario 9: Parameters passed correctly and reflected in results ───────────


@pytest.mark.asyncio
async def test_parameters_passed_and_results(sm: StateManager) -> None:
    diagram = """
    flowchart TD
      A["echo_task@1(value=hello)"]
    """
    roots = parse_mermaid_dag(diagram)
    _, submitted = await sm.submit_dag(*roots)
    await run_until_done(sm)

    task = await sm.ta.get_task(submitted[0].id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.results == {"value": "hello"}
    assert task.parameters == {"value": "hello"}
