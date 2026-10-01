"""
Runtime tests for router nodes.

``TaskProcessor._handle_router`` drives simple-mode selection; the per-item
variant runs inside ``_handle_declarative_fanout``. Fan-in sizing and dispatch
are involved on both paths, so the converging-branch and per-item cases are
exercised against a real backend (``state_manager_real_ta`` -- RedisTaskState /
RedisTaskSubmit over FakeRedis, real Lua scripts) rather than only a stub.
"""

from __future__ import annotations

import pytest
from ulid import ULID

from jobbers.models.dag import FanInCallback, RouterCallback
from jobbers.models.router import RouteTo
from jobbers.models.task import Task, TaskStatus
from jobbers.registry import clear_registry, register_router, register_task
from jobbers.task_processor import RouterError, TaskProcessor
from jobbers.utils.mermaid_dag import parse_mermaid_dag

SIMPLE_ROUTER = """
flowchart TD
    A["measure_payload"]
    R{"route_by_size(threshold=100)"}
    B["fast_path"]
    C["slow_path:heavy"]

    A --> R
    R --> B
    R --> C
"""

TIERED_ROUTER = """
flowchart TD
    A["classify_order"]
    R{"route_by_tier"}
    P["fulfil_order:priority"]
    S["fulfil_order:standard"]

    A --> R
    R --> P
    R --> S
"""

CONVERGING = """
flowchart TD
    A["measure_payload"]
    R{"route_by_size"}
    B["fast_path"]
    C["slow_path"]
    D["report"]

    A --> R
    R --> B
    R --> C
    B --> D
    C --> D
"""

PER_ITEM = """
flowchart TD
    A["fetch_records"]
    R{"route_by_region"}
    US["process_record:us"]
    EU["process_record:eu"]
    C["aggregate"]

    A -->> R
    R --> US
    R --> EU
    US --o C
    EU --o C
"""


@pytest.fixture(autouse=True)
def _clean_registry():
    clear_registry()
    yield
    clear_registry()


def _register_tasks(*names: str) -> None:
    for name in names:

        @register_task(name=name, version=0)
        async def _task(**kwargs):  # pragma: no cover - never executed here
            return {}


def _root_task(diagram: str, *, dag_run_id: ULID | None = None, results: dict | None = None) -> Task:
    """Parse *diagram* and return its root as a COMPLETED task ready for post_process."""
    root = parse_mermaid_dag(diagram)[0]
    task = root.to_task(dag_run_id=dag_run_id or ULID(), dag_run_name="test-run")
    task.results = results or {}
    task.status = TaskStatus.COMPLETED
    return task


async def _run_tasks_by_name(sm, dag_run_id: ULID) -> dict[str, list[Task]]:
    """Group every task registered to *dag_run_id* by task name."""
    detail = await sm.task_state.get_dag_run(dag_run_id)
    assert detail is not None
    grouped: dict[str, list[Task]] = {}
    for tid in detail.task_ids:
        task = await sm.task_state.get_task(tid)
        if task is not None:
            grouped.setdefault(task.name, []).append(task)
    return grouped


async def _collector_of(sm, arm: Task) -> Task:
    """
    Fetch the collector an *arm* fans into.

    Declarative fan-out clones its collector template (``fresh_copy``), so the
    submitted collector's ULID is not ``cb.collector.id``; the arm's own
    FanInCallback is what names the real one. It is pre-saved rather than
    submitted, so it is also absent from the run's task index until it fires.
    """
    fan_ins = [cb for cb in arm.dag_callbacks if isinstance(cb, FanInCallback)]
    assert len(fan_ins) == 1, arm.dag_callbacks
    collector = await sm.task_state.get_task(fan_ins[0].task.id)
    assert collector is not None
    return collector


# ── simple-mode selection ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_router_submits_the_selected_candidate(state_manager_real_ta):
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        return "fast_path" if results["bytes"] < threshold else "slow_path"

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    chosen = next(c for c in cb.router.candidates if c.name == "fast_path")
    unchosen = next(c for c in cb.router.candidates if c.name == "slow_path")

    submitted = await state_manager_real_ta.task_state.get_task(chosen.id)
    assert submitted is not None
    assert submitted.name == "fast_path"
    assert submitted.status == TaskStatus.SUBMITTED
    assert submitted.parent_ids == [task.id]
    # The branch that was not selected is never created.
    assert await state_manager_real_ta.task_state.get_task(unchosen.id) is None


@pytest.mark.asyncio
async def test_router_selects_by_queue_when_candidates_share_a_task_name(state_manager_real_ta):
    _register_tasks("classify_order", "fulfil_order")

    @register_router(name="route_by_tier", version=0)
    def route_by_tier(results):
        return RouteTo("fulfil_order", queue="priority" if results["vip"] else "standard")

    task = _root_task(TIERED_ROUTER, results={"vip": True})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    priority = next(c for c in cb.router.candidates if c.queue == "priority")
    standard = next(c for c in cb.router.candidates if c.queue == "standard")

    submitted = await state_manager_real_ta.task_state.get_task(priority.id)
    assert submitted is not None
    assert submitted.queue == "priority"
    # With no routing rule, the identity default puts it on the same-named queue.
    assert submitted.queue == "priority"
    assert await state_manager_real_ta.task_state.get_task(standard.id) is None


@pytest.mark.asyncio
async def test_router_returning_none_submits_nothing(state_manager_real_ta):
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        return None

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    for candidate in cb.router.candidates:
        assert await state_manager_real_ta.task_state.get_task(candidate.id) is None


@pytest.mark.asyncio
async def test_router_receives_node_parameters(state_manager_real_ta):
    _register_tasks("measure_payload", "fast_path", "slow_path")
    seen: dict = {}

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        seen["threshold"] = threshold
        seen["results"] = results
        return "fast_path"

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 7})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    assert seen == {"threshold": 100, "results": {"bytes": 7}}


# ── failure modes ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("router_body", "match"),
    [
        pytest.param(lambda results, threshold: 1 / 0, "raised", id="router-raises"),
        pytest.param(lambda results, threshold: "no_such_task", "matches none", id="unknown-name"),
        pytest.param(lambda results, threshold: 42, "expected a task name", id="bad-return-type"),
    ],
)
async def test_router_failures_raise_router_error(state_manager_real_ta, router_body, match):
    _register_tasks("measure_payload", "fast_path", "slow_path")
    register_router(name="route_by_size", version=0)(
        lambda results, *, threshold: router_body(results, threshold)
    )

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    with pytest.raises(RouterError, match=match):
        await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)


@pytest.mark.asyncio
async def test_unregistered_router_raises_router_error(state_manager_real_ta):
    _register_tasks("measure_payload", "fast_path", "slow_path")
    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    with pytest.raises(RouterError, match="Unknown router"):
        await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)


@pytest.mark.asyncio
async def test_ambiguous_bare_name_selection_names_the_queues(state_manager_real_ta):
    """A bare task name is ambiguous when candidates differ only by queue."""
    _register_tasks("classify_order", "fulfil_order")

    @register_router(name="route_by_tier", version=0)
    def route_by_tier(results):
        return "fulfil_order"

    task = _root_task(TIERED_ROUTER, results={})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    with pytest.raises(RouterError, match="ambiguous across queues"):
        await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)


@pytest.mark.asyncio
async def test_router_failure_is_recorded_without_changing_task_status(state_manager_real_ta):
    """post_process failures leave the parent COMPLETED but record the error."""
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        raise RuntimeError("boom")

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    processor = TaskProcessor(state_manager_real_ta)
    try:
        await processor.post_process(task)
    except RouterError as exc:
        await processor._handle_post_process_failure(task, exc)

    assert task.status == TaskStatus.COMPLETED
    assert any("post_process failed" in e and "boom" in e for e in task.errors)


# ── converging branches ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_converging_branches_submit_the_collector_exactly_once(state_manager_real_ta):
    """
    Only one branch runs, so `report` must fire on that branch alone.

    Real-backend test: a fan-in set would leave `report` waiting on a branch that
    never executes, and only real fan-in semantics can demonstrate it does not.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path", "report")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results):
        return "fast_path"

    dag_run_id = ULID()
    task = _root_task(CONVERGING, dag_run_id=dag_run_id, results={})
    processor = TaskProcessor(state_manager_real_ta)
    await processor.post_process(task)

    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    fast = next(c for c in cb.router.candidates if c.name == "fast_path")
    report_spec = fast.dag_callbacks[0].task

    # Now complete the chosen branch; `report` should be submitted immediately,
    # with no fan-in set holding it back.
    branch_task = await state_manager_real_ta.task_state.get_task(fast.id)
    assert branch_task is not None
    branch_task.status = TaskStatus.COMPLETED
    await processor.post_process(branch_task)

    report = await state_manager_real_ta.task_state.get_task(report_spec.id)
    assert report is not None
    assert report.name == "report"
    assert report.status == TaskStatus.SUBMITTED
    assert report.parent_ids == [fast.id]


# ── per-item routing ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_per_item_routing_spreads_arms_across_queues(state_manager_real_ta):
    """
    Each item routes independently, so one fan-out can span several queues.

    Real-backend test: the collector's fan-in set is sized at routing time from
    the arms actually spawned, which only real fan-in semantics can verify.
    """
    _register_tasks("fetch_records", "process_record", "aggregate")

    @register_router(name="route_by_region", version=0)
    def route_by_region(item):
        return RouteTo("process_record", queue=item["region"])

    dag_run_id = ULID()
    task = _root_task(
        PER_ITEM,
        dag_run_id=dag_run_id,
        results={"items": [{"region": "us", "n": 1}, {"region": "eu", "n": 2}, {"region": "us", "n": 3}]},
    )
    processor = TaskProcessor(state_manager_real_ta)
    await processor.post_process(task)

    grouped = await _run_tasks_by_name(state_manager_real_ta, dag_run_id)
    arms = grouped["process_record"]
    assert len(arms) == 3
    assert sorted(a.queue for a in arms) == ["eu", "us", "us"]
    assert sorted(a.parameters["n"] for a in arms) == [1, 2, 3]

    collector = await _collector_of(state_manager_real_ta, arms[0])
    assert collector.name == "aggregate"
    assert sorted(str(i) for i in collector.parent_ids) == sorted(str(a.id) for a in arms)

    # Completing every arm closes the fan-in exactly once.
    submitted_collectors = 0
    for arm in arms:
        arm.status = TaskStatus.COMPLETED
        before = await state_manager_real_ta.task_state.get_task(collector.id)
        await processor.post_process(arm)
        after = await state_manager_real_ta.task_state.get_task(collector.id)
        if before.status != TaskStatus.SUBMITTED and after.status == TaskStatus.SUBMITTED:
            submitted_collectors += 1
    assert submitted_collectors == 1


@pytest.mark.asyncio
async def test_per_item_router_declining_an_item_drops_that_arm(state_manager_real_ta):
    _register_tasks("fetch_records", "process_record", "aggregate")

    @register_router(name="route_by_region", version=0)
    def route_by_region(item):
        return None if item["region"] == "skip" else RouteTo("process_record", queue=item["region"])

    dag_run_id = ULID()
    task = _root_task(
        PER_ITEM,
        dag_run_id=dag_run_id,
        results={"items": [{"region": "us"}, {"region": "skip"}, {"region": "eu"}]},
    )
    await TaskProcessor(state_manager_real_ta).post_process(task)

    grouped = await _run_tasks_by_name(state_manager_real_ta, dag_run_id)
    arms = grouped["process_record"]
    assert len(arms) == 2
    assert sorted(a.queue for a in arms) == ["eu", "us"]

    # The declined item contributes no arm, so the fan-in is sized to 2.
    collector = await _collector_of(state_manager_real_ta, arms[0])
    assert len(collector.parent_ids) == 2


@pytest.mark.asyncio
async def test_per_item_router_gets_the_item_not_the_whole_result(state_manager_real_ta):
    _register_tasks("fetch_records", "process_record", "aggregate")
    seen: list[dict] = []

    @register_router(name="route_by_region", version=0)
    def route_by_region(item):
        seen.append(item)
        return RouteTo("process_record", queue=item["region"])

    task = _root_task(PER_ITEM, results={"items": [{"region": "us"}, {"region": "eu"}]})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    assert seen == [{"region": "us"}, {"region": "eu"}]
