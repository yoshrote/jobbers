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

from jobbers import db
from jobbers.constants import RERUN_ROUTER_TASK
from jobbers.models.dag import DAGNode, FanInCallback, RouterCallback, SimpleCallback
from jobbers.models.router import RouteTo
from jobbers.models.task import Task, TaskStatus
from jobbers.registry import register_router, register_task, reset_registry
from jobbers.system_tasks import rerun_router
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
    reset_registry()
    yield
    reset_registry()


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
    ("router_body", "match", "reason"),
    [
        pytest.param(lambda results, threshold: 1 / 0, "raised", "raised", id="router-raises"),
        pytest.param(
            lambda results, threshold: "no_such_task",
            "matches none",
            "no_candidate",
            id="unknown-name",
        ),
        pytest.param(
            lambda results, threshold: 42,
            "expected a task name",
            "invalid_return",
            id="bad-return-type",
        ),
    ],
)
async def test_router_failures_produce_a_placeholder(state_manager_real_ta, router_body, match, reason):
    """
    Every router failure mode lands as a FAILED placeholder carrying the router's own id.

    The exception does not escape: a router failure is the router's business, not the
    parent's, so ``_handle_router`` contains it.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path")
    register_router(name="route_by_size", version=0)(
        lambda results, *, threshold: router_body(results, threshold)
    )

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)

    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None, "the router node's id must now hold a placeholder task"
    assert placeholder.name == RERUN_ROUTER_TASK
    assert placeholder.status == TaskStatus.FAILED
    assert any(match in e for e in placeholder.errors)
    assert placeholder.parameters["reason"] == reason
    assert placeholder.queue == task.queue, "the placeholder runs where the parent ran"
    # No candidate was submitted — the branch is held, not guessed at.
    for candidate in cb.router.candidates:
        assert await state_manager_real_ta.task_state.get_task(candidate.id) is None


@pytest.mark.asyncio
async def test_unregistered_router_placeholder_is_marked_retryable(state_manager_real_ta):
    """An unregistered router is the one transient failure: a mid-deploy worker may have it."""
    _register_tasks("measure_payload", "fast_path", "slow_path")
    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)

    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None
    assert placeholder.parameters["reason"] == "unregistered"
    assert placeholder.parameters["retryable"] is True


@pytest.mark.asyncio
async def test_ambiguous_bare_name_selection_names_the_queues(state_manager_real_ta):
    """A bare task name is ambiguous when candidates differ only by queue."""
    _register_tasks("classify_order", "fulfil_order")

    @register_router(name="route_by_tier", version=0)
    def route_by_tier(results):
        return "fulfil_order"

    task = _root_task(TIERED_ROUTER, results={})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)

    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None
    assert any("ambiguous across queues" in e for e in placeholder.errors)


@pytest.mark.asyncio
async def test_router_failure_leaves_the_parent_completed(state_manager_real_ta):
    """The parent did its own work correctly, so its status and errors are untouched."""
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        raise RuntimeError("boom")

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    task.set_status(TaskStatus.COMPLETED)
    await TaskProcessor(state_manager_real_ta).post_process(task)

    assert task.status == TaskStatus.COMPLETED
    assert task.errors == [], "the failure belongs to the router's placeholder, not the parent"
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None
    assert any("boom" in e for e in placeholder.errors)


@pytest.mark.asyncio
async def test_router_failure_does_not_drop_the_parents_other_callbacks(state_manager_real_ta):
    """
    A broken router must not take the parent's unrelated continuations down with it.

    Real-backend test: the regression is that the RouterError escaped post_process before
    the has_callbacks() block ran, so the sibling was silently never submitted.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path", "unrelated")

    @register_router(name="route_by_size", version=0)
    def route_by_size(results, *, threshold):
        raise RuntimeError("boom")

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    sibling = DAGNode("unrelated").to_spec()
    task.dag_callbacks = list(task.dag_callbacks) + [SimpleCallback(task=sibling)]

    await TaskProcessor(state_manager_real_ta).post_process(task)

    submitted = await state_manager_real_ta.task_state.get_task(sibling.id)
    assert submitted is not None, "the parent's unrelated callback must still fire"
    assert submitted.name == "unrelated"


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


# ── placeholder: fan-in handover, fallback, resume ────────────────────────────

FAN_IN_AROUND_ROUTER = """
flowchart TD
    A["measure_payload"]
    R{"route_by_size"}
    B["fast_path"]
    C["slow_path"]
    X["sibling"]
    Z["collect"]

    A --> R
    R --> B
    R --> C
    A --> Z
    X --> Z
"""

FALLBACK_ROUTER = """
flowchart TD
    A["measure_payload"]
    R{"route_by_size"}
    B["fast_path"]
    C["slow_path"]
    E["degraded_path"]

    A --> R
    R --> B
    R --> C
    R -.-> E
"""

FALLBACK_WITH_FAN_IN = """
flowchart TD
    A["measure_payload"]
    R{"route_by_size"}
    B["fast_path"]
    C["slow_path"]
    E["degraded_path"]
    X["sibling"]
    Z["collect"]

    A --> R
    R --> B
    R --> C
    R -.-> E
    A --> Z
    X --> Z
"""


def _boom_router(name: str = "route_by_size") -> None:
    @register_router(name=name, version=0)
    def router(results, **_kwargs):
        raise RuntimeError("boom")


async def _size_fan_in(sm, task: Task, cb: FanInCallback) -> None:
    """Populate the fan-in set the way the submission of both predecessors would have."""
    await sm.init_fan_in(task.dag_run_id, cb.fan_in_key, {task.id, ULID()})


@pytest.mark.asyncio
async def test_halt_placeholder_takes_over_the_parents_fan_in(state_manager_real_ta):
    """
    The placeholder inherits the fan-in obligation, so the collector waits on it.

    Real-backend test: this is about actual fan-in set membership. A stub would only show
    that delegate_fan_in was called, not that the collector is now blocked on the right id
    and that the parent did not discharge the obligation on its way past.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path", "sibling", "collect")
    _boom_router()

    task = _root_task(FAN_IN_AROUND_ROUTER, results={})
    fan_in_cb = next(c for c in task.dag_callbacks if isinstance(c, FanInCallback))
    router_cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await _size_fan_in(state_manager_real_ta, task, fan_in_cb)

    await TaskProcessor(state_manager_real_ta).post_process(task)

    members = await state_manager_real_ta.task_state.get_fan_in_members(task.dag_run_id, fan_in_cb.fan_in_key)
    assert router_cb.router.id in members, "the collector must now wait on the placeholder"
    assert task.id not in members, "the parent handed its obligation over"
    assert await state_manager_real_ta.task_state.get_task(fan_in_cb.task.id) is None, (
        "the collector must not have fired early"
    )


@pytest.mark.asyncio
async def test_completing_the_placeholder_discharges_the_inherited_fan_in(state_manager_real_ta):
    """
    The placeholder carries the FanInCallbacks, so ordinary post_process closes them.

    This is why the rerun_router handler needs no fan-in bookkeeping of its own.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path", "sibling", "collect")
    _boom_router()

    task = _root_task(FAN_IN_AROUND_ROUTER, results={})
    fan_in_cb = next(c for c in task.dag_callbacks if isinstance(c, FanInCallback))
    router_cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    # Only the parent is pending, so discharging its obligation should fire the collector.
    await state_manager_real_ta.init_fan_in(task.dag_run_id, fan_in_cb.fan_in_key, {task.id})
    await TaskProcessor(state_manager_real_ta).post_process(task)

    placeholder = await state_manager_real_ta.task_state.get_task(router_cb.router.id)
    assert placeholder is not None
    assert [c.fan_in_key for c in placeholder.dag_callbacks if isinstance(c, FanInCallback)] == [
        fan_in_cb.fan_in_key
    ]

    placeholder.set_status(TaskStatus.COMPLETED)
    await TaskProcessor(state_manager_real_ta).post_process(placeholder)

    collector = await state_manager_real_ta.task_state.get_task(fan_in_cb.task.id)
    assert collector is not None, "the collector fires once the placeholder resolves"
    assert collector.name == "collect"


@pytest.mark.asyncio
async def test_fallback_submits_the_degraded_path_and_completes_the_placeholder(
    state_manager_real_ta,
):
    """A declared dotted edge means continue degraded rather than halt."""
    _register_tasks("measure_payload", "fast_path", "slow_path", "degraded_path")
    _boom_router()

    task = _root_task(FALLBACK_ROUTER, results={})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    assert cb.error_callback is not None

    await TaskProcessor(state_manager_real_ta).post_process(task)

    fallback = await state_manager_real_ta.task_state.get_task(cb.error_callback.id)
    assert fallback is not None, "the degraded path must be submitted"
    assert fallback.name == "degraded_path"

    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None
    # COMPLETED, not FAILED: every non-COMPLETED terminal status is in stuck_statuses()
    # and would claim the run needs operator intervention when it does not.
    assert placeholder.status == TaskStatus.COMPLETED
    assert any("boom" in e for e in placeholder.errors)


@pytest.mark.asyncio
async def test_fallback_node_takes_over_the_fan_in_not_the_placeholder(state_manager_real_ta):
    """Under FALLBACK the degraded node is the successor, so it inherits the obligation."""
    _register_tasks("measure_payload", "fast_path", "slow_path", "degraded_path", "sibling", "collect")
    _boom_router()

    task = _root_task(FALLBACK_WITH_FAN_IN, results={})
    fan_in_cb = next(c for c in task.dag_callbacks if isinstance(c, FanInCallback))
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    assert cb.error_callback is not None
    await _size_fan_in(state_manager_real_ta, task, fan_in_cb)

    await TaskProcessor(state_manager_real_ta).post_process(task)

    members = await state_manager_real_ta.task_state.get_fan_in_members(task.dag_run_id, fan_in_cb.fan_in_key)
    assert cb.error_callback.id in members, "the degraded node is the successor"
    assert cb.router.id not in members, "the inert placeholder is not waited on"


@pytest.mark.asyncio
async def test_resuming_the_placeholder_routes_with_the_fixed_router(state_manager_real_ta, monkeypatch):
    """
    The point of the placeholder: deploy a fix, resume, and the routing happens.

    The parent is never re-run and the candidate keeps its pre-assigned id, so the live
    diagram still lines up with what was submitted.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path")
    calls: list[str] = []

    @register_router(name="route_by_size", version=0)
    def broken(results, **_kwargs):
        calls.append("broken")
        raise RuntimeError("boom")

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    await state_manager_real_ta.task_state.save_task(task)
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)
    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None
    assert placeholder.status == TaskStatus.FAILED

    # "Deploy the fix", then run what a resume would run.
    reset_registry()
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def fixed(results, **_kwargs):
        calls.append("fixed")
        return "fast_path"

    monkeypatch.setattr(db, "get_state_manager", lambda: state_manager_real_ta)
    result = await rerun_router(**placeholder.parameters)

    chosen = next(c for c in cb.router.candidates if c.name == "fast_path")
    assert result["routed"] == str(chosen.id)
    submitted = await state_manager_real_ta.task_state.get_task(chosen.id)
    assert submitted is not None, "the candidate keeps its pre-assigned id"
    assert submitted.name == "fast_path"
    assert calls == ["broken", "fixed"], "the parent task itself was never re-run"


@pytest.mark.asyncio
async def test_placeholder_joins_the_dag_run_so_it_is_resumable(state_manager_real_ta):
    """
    The placeholder must be in the run task index, or can_resume_dag_run cannot see it.

    Real-backend test: run membership comes from the enqueue path, which a task created
    directly in a terminal status never takes.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path")
    _boom_router()

    task = _root_task(SIMPLE_ROUTER, results={})
    await state_manager_real_ta.submit_tasks_batch([task])
    task.set_status(TaskStatus.COMPLETED)
    await TaskProcessor(state_manager_real_ta).post_process(task)

    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    detail = await state_manager_real_ta.task_state.get_dag_run(task.dag_run_id)
    assert detail is not None
    assert cb.router.id in detail.task_ids, "a run that cannot see the placeholder cannot resume it"

    precheck = await state_manager_real_ta.can_resume_dag_run(task.dag_run_id)
    assert precheck.resumable, precheck.reason
    assert cb.router.id in precheck.stuck_task_ids


@pytest.mark.asyncio
async def test_rerun_router_declining_submits_nothing(state_manager_real_ta, monkeypatch):
    """A fixed router may legitimately decide the right answer is to route nowhere."""
    _register_tasks("measure_payload", "fast_path", "slow_path")
    _boom_router()

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    await state_manager_real_ta.task_state.save_task(task)
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)
    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None

    reset_registry()
    _register_tasks("measure_payload", "fast_path", "slow_path")

    @register_router(name="route_by_size", version=0)
    def declines(results, **_kwargs):
        return None

    monkeypatch.setattr(db, "get_state_manager", lambda: state_manager_real_ta)
    assert await rerun_router(**placeholder.parameters) == {"routed": None}
    for candidate in cb.router.candidates:
        assert await state_manager_real_ta.task_state.get_task(candidate.id) is None


@pytest.mark.asyncio
async def test_rerun_router_fails_clearly_when_the_parent_has_aged_out(state_manager_real_ta, monkeypatch):
    """
    Resuming after the parent blob is swept cannot work, and says so.

    The router needs the parent results to route on, and the Cleaner prunes terminal tasks
    on ``completed_task_age``. No router fix recovers this; the run has to be re-submitted.
    """
    _register_tasks("measure_payload", "fast_path", "slow_path")
    _boom_router()

    task = _root_task(SIMPLE_ROUTER, results={"bytes": 10})
    cb = next(c for c in task.dag_callbacks if isinstance(c, RouterCallback))
    await TaskProcessor(state_manager_real_ta)._handle_router(task, cb)
    placeholder = await state_manager_real_ta.task_state.get_task(cb.router.id)
    assert placeholder is not None

    # The parent was never saved here, which is what an aged-out blob looks like.
    monkeypatch.setattr(db, "get_state_manager", lambda: state_manager_real_ta)
    with pytest.raises(RouterError, match="no longer exists"):
        await rerun_router(**placeholder.parameters)
