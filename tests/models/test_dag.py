"""Tests for DAGTaskSpec.fresh_copy, collect_fan_in_keys, DAGNode, and TaskResult."""

from typing import Annotated

import pytest
from ulid import ULID

from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOut,
    DynamicFanOutCallback,
    FanInCallback,
    FanInCardinalityError,
    FromParent,
    SimpleCallback,
    TaskResult,
    collect_fan_in_keys,
)
from jobbers.registry import clear_registry, register_task


def make_spec(name: str = "task", **kwargs) -> DAGTaskSpec:
    return DAGTaskSpec(name=name, **kwargs)


# ── fresh_copy: basic ─────────────────────────────────────────────────────────


def test_fresh_copy_leaf_gets_new_id():
    spec = make_spec()
    fresh, id_map = spec.fresh_copy()
    assert fresh.id != spec.id
    assert id_map[spec.id] == fresh.id


def test_fresh_copy_preserves_fields():
    spec = make_spec(name="my_task", queue="my_queue", version=3, parameters={"k": "v"})
    fresh, _ = spec.fresh_copy()
    assert fresh.name == "my_task"
    assert fresh.queue == "my_queue"
    assert fresh.version == 3
    assert fresh.parameters == {"k": "v"}


def test_fresh_copy_leaf_has_no_callbacks():
    spec = make_spec()
    fresh, _ = spec.fresh_copy()
    assert fresh.dag_callbacks == []


def test_fresh_copy_is_independent_each_call():
    spec = make_spec()
    fresh1, _ = spec.fresh_copy()
    fresh2, _ = spec.fresh_copy()
    assert fresh1.id != fresh2.id


# ── fresh_copy: linear chain ──────────────────────────────────────────────────


def test_fresh_copy_linear_chain_remaps_child_id():
    child = make_spec("child")
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=child)])
    fresh, id_map = root.fresh_copy()

    assert fresh.id != root.id
    cb = fresh.dag_callbacks[0]
    assert isinstance(cb, SimpleCallback)
    assert cb.task.id != child.id
    assert id_map[child.id] == cb.task.id


def test_fresh_copy_linear_chain_no_shared_ids():
    child = make_spec("child")
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=child)])
    fresh, id_map = root.fresh_copy()

    old_ids = {root.id, child.id}
    new_ids = set(id_map.values())
    assert old_ids.isdisjoint(new_ids)


# ── fresh_copy: fan-in ────────────────────────────────────────────────────────


def _make_fan_in_dag() -> tuple[DAGTaskSpec, DAGTaskSpec, DAGTaskSpec]:
    """Return (root, branch, collector) with branch fan-in to collector."""
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    branch = DAGTaskSpec(
        name="branch",
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)],
    )
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=branch)])
    return root, branch, collector


def test_fresh_copy_fan_in_rewrites_fan_in_key():
    root, branch, collector = _make_fan_in_dag()
    fresh_root, id_map = root.fresh_copy()

    new_collector_id = id_map[collector.id]
    fresh_branch = fresh_root.dag_callbacks[0].task
    assert isinstance(fresh_branch.dag_callbacks[0], FanInCallback)
    assert fresh_branch.dag_callbacks[0].fan_in_key == f"dag:fan-in:{new_collector_id}"


def test_fresh_copy_fan_in_no_shared_ids():
    root, branch, collector = _make_fan_in_dag()
    _, id_map = root.fresh_copy()

    old_ids = {root.id, branch.id, collector.id}
    assert old_ids.isdisjoint(set(id_map.values()))


def test_fresh_copy_fan_in_two_predecessors_same_new_collector():
    """Two predecessors pointing to the same collector must share the new collector ID."""
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    b1 = DAGTaskSpec(name="b1", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    b2 = DAGTaskSpec(name="b2", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[SimpleCallback(task=b1), SimpleCallback(task=b2)],
    )
    fresh, id_map = root.fresh_copy()

    new_collector_id = id_map[collector.id]
    # Both branches' fan_in_key should reference the same new collector ID
    fresh_b1 = fresh.dag_callbacks[0].task
    fresh_b2 = fresh.dag_callbacks[1].task
    assert fresh_b1.dag_callbacks[0].fan_in_key == f"dag:fan-in:{new_collector_id}"  # type: ignore[union-attr]
    assert fresh_b2.dag_callbacks[0].fan_in_key == f"dag:fan-in:{new_collector_id}"  # type: ignore[union-attr]


# ── collect_fan_in_keys ───────────────────────────────────────────────────────


def test_collect_fan_in_keys_no_callbacks():
    spec = make_spec()
    assert collect_fan_in_keys(spec) == {}


def test_collect_fan_in_keys_simple_chain_has_no_fan_in():
    child = make_spec("child")
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=child)])
    assert collect_fan_in_keys(root) == {}


def test_collect_fan_in_keys_single_predecessor():
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    predecessor = DAGTaskSpec(
        name="pred",
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)],
    )
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=predecessor)])
    result = collect_fan_in_keys(root)
    assert result == {fan_key: {predecessor.id}}


def test_collect_fan_in_keys_two_predecessors():
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    b1 = DAGTaskSpec(name="b1", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    b2 = DAGTaskSpec(name="b2", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[SimpleCallback(task=b1), SimpleCallback(task=b2)],
    )
    result = collect_fan_in_keys(root)
    assert result == {fan_key: {b1.id, b2.id}}


def test_collect_fan_in_keys_visited_guard_handles_diamond():
    """collect_fan_in_keys should not double-count predecessors in a diamond pattern."""
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    b = DAGTaskSpec(name="branch", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    # root points to branch twice (unusual but should not double-count)
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[SimpleCallback(task=b), SimpleCallback(task=b)],
    )
    result = collect_fan_in_keys(root)
    # branch appears twice in root callbacks but visit guard should make id appear once
    assert result[fan_key] == {b.id}


def test_fresh_copy_then_collect_fan_in_keys_no_old_ids():
    """After fresh_copy, collect_fan_in_keys should reference only new ULIDs."""
    root, branch, collector = _make_fan_in_dag()
    fresh, id_map = root.fresh_copy()
    fan_ins = collect_fan_in_keys(fresh)

    old_predecessor_id = branch.id
    new_predecessor_id = id_map[branch.id]
    new_collector_id = id_map[collector.id]

    assert old_predecessor_id not in next(iter(fan_ins.values()))
    assert new_predecessor_id in fan_ins[f"dag:fan-in:{new_collector_id}"]


def test_collect_fan_in_keys_walks_into_dynamic_fanout_collector():
    """
    A static fan-in reachable through a dynamic fan-out's collector must be pre-populated.

    Regression test: the old code `continue`d past every DynamicFanOutCallback
    without ever visiting cb.collector, so any static fan-in downstream of a
    dynamic-fanout collector (e.g. `collector --> grand_collector` in a mermaid
    diagram) was under-counted at DAG-submission time — that fan-in could then
    fire before the collector's own branch actually completed.
    """
    grand_collector = make_spec("grand_collector")
    fan_key = f"dag:fan-in:{grand_collector.id}"
    collector = DAGTaskSpec(
        name="collector",
        dag_callbacks=[FanInCallback(task=grand_collector, fan_in_key=fan_key)],
    )
    arm_root = make_spec("arm_root")
    fanout_cb = DynamicFanOutCallback(arm_root=arm_root, collector=collector, items_key="items")
    dispatcher = DAGTaskSpec(name="dispatcher", dag_callbacks=[fanout_cb])

    result = collect_fan_in_keys(dispatcher)
    assert result == {fan_key: {collector.id}}


def test_collect_fan_in_keys_does_not_walk_into_arm_root():
    """Arm-root fan-ins are intentionally NOT pre-populated — initialised at runtime by the processor."""
    inner_collector = make_spec("inner_collector")
    inner_fan_key = f"dag:fan-in:{inner_collector.id}"
    arm_step = DAGTaskSpec(
        name="arm_step",
        dag_callbacks=[FanInCallback(task=inner_collector, fan_in_key=inner_fan_key)],
    )
    arm_root = DAGTaskSpec(name="arm_root", dag_callbacks=[SimpleCallback(task=arm_step)])
    collector = make_spec("collector")
    fanout_cb = DynamicFanOutCallback(arm_root=arm_root, collector=collector, items_key="items")
    dispatcher = DAGTaskSpec(name="dispatcher", dag_callbacks=[fanout_cb])

    result = collect_fan_in_keys(dispatcher)
    assert inner_fan_key not in result


def test_dag_node_fan_in_predecessors_walks_into_fanout_collector():
    """
    DAGNode.fan_in_predecessors must include fan-ins reachable through a fan-out collector.

    A DynamicFanOutCallback's collector is a fully-resolved DAGTaskSpec subtree
    (attached via add_fanout_callback), not a DAGNode, so fan_in_predecessors has
    to delegate to collect_fan_in_keys for it rather than only walking _successors.
    """
    grand_collector = make_spec("grand_collector")
    fan_key = f"dag:fan-in:{grand_collector.id}"
    collector_spec = DAGTaskSpec(
        name="collector",
        dag_callbacks=[FanInCallback(task=grand_collector, fan_in_key=fan_key)],
    )
    arm_root_spec = make_spec("arm_root")
    fanout_cb = DynamicFanOutCallback(arm_root=arm_root_spec, collector=collector_spec, items_key="items")

    dispatcher_node = DAGNode("dispatcher")
    dispatcher_node.add_fanout_callback(fanout_cb)

    preds = dispatcher_node.fan_in_predecessors()
    assert preds == {fan_key: {collector_spec.id}}


# ── TaskResult ────────────────────────────────────────────────────────────────


def test_task_result_defaults():
    """TaskResult defaults to empty results, no fanout."""
    tr = TaskResult()
    assert tr.results == {}
    assert tr.fanout is None


def test_task_result_with_results():
    tr = TaskResult(results={"count": 5})
    assert tr.results == {"count": 5}


def test_task_result_with_fanout():
    children = [DAGNode("child")]
    collector = DAGNode("collect")
    fanout = DynamicFanOut(arms=children, collector=collector)
    tr = TaskResult(results={}, fanout=fanout)
    assert tr.fanout is fanout


# ── DAGNode.to_task ───────────────────────────────────────────────────────────


def test_dag_node_to_task_no_parent():
    """to_task() with no parent creates a root task with empty parent_ids."""
    node = DAGNode("fetch_data", queue="urgent", version=2, parameters={"k": "v"})
    task = node.to_task()

    assert task.id == node.id
    assert task.name == "fetch_data"
    assert task.queue == "urgent"
    assert task.version == 2
    assert task.parameters == {"k": "v"}
    assert task.parent_ids == []
    assert task.dag_callbacks == []


def test_dag_node_to_task_with_parent():
    """to_task(parent_id=...) sets parent_ids on the returned task."""
    parent_id = ULID()
    node = DAGNode("process")
    task = node.to_task(parent_id=parent_id)

    assert task.parent_ids == [parent_id]


def test_dag_node_to_task_carries_callbacks():
    """to_task() embeds the callback chain for downstream execution."""
    child = DAGNode("child")
    root = DAGNode("root")
    root.then(child)

    task = root.to_task()
    assert len(task.dag_callbacks) == 1
    assert isinstance(task.dag_callbacks[0], SimpleCallback)
    assert task.dag_callbacks[0].task.id == child.id


def test_dag_node_to_task_fan_in_callback_embedded():
    """to_task() correctly embeds FanInCallback when merge() was used."""
    collector = DAGNode("collector")
    branch = DAGNode("branch")
    DAGNode.merge(branch, into=collector)

    task = branch.to_task()
    assert len(task.dag_callbacks) == 1
    assert isinstance(task.dag_callbacks[0], FanInCallback)
    assert task.dag_callbacks[0].task.id == collector.id


def test_dag_node_to_task_id_matches_node_id():
    """The task's ULID must match the pre-assigned node ULID."""
    node = DAGNode("task_a")
    task = node.to_task()
    assert task.id == node.id


def test_dag_node_fan_in_predecessors_single():
    """fan_in_predecessors() returns the collector key mapped to the branch's ID."""
    collector = DAGNode("collector")
    branch = DAGNode("branch")
    DAGNode.merge(branch, into=collector)

    preds = branch.fan_in_predecessors()
    expected_key = f"dag:fan-in:{collector.id}"
    assert expected_key in preds
    assert branch.id in preds[expected_key]


def test_dag_node_fan_in_predecessors_multiple():
    """fan_in_predecessors() collects both branches for a two-predecessor merge."""
    collector = DAGNode("collector")
    b1 = DAGNode("branch1")
    b2 = DAGNode("branch2")
    root = DAGNode("root")
    root.then(b1, b2)
    DAGNode.merge(b1, b2, into=collector)

    preds = root.fan_in_predecessors()
    expected_key = f"dag:fan-in:{collector.id}"
    assert preds[expected_key] == {b1.id, b2.id}


def test_dag_node_fan_in_predecessors_chain_before_merge():
    """fan_in_predecessors walks the full subgraph, so chains before a merge are traversed correctly."""
    # root → step_1 → b1 \
    #      → step_2 → b2 / → collector
    # The fan-in edge is on b1 and b2 (direct predecessors of collector).
    collector = DAGNode("collector")
    b1 = DAGNode("b1")
    b2 = DAGNode("b2")
    step1 = DAGNode("step1")
    step2 = DAGNode("step2")
    root = DAGNode("root")
    step1.then(b1)
    step2.then(b2)
    root.then(step1, step2)
    DAGNode.merge(b1, b2, into=collector)

    preds = root.fan_in_predecessors()
    expected_key = f"dag:fan-in:{collector.id}"
    assert preds[expected_key] == {b1.id, b2.id}


# ── DAGNode.find_terminals ────────────────────────────────────────────────────


def test_find_terminals_single_node():
    """A node with no successors is its own terminal."""
    node = DAGNode("task")
    assert DAGNode.find_terminals([node]) == [node]


def test_find_terminals_linear_chain():
    """find_terminals follows .then() chains to the leaf node."""
    root = DAGNode("a")
    mid = DAGNode("b")
    leaf = DAGNode("c")
    root.then(mid.then(leaf))
    assert DAGNode.find_terminals([root]) == [leaf]


def test_find_terminals_diamond():
    """find_terminals resolves to the merge target for a diamond sub-graph."""
    root = DAGNode("root")
    b1 = DAGNode("branch_1")
    b2 = DAGNode("branch_2")
    merger = DAGNode("merge_node")
    root.then(b1, b2)
    DAGNode.merge(b1, b2, into=merger)
    assert DAGNode.find_terminals([root]) == [merger]


def test_find_terminals_multiple_roots():
    """Each root contributes its own terminal(s) without cross-contamination."""
    root_a = DAGNode("a")
    leaf_a = DAGNode("a_leaf")
    root_a.then(leaf_a)
    root_b = DAGNode("b")  # single-step arm
    terminals = DAGNode.find_terminals([root_a, root_b])
    assert {t.id for t in terminals} == {leaf_a.id, root_b.id}


def test_find_terminals_deduplicates_shared_merge_target():
    """Two branches that converge on the same merge node produce one terminal entry."""
    b1 = DAGNode("b1")
    b2 = DAGNode("b2")
    merger = DAGNode("merger")
    root = DAGNode("root")
    root.then(b1, b2)
    DAGNode.merge(b1, b2, into=merger)
    terminals = DAGNode.find_terminals([root])
    assert terminals == [merger]


def test_find_terminals_treats_nested_dispatcher_as_its_own_terminal():
    """
    An arm that is itself a nested dispatcher is intentionally its own terminal here.

    This is by design, not a gap: the outer collector's FanInCallback is wired to this
    node provisionally; if the nested DynamicFanOutCallback's own propagate_fan_in is
    True (the default), _handle_declarative_fanout delegates that FanInCallback to the
    nested collector at runtime once this node actually executes and reveals its nested
    fan-out. If propagate_fan_in is False, this node completing — not its nested tree
    completing — is exactly what the caller asked to close the outer fan-in.
    """
    dispatcher = DAGNode("dispatcher")
    dispatcher.add_fanout_callback(
        DynamicFanOutCallback(
            arm_root=make_spec("inner_arm"),
            collector=make_spec("inner_collector"),
            items_key="items",
        )
    )
    assert DAGNode.find_terminals([dispatcher]) == [dispatcher]


# ── error callbacks ───────────────────────────────────────────────────────────


def test_simple_callback_error_callback_defaults_none():
    """SimpleCallback.error_callback is None by default."""
    spec = make_spec("child")
    cb = SimpleCallback(task=spec)
    assert cb.error_callback is None


def test_fan_in_callback_error_callback_defaults_none():
    """FanInCallback.error_callback is None by default."""
    collector = make_spec("collector")
    cb = FanInCallback(task=collector, fan_in_key=f"dag:fan-in:{collector.id}")
    assert cb.error_callback is None


def test_dag_node_then_on_error_embeds_error_spec():
    """then(on_error=...) stores the error callback spec in the SimpleCallback."""
    child = DAGNode("child")
    err = DAGNode("on_error")
    root = DAGNode("root")
    root.then(child, on_error=err)

    task = root.to_task()
    assert len(task.dag_callbacks) == 1
    cb = task.dag_callbacks[0]
    assert isinstance(cb, SimpleCallback)
    assert cb.error_callback is not None
    assert cb.error_callback.id == err.id
    assert cb.error_callback.name == "on_error"


def test_dag_node_then_without_on_error_has_no_error_callback():
    """then() with no on_error leaves error_callback as None."""
    child = DAGNode("child")
    root = DAGNode("root")
    root.then(child)

    task = root.to_task()
    assert task.dag_callbacks[0].error_callback is None


def test_dag_node_merge_on_error_embeds_error_spec():
    """merge(on_error=...) stores the error callback spec in each FanInCallback."""
    collector = DAGNode("collector")
    b1 = DAGNode("branch1")
    b2 = DAGNode("branch2")
    err = DAGNode("on_error")
    DAGNode.merge(b1, b2, into=collector, on_error=err)

    task_b1 = b1.to_task()
    task_b2 = b2.to_task()

    assert isinstance(task_b1.dag_callbacks[0], FanInCallback)
    assert task_b1.dag_callbacks[0].error_callback is not None
    assert task_b1.dag_callbacks[0].error_callback.id == err.id

    assert isinstance(task_b2.dag_callbacks[0], FanInCallback)
    assert task_b2.dag_callbacks[0].error_callback is not None
    assert task_b2.dag_callbacks[0].error_callback.id == err.id


def test_dag_node_merge_without_on_error_has_no_error_callback():
    """merge() with no on_error leaves error_callback as None."""
    collector = DAGNode("collector")
    branch = DAGNode("branch")
    DAGNode.merge(branch, into=collector)

    task = branch.to_task()
    assert task.dag_callbacks[0].error_callback is None


def test_fresh_copy_remaps_error_callback_id():
    """_remap remaps the error_callback ULID so cron runs never share keys."""
    err_spec = make_spec("err_handler")
    child_spec = make_spec("child")
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[SimpleCallback(task=child_spec, error_callback=err_spec)],
    )
    fresh, id_map = root.fresh_copy()

    cb = fresh.dag_callbacks[0]
    assert isinstance(cb, SimpleCallback)
    assert cb.error_callback is not None
    assert cb.error_callback.id != err_spec.id
    assert id_map[err_spec.id] == cb.error_callback.id


def test_fresh_copy_fan_in_remaps_error_callback_id():
    """_remap also remaps error_callback inside FanInCallback."""
    err_spec = make_spec("err_handler")
    collector = make_spec("collector")
    fan_key = f"dag:fan-in:{collector.id}"
    branch = DAGTaskSpec(
        name="branch",
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key, error_callback=err_spec)],
    )
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=branch)])
    fresh, id_map = root.fresh_copy()

    fresh_branch = fresh.dag_callbacks[0].task
    fan_in_cb = fresh_branch.dag_callbacks[0]
    assert isinstance(fan_in_cb, FanInCallback)
    assert fan_in_cb.error_callback is not None
    assert fan_in_cb.error_callback.id != err_spec.id
    assert id_map[err_spec.id] == fan_in_cb.error_callback.id


def test_fresh_copy_none_error_callback_stays_none():
    """_remap leaves error_callback as None when not set."""
    child_spec = make_spec("child")
    root = DAGTaskSpec(name="root", dag_callbacks=[SimpleCallback(task=child_spec)])
    fresh, _ = root.fresh_copy()

    cb = fresh.dag_callbacks[0]
    assert cb.error_callback is None


# ── FromParent ────────────────────────────────────────────────────────────────


def test_from_parent_defaults():
    """FromParent() with no args defaults key to None and many to False."""
    fp = FromParent()
    assert fp.key is None
    assert fp.many is False


def test_from_parent_stores_key_and_many():
    fp = FromParent("rows", many=True)
    assert fp.key == "rows"
    assert fp.many is True


# ── DAGNode.merge: FanInCardinalityError ─────────────────────────────────────


@pytest.fixture
def register_collector():
    """Register a "collector" task and clean it up afterward."""

    def _register(fn):
        register_task(name="collector", version=0)(fn)
        return fn

    yield _register
    clear_registry()


def test_merge_raises_for_singular_from_parent_on_collector(register_collector):
    """merge() always supplies 2+ parents, so a singular FromParent param on `into` always errors."""

    async def collector_fn(rows: Annotated[int, FromParent("rows")], **kwargs): ...

    register_collector(collector_fn)

    b1 = DAGNode("branch1")
    b2 = DAGNode("branch2")
    collector = DAGNode("collector")

    with pytest.raises(FanInCardinalityError, match="singular FromParent"):
        DAGNode.merge(b1, b2, into=collector)


def test_merge_raises_regardless_of_predecessor_task_types(register_collector):
    """The check doesn't care whether predecessors are the same or different task types."""

    async def collector_fn(rows: Annotated[int, FromParent("rows")], **kwargs): ...

    register_collector(collector_fn)

    b1 = DAGNode("fetch_a")
    b2 = DAGNode("fetch_b")  # distinct task name from b1 -- still always invalid
    collector = DAGNode("collector")

    with pytest.raises(FanInCardinalityError):
        DAGNode.merge(b1, b2, into=collector)


def test_merge_allows_many_true_from_parent(register_collector):
    """A many=True FromParent param on the collector is exactly what merge() expects."""

    async def collector_fn(rows: Annotated[list[int], FromParent("rows", many=True)], **kwargs): ...

    register_collector(collector_fn)

    b1 = DAGNode("branch1")
    b2 = DAGNode("branch2")
    collector = DAGNode("collector")

    merged = DAGNode.merge(b1, b2, into=collector)
    assert merged is collector


def test_merge_skips_validation_for_unregistered_task():
    """merge() doesn't raise when `into`'s task isn't registered -- can't validate, so it's skipped."""
    b1 = DAGNode("branch1")
    b2 = DAGNode("branch2")
    collector = DAGNode("never_registered_collector")

    merged = DAGNode.merge(b1, b2, into=collector)
    assert merged is collector


def test_merge_single_predecessor_never_raises(register_collector):
    """A single-predecessor merge() call can never violate the singular FromParent contract."""

    async def collector_fn(rows: Annotated[int, FromParent("rows")], **kwargs): ...

    register_collector(collector_fn)

    branch = DAGNode("branch1")
    collector = DAGNode("collector")

    merged = DAGNode.merge(branch, into=collector)
    assert merged is collector
