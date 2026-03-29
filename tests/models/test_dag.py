"""Tests for DAGTaskSpec.fresh_copy, collect_fan_in_keys, DAGNode, and TaskResult."""

from ulid import ULID

from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOut,
    FanInCallback,
    SimpleCallback,
    TaskResult,
    collect_fan_in_keys,
)


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
    fresh_branch = fresh_root.dag_callbacks[0].task  # type: ignore[union-attr]
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
    fresh_b1 = fresh.dag_callbacks[0].task  # type: ignore[union-attr]
    fresh_b2 = fresh.dag_callbacks[1].task  # type: ignore[union-attr]
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
    fanout = DynamicFanOut(children=children, collector=collector)
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
