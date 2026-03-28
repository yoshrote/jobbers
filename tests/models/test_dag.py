"""Tests for DAGTaskSpec.fresh_copy and collect_fan_in_keys."""

import pytest
from ulid import ULID

from jobbers.models.dag import DAGTaskSpec, FanInCallback, SimpleCallback, collect_fan_in_keys


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
