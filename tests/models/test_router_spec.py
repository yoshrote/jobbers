"""Model tests for RouterSpec / RouterCallback and their effect on DAG bookkeeping."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOutCallback,
    FanInCallback,
    RouterCallback,
    RouterSpec,
    SimpleCallback,
    collect_fan_in_keys,
)


def _router_tree() -> DAGTaskSpec:
    """Build a root whose RouterCallback offers two candidates, one with a downstream chain."""
    tail = DAGTaskSpec(name="tail")
    fast = DAGTaskSpec(name="fast_path", dag_callbacks=[SimpleCallback(task=tail)])
    slow = DAGTaskSpec(name="slow_path", lane="heavy")
    err = DAGTaskSpec(name="notify_bad_route")
    return DAGTaskSpec(
        name="root",
        dag_callbacks=[
            RouterCallback(
                router=RouterSpec(router="route", parameters={"threshold": 5}, candidates=[fast, slow]),
                error_callback=err,
            )
        ],
    )


# ── fresh_copy / _remap ───────────────────────────────────────────────────────


def test_fresh_copy_remaps_router_and_candidate_ids():
    spec = _router_tree()
    original = spec.dag_callbacks[0]
    fresh, id_map = spec.fresh_copy()
    copied = fresh.dag_callbacks[0]

    assert isinstance(copied, RouterCallback)
    assert copied.router.id != original.router.id
    assert id_map[original.router.id] == copied.router.id
    for before, after in zip(original.router.candidates, copied.router.candidates):
        assert after.id != before.id
        assert id_map[before.id] == after.id


def test_fresh_copy_preserves_router_payload():
    spec = _router_tree()
    fresh, _ = spec.fresh_copy()
    cb = fresh.dag_callbacks[0]
    assert cb.router.router == "route"
    assert cb.router.parameters == {"threshold": 5}
    assert [(c.name, c.lane) for c in cb.router.candidates] == [
        ("fast_path", "default"),
        ("slow_path", "heavy"),
    ]


def test_fresh_copy_remaps_candidate_subtrees_and_error_callback():
    spec = _router_tree()
    original = spec.dag_callbacks[0]
    fresh, _ = spec.fresh_copy()
    copied = fresh.dag_callbacks[0]

    old_tail = original.router.candidates[0].dag_callbacks[0].task
    new_tail = copied.router.candidates[0].dag_callbacks[0].task
    assert new_tail.name == "tail"
    assert new_tail.id != old_tail.id

    assert copied.error_callback is not None
    assert copied.error_callback.id != original.error_callback.id


def test_fresh_copy_shares_no_ids_with_the_original():
    spec = _router_tree()
    _, id_map = spec.fresh_copy()
    assert set(id_map).isdisjoint(set(id_map.values()))


# ── collect_fan_in_keys ───────────────────────────────────────────────────────


def test_collect_fan_in_keys_skips_router_candidate_subtrees():
    """
    Only one candidate ever runs, so its fan-in sets must not be pre-populated.

    Pre-populating them at submit time would leave the collector of an unchosen
    branch waiting on predecessors that never execute; the processor initialises
    the chosen branch's sets at routing time instead.
    """
    collector = DAGTaskSpec(name="collector")
    fan_key = f"dag:fan-in:{collector.id}"
    b1 = DAGTaskSpec(name="b1", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    b2 = DAGTaskSpec(name="b2", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    branch = DAGTaskSpec(
        name="branch",
        dag_callbacks=[SimpleCallback(task=b1), SimpleCallback(task=b2)],
    )
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[RouterCallback(router=RouterSpec(router="route", candidates=[branch]))],
    )
    assert collect_fan_in_keys(root) == {}


def test_collect_fan_in_keys_still_walks_non_router_siblings():
    """A router callback must not mask an ordinary fan-in elsewhere on the same node."""
    collector = DAGTaskSpec(name="collector")
    fan_key = f"dag:fan-in:{collector.id}"
    pred = DAGTaskSpec(name="pred", dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)])
    root = DAGTaskSpec(
        name="root",
        dag_callbacks=[
            RouterCallback(router=RouterSpec(router="route", candidates=[DAGTaskSpec(name="x")])),
            SimpleCallback(task=pred),
        ],
    )
    assert collect_fan_in_keys(root) == {fan_key: {pred.id}}


def test_dag_node_fan_in_predecessors_skips_router_branches():
    """The DAGNode-side walk mirrors collect_fan_in_keys' router exclusion."""
    collector = DAGTaskSpec(name="collector")
    fan_key = f"dag:fan-in:{collector.id}"
    branch = DAGTaskSpec(
        name="branch",
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_key)],
    )
    node = DAGNode("root")
    node.add_router_callback(RouterCallback(router=RouterSpec(router="route", candidates=[branch])))
    assert node.fan_in_predecessors() == {}


# ── DAGNode wiring ────────────────────────────────────────────────────────────


def test_add_router_callback_appears_in_the_spec():
    node = DAGNode("root")
    cb = RouterCallback(router=RouterSpec(router="route", candidates=[DAGTaskSpec(name="b")]))
    node.add_router_callback(cb)
    spec = node.to_spec()
    assert [type(x).__name__ for x in spec.dag_callbacks] == ["RouterCallback"]
    assert spec.dag_callbacks[0].router.router == "route"


def test_router_callbacks_coexist_with_successors():
    node = DAGNode("root")
    node.then(DAGNode("plain_next"))
    node.add_router_callback(
        RouterCallback(router=RouterSpec(router="route", candidates=[DAGTaskSpec(name="b")]))
    )
    kinds = [type(x).__name__ for x in node.to_spec().dag_callbacks]
    assert kinds == ["SimpleCallback", "RouterCallback"]


# ── DynamicFanOutCallback arm source ──────────────────────────────────────────


def test_fanout_requires_exactly_one_arm_source():
    collector = DAGTaskSpec(name="collector")
    with pytest.raises(ValidationError, match="exactly one of arm_root or arm_router"):
        DynamicFanOutCallback(collector=collector)
    with pytest.raises(ValidationError, match="exactly one of arm_root or arm_router"):
        DynamicFanOutCallback(
            collector=collector,
            arm_root=DAGTaskSpec(name="arm"),
            arm_router=RouterSpec(router="route", candidates=[DAGTaskSpec(name="arm")]),
        )


def test_fanout_with_arm_router_round_trips_through_json():
    collector = DAGTaskSpec(name="collector")
    cb = DynamicFanOutCallback(
        collector=collector,
        arm_router=RouterSpec(
            router="route_by_region",
            candidates=[DAGTaskSpec(name="process", lane="us"), DAGTaskSpec(name="process", lane="eu")],
        ),
    )
    restored = DynamicFanOutCallback.model_validate(cb.model_dump(mode="json"))
    assert restored.arm_root is None
    assert restored.arm_router is not None
    assert restored.arm_router.router == "route_by_region"
    assert [(c.name, c.lane) for c in restored.arm_router.candidates] == [
        ("process", "us"),
        ("process", "eu"),
    ]


def test_fresh_copy_remaps_arm_router():
    cb = DynamicFanOutCallback(
        collector=DAGTaskSpec(name="collector"),
        arm_router=RouterSpec(router="route", candidates=[DAGTaskSpec(name="arm")]),
    )
    root = DAGTaskSpec(name="root", dag_callbacks=[cb])
    fresh, id_map = root.fresh_copy()
    copied = fresh.dag_callbacks[0]
    assert copied.arm_root is None
    assert copied.arm_router is not None
    assert copied.arm_router.id != cb.arm_router.id
    assert id_map[cb.arm_router.candidates[0].id] == copied.arm_router.candidates[0].id


# ── discriminated union ───────────────────────────────────────────────────────


def test_router_callback_round_trips_through_the_union_discriminator():
    spec = _router_tree()
    restored = DAGTaskSpec.model_validate(spec.model_dump(mode="json"))
    cb = restored.dag_callbacks[0]
    assert isinstance(cb, RouterCallback)
    assert cb.type == "router"
    assert cb.router.router == "route"
    assert cb.error_callback is not None
    assert cb.error_callback.name == "notify_bad_route"
