"""
Parser and generator tests for router nodes (mermaid rhombus nodes).

Covers the three shapes a router can take part in -- simple selection
(``parent --> R``), per-item fan-out routing (``parent -->> R``), and converging
branches -- plus every edge shape the parser rejects.
"""

from __future__ import annotations

import pytest

from jobbers.models.dag import DynamicFanOutCallback, FanInCallback, RouterCallback
from jobbers.utils.mermaid_dag import MermaidParseError, dag_spec_to_mermaid, parse_mermaid_dag

# ── helpers ───────────────────────────────────────────────────────────────────


def _router_cb(diagram: str) -> RouterCallback:
    """Parse *diagram* and return the single RouterCallback on its single root."""
    roots = parse_mermaid_dag(diagram)
    assert len(roots) == 1, [r._name for r in roots]
    cbs = [cb for cb in roots[0].to_spec().dag_callbacks if isinstance(cb, RouterCallback)]
    assert len(cbs) == 1, cbs
    return cbs[0]


def _fanout_cb(diagram: str) -> DynamicFanOutCallback:
    """Parse *diagram* and return the single DynamicFanOutCallback on its single root."""
    roots = parse_mermaid_dag(diagram)
    assert len(roots) == 1, [r._name for r in roots]
    cbs = [cb for cb in roots[0].to_spec().dag_callbacks if isinstance(cb, DynamicFanOutCallback)]
    assert len(cbs) == 1, cbs
    return cbs[0]


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

PER_ITEM_ROUTER = """
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


# ── parsing ───────────────────────────────────────────────────────────────────


def test_parse_router_node_and_candidates() -> None:
    cb = _router_cb(SIMPLE_ROUTER)
    assert cb.router.router == "route_by_size"
    assert cb.router.version == 0
    assert cb.router.parameters == {"threshold": 100}
    assert [(c.name, c.queue) for c in cb.router.candidates] == [
        ("fast_path", "default"),
        ("slow_path", "heavy"),
    ]


def test_parse_router_versioned() -> None:
    cb = _router_cb(SIMPLE_ROUTER.replace('R{"route_by_size', 'R{"route_by_size@3'))
    assert cb.router.version == 3


def test_parse_router_unquoted_label() -> None:
    cb = _router_cb(SIMPLE_ROUTER.replace('R{"route_by_size(threshold=100)"}', "R{route_by_size}"))
    assert cb.router.router == "route_by_size"
    assert cb.router.parameters == {}


def test_parse_router_error_edge_becomes_router_error_callback() -> None:
    cb = _router_cb(SIMPLE_ROUTER + '    R -.-> err["notify_bad_route"]\n')
    assert cb.error_callback is not None
    assert cb.error_callback.name == "notify_bad_route"


def test_parse_router_candidates_may_share_a_task_name_across_queues() -> None:
    cb = _router_cb("""
    flowchart TD
        A["classify_order"]
        R{"route_by_tier"}
        P["fulfil_order:priority"]
        S["fulfil_order:standard"]

        A --> R
        R --> P
        R --> S
    """)
    assert [(c.name, c.queue) for c in cb.router.candidates] == [
        ("fulfil_order", "priority"),
        ("fulfil_order", "standard"),
    ]


def test_parse_router_candidate_subtree_is_carried_along() -> None:
    cb = _router_cb(SIMPLE_ROUTER + '    B --> D["after_fast"]\n')
    fast = next(c for c in cb.router.candidates if c.name == "fast_path")
    assert [x.task.name for x in fast.dag_callbacks] == ["after_fast"]


def test_router_and_its_branches_are_never_roots() -> None:
    roots = parse_mermaid_dag(SIMPLE_ROUTER)
    assert [r._name for r in roots] == ["measure_payload"]


def test_router_status_suffix_is_stripped_on_parse() -> None:
    cb = _router_cb("""
    flowchart TD
        A["measure_payload{COMPLETED}"]
        R{"route_by_size{small}"}
        B["fast_path"]

        A --> R
        R --> B
    """)
    assert cb.router.router == "route_by_size"


def test_task_label_status_suffix_is_not_read_as_a_router() -> None:
    """A '{STATUS}' inside a task label must not be lexed as a rhombus node."""
    roots = parse_mermaid_dag("""
    flowchart TD
        A["fetch_data:heavy{COMPLETED|2026-03-30}"]
        B["process_data{STARTED}"]
        A --> B
    """)
    assert [r._name for r in roots] == ["fetch_data"]


# ── converging branches ───────────────────────────────────────────────────────


def test_converging_router_branches_are_not_promoted_to_fan_in() -> None:
    """Exactly one branch ever runs, so `report` must be a plain SimpleCallback."""
    cb = _router_cb(CONVERGING)
    for candidate in cb.router.candidates:
        assert [type(x).__name__ for x in candidate.dag_callbacks] == ["SimpleCallback"]


def test_converging_router_branches_share_one_collector_node() -> None:
    cb = _router_cb(CONVERGING)
    ids = {c.dag_callbacks[0].task.id for c in cb.router.candidates}
    assert len(ids) == 1


def test_two_predecessors_on_the_same_branch_are_still_a_fan_in() -> None:
    """Both run, so this is an ordinary fan-in despite sitting inside a branch."""
    cb = _router_cb("""
    flowchart TD
        A["start"]
        R{"route"}
        B["branch_root"]
        B1["part_a"]
        B2["part_b"]
        M["merge"]

        A --> R
        R --> B
        B --> B1
        B --> B2
        B1 --> M
        B2 --> M
    """)
    branch = cb.router.candidates[0]
    part_a = branch.dag_callbacks[0].task
    assert isinstance(part_a.dag_callbacks[0], FanInCallback)


def test_mixing_router_branch_and_unconditional_predecessors_is_rejected() -> None:
    with pytest.raises(MermaidParseError, match="not all guaranteed to run"):
        parse_mermaid_dag("""
        flowchart TD
            A["start"]
            X["sibling"]
            R{"route"}
            B["branch"]
            D["report"]

            A --> R
            A --> X
            R --> B
            B --> D
            X --> D
        """)


# ── per-item fan-out routing ──────────────────────────────────────────────────


def test_parse_router_as_fanout_arm_root() -> None:
    cb = _fanout_cb(PER_ITEM_ROUTER)
    assert cb.arm_root is None
    assert cb.arm_router is not None
    assert cb.arm_router.router == "route_by_region"
    assert [(c.name, c.queue) for c in cb.arm_router.candidates] == [
        ("process_record", "us"),
        ("process_record", "eu"),
    ]
    assert cb.collector.name == "aggregate"


def test_parse_router_fanout_honours_custom_items_key() -> None:
    cb = _fanout_cb(PER_ITEM_ROUTER.replace("A -->> R", 'A --"records">> R'))
    assert cb.items_key == "records"


def test_router_fanout_candidates_must_share_a_collector() -> None:
    with pytest.raises(MermaidParseError, match="different collectors"):
        parse_mermaid_dag("""
        flowchart TD
            A["fetch_records"]
            R{"route_by_region"}
            US["process_us"]
            EU["process_eu"]
            C1["aggregate_us"]
            C2["aggregate_eu"]

            A -->> R
            R --> US
            R --> EU
            US --o C1
            EU --o C2
        """)


def test_router_fanout_arms_may_be_multi_step_chains() -> None:
    cb = _fanout_cb("""
    flowchart TD
        A["fetch_records"]
        R{"route_by_region"}
        US["start_us"]
        US2["finish_us"]
        EU["start_eu"]
        EU2["finish_eu"]
        C["aggregate"]

        A -->> R
        R --> US
        R --> EU
        US --> US2
        EU --> EU2
        US2 --o C
        EU2 --o C
    """)
    assert cb.arm_router is not None
    us = next(c for c in cb.arm_router.candidates if c.name == "start_us")
    assert [x.task.name for x in us.dag_callbacks] == ["finish_us"]


# ── rejected shapes ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("diagram", "match"),
    [
        pytest.param(
            'flowchart TD\n A["t"] -.-> R{"route"}\n R --> B["b"]\n',
            "must target a task node",
            id="error-edge-into-router",
        ),
        pytest.param(
            'flowchart TD\n A["t"] --> R{"route"}\n R -->> B["b"]\n B --o C["c"]\n',
            "cannot be a dispatcher",
            id="router-as-dispatcher",
        ),
        pytest.param(
            'flowchart TD\n A["a"] -->> B["b"]\n A --> R{"route"}\n R --> Z["z"]\n B --o R\n',
            "collector must be a task node",
            id="fan-in-into-router",
        ),
        pytest.param(
            'flowchart TD\n A["t"] --> R{"route"}\n R --> R2{"route2"}\n R2 --> B["b"]\n',
            "chaining is not supported",
            id="router-chaining",
        ),
        pytest.param(
            'flowchart TD\n A["t"] --> R{"route"}\n',
            "no candidates",
            id="router-without-candidates",
        ),
        pytest.param(
            'flowchart TD\n R{"route"} --> B["b"]\n',
            "cannot be a DAG root",
            id="router-as-root",
        ),
        pytest.param(
            'flowchart TD\n A["t"] --> R{"route:heavy"}\n R --> B["b"]\n',
            "Routers do not run on a queue",
            id="router-with-queue",
        ),
        pytest.param(
            'flowchart TD\n A["t"] --> R{"route"}\n R --> B["dup"]\n R --> C["dup"]\n',
            "indistinguishable",
            id="duplicate-candidates",
        ),
    ],
)
def test_router_rejected_shapes(diagram: str, match: str) -> None:
    with pytest.raises(MermaidParseError, match=match):
        parse_mermaid_dag(diagram)


# ── generation and round-trip ─────────────────────────────────────────────────


def test_generator_emits_rhombus_for_router() -> None:
    spec = parse_mermaid_dag(SIMPLE_ROUTER)[0].to_spec()
    cb = next(c for c in spec.dag_callbacks if isinstance(c, RouterCallback))
    diagram = dag_spec_to_mermaid(spec)
    assert f'{cb.router.id}{{"route_by_size(threshold=100)"}}:::router' in diagram
    assert f"{spec.id} --> {cb.router.id}" in diagram
    for candidate in cb.router.candidates:
        assert f"{cb.router.id} --> {candidate.id}" in diagram
    assert "classDef router" in diagram


def test_generator_emits_fanout_arrow_into_arm_router() -> None:
    spec = parse_mermaid_dag(PER_ITEM_ROUTER)[0].to_spec()
    cb = next(c for c in spec.dag_callbacks if isinstance(c, DynamicFanOutCallback))
    assert cb.arm_router is not None
    diagram = dag_spec_to_mermaid(spec)
    assert f"{spec.id} -->> {cb.arm_router.id}" in diagram
    for candidate in cb.arm_router.candidates:
        assert f"{cb.arm_router.id} --> {candidate.id}" in diagram
        assert f"{candidate.id} --o {cb.collector.id}" in diagram


def test_round_trip_simple_router_preserves_candidates() -> None:
    spec = parse_mermaid_dag(SIMPLE_ROUTER)[0].to_spec()
    reparsed = parse_mermaid_dag(dag_spec_to_mermaid(spec))[0].to_spec()
    original = next(c for c in spec.dag_callbacks if isinstance(c, RouterCallback))
    again = next(c for c in reparsed.dag_callbacks if isinstance(c, RouterCallback))
    assert again.router.router == original.router.router
    assert again.router.parameters == original.router.parameters
    assert [(c.name, c.queue) for c in again.router.candidates] == [
        (c.name, c.queue) for c in original.router.candidates
    ]


def test_round_trip_router_error_callback() -> None:
    spec = parse_mermaid_dag(SIMPLE_ROUTER + '    R -.-> err["notify_bad_route"]\n')[0].to_spec()
    reparsed = parse_mermaid_dag(dag_spec_to_mermaid(spec))[0].to_spec()
    cb = next(c for c in reparsed.dag_callbacks if isinstance(c, RouterCallback))
    assert cb.error_callback is not None
    assert cb.error_callback.name == "notify_bad_route"


def test_round_trip_converging_router_stays_non_fan_in() -> None:
    spec = parse_mermaid_dag(CONVERGING)[0].to_spec()
    reparsed = parse_mermaid_dag(dag_spec_to_mermaid(spec))[0].to_spec()
    cb = next(c for c in reparsed.dag_callbacks if isinstance(c, RouterCallback))
    for candidate in cb.router.candidates:
        assert [type(x).__name__ for x in candidate.dag_callbacks] == ["SimpleCallback"]


def test_round_trip_per_item_router_preserves_arm_router() -> None:
    spec = parse_mermaid_dag(PER_ITEM_ROUTER)[0].to_spec()
    reparsed = parse_mermaid_dag(dag_spec_to_mermaid(spec))[0].to_spec()
    cb = next(c for c in reparsed.dag_callbacks if isinstance(c, DynamicFanOutCallback))
    assert cb.arm_root is None
    assert cb.arm_router is not None
    assert {(c.name, c.queue) for c in cb.arm_router.candidates} == {
        ("process_record", "us"),
        ("process_record", "eu"),
    }
    assert cb.collector.name == "aggregate"


def test_compact_generation_keeps_router_skeleton() -> None:
    spec = parse_mermaid_dag(PER_ITEM_ROUTER)[0].to_spec()
    cb = next(c for c in spec.dag_callbacks if isinstance(c, DynamicFanOutCallback))
    assert cb.arm_router is not None
    diagram = dag_spec_to_mermaid(spec, expand_fanouts=False)
    assert f"{spec.id} -->> {cb.arm_router.id}" in diagram
    for candidate in cb.arm_router.candidates:
        assert f"{candidate.id} --o {cb.collector.id}" in diagram
