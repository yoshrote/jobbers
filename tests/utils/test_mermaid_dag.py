"""Tests for the mermaid DAG parser and generator."""

from __future__ import annotations

import base64
import json

import pytest
from ulid import ULID

from jobbers.models.dag import DAGTaskSpec, FanInCallback, SimpleCallback
from jobbers.models.task_status import TaskStatus
from jobbers.utils.mermaid_dag import (
    MermaidParseError,
    _parse_label,
    _parse_param_value,
    _serialize_params,
    dag_spec_to_mermaid,
    parse_mermaid_dag,
)

# ── _parse_param_value ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("false", False),
        ("null", None),
        ("NULL", None),
        ("42", 42),
        ("-7", -7),
        ("3.14", 3.14),
        ('"hello world"', "hello world"),
        ("'single'", "single"),
        ("plain", "plain"),
        ("hello,world", "hello,world"),
        # base64-encoded JSON values
        ("~WzEsMiwzXQ==", [1, 2, 3]),
        ('~eyJhIjoxfQ==', {"a": 1}),
        ("~bnVsbA==", None),
    ],
)
def test_parse_param_value(raw: str, expected: object) -> None:
    assert _parse_param_value(raw) == expected


# ── _parse_label ──────────────────────────────────────────────────────────────


def test_parse_label_name_only() -> None:
    name, version, queue, params = _parse_label("fetch_data")
    assert name == "fetch_data"
    assert version == 0
    assert queue == "default"
    assert params == {}


def test_parse_label_name_and_queue() -> None:
    name, version, queue, params = _parse_label("fetch_data:heavy")
    assert name == "fetch_data"
    assert version == 0
    assert queue == "heavy"


def test_parse_label_name_version_only() -> None:
    name, version, queue, params = _parse_label("fetch_data@3")
    assert name == "fetch_data"
    assert version == 3
    assert queue == "default"
    assert params == {}


def test_parse_label_name_version_queue() -> None:
    name, version, queue, params = _parse_label("fetch_data@2:heavy")
    assert name == "fetch_data"
    assert version == 2
    assert queue == "heavy"


def test_parse_label_name_queue_params() -> None:
    name, version, queue, params = _parse_label("fetch_data:heavy(url=https://example.com,limit=100)")
    assert name == "fetch_data"
    assert version == 0
    assert queue == "heavy"
    assert params == {"url": "https://example.com", "limit": 100}


def test_parse_label_version_queue_params() -> None:
    name, version, queue, params = _parse_label("fetch_data@5:heavy(limit=100)")
    assert name == "fetch_data"
    assert version == 5
    assert queue == "heavy"
    assert params == {"limit": 100}


def test_parse_label_strips_status_suffix() -> None:
    name, version, queue, params = _parse_label("fetch_data:default(limit=5){COMPLETED|2026-03-30}")
    assert name == "fetch_data"
    assert version == 0
    assert queue == "default"
    assert params == {"limit": 5}


def test_parse_label_bool_params() -> None:
    _, _, _, params = _parse_label("my_task(dry_run=true,verbose=false)")
    assert params == {"dry_run": True, "verbose": False}


def test_parse_label_invalid() -> None:
    with pytest.raises(MermaidParseError, match="Invalid node label"):
        _parse_label("123invalid")


# ── _serialize_params ─────────────────────────────────────────────────────────


def test_serialize_params_empty() -> None:
    assert _serialize_params({}) == ""


def test_serialize_params_basic() -> None:
    result = _serialize_params({"limit": 100, "dry_run": True})
    assert "limit=100" in result
    assert "dry_run=true" in result


def test_serialize_params_quoted_string() -> None:
    result = _serialize_params({"msg": "hello, world"})
    assert '"hello, world"' in result


def test_serialize_params_null() -> None:
    assert _serialize_params({"x": None}) == "x=null"


def test_serialize_params_list() -> None:
    result = _serialize_params({"ids": [1, 2, 3]})
    assert result.startswith("ids=~")
    # Round-trip: the blob must decode back to the original list.
    blob = result[len("ids=~"):]
    assert json.loads(base64.b64decode(blob)) == [1, 2, 3]


def test_serialize_params_dict() -> None:
    result = _serialize_params({"cfg": {"retries": 3}})
    assert result.startswith("cfg=~")


def test_serialize_params_mixed() -> None:
    result = _serialize_params({"limit": 10, "ids": [1, 2], "dry_run": True})
    assert "limit=10" in result
    assert "dry_run=true" in result
    assert "ids=~" in result


# ── parse_mermaid_dag — linear chain ─────────────────────────────────────────


def test_parse_linear_chain() -> None:
    text = """
    flowchart TD
        A["fetch_data"]
        B["process_data"]
        C["save_results"]
        A --> B
        B --> C
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    root = roots[0]
    assert root._name == "fetch_data"  # type: ignore[attr-defined]
    assert len(root._successors) == 1  # type: ignore[attr-defined]
    b_node = root._successors[0][0]  # type: ignore[attr-defined]
    assert b_node._name == "process_data"  # type: ignore[attr-defined]
    assert len(b_node._successors) == 1  # type: ignore[attr-defined]


def test_parse_inline_edge_with_label() -> None:
    """Node labels and edges on the same line are supported."""
    text = 'flowchart TD\n    A["fetch_data"] --> B["process_data"]'
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    assert roots[0]._name == "fetch_data"  # type: ignore[attr-defined]


# ── parse_mermaid_dag — fan-out ───────────────────────────────────────────────


def test_parse_fan_out() -> None:
    text = """
    flowchart TD
        A["split_work"]
        B["process_chunk_a"]
        C["process_chunk_b"]
        A --> B
        A --> C
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    root = roots[0]
    assert len(root._successors) == 2  # type: ignore[attr-defined]
    successor_names = {s[0]._name for s in root._successors}  # type: ignore[attr-defined]
    assert successor_names == {"process_chunk_a", "process_chunk_b"}


# ── parse_mermaid_dag — diamond (fan-out + fan-in) ───────────────────────────


def test_parse_diamond() -> None:
    text = """
    flowchart TD
        A["split_work"]
        B["process_chunk_a"]
        C["process_chunk_b"]
        D["merge_results"]
        A --> B
        A --> C
        B --> D
        C --> D
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    root = roots[0]
    # Root fans out to B and C.
    assert len(root._successors) == 2  # type: ignore[attr-defined]

    # B and C should both have a FanInCallback pointing to D.
    b_node = next(s[0] for s in root._successors if s[0]._name == "process_chunk_a")  # type: ignore[attr-defined]
    assert len(b_node._successors) == 1  # type: ignore[attr-defined]
    fan_in_key = b_node._successors[0][1]  # type: ignore[attr-defined]
    assert fan_in_key is not None  # FanInCallback has a key
    assert "dag:fan-in:" in fan_in_key

    # The fan-in collector has the expected name.
    collector = b_node._successors[0][0]  # type: ignore[attr-defined]
    assert collector._name == "merge_results"  # type: ignore[attr-defined]


def test_parse_diamond_fan_in_predecessors() -> None:
    """Both B and C must appear as predecessors in the fan-in set."""
    text = """
    flowchart TD
        A["root"]
        B["branch_a"]
        C["branch_b"]
        D["collector"]
        A --> B
        A --> C
        B --> D
        C --> D
    """
    roots = parse_mermaid_dag(text)
    root = roots[0]
    fan_in_map = root.fan_in_predecessors()
    assert len(fan_in_map) == 1
    key, preds = next(iter(fan_in_map.items()))
    assert "dag:fan-in:" in key
    assert len(preds) == 2


# ── parse_mermaid_dag — error callbacks ──────────────────────────────────────


def test_parse_error_callback() -> None:
    text = """
    flowchart TD
        A["fetch_data"]
        B["process_data"]
        err["notify_failure(channel=ops)"]
        A --> B
        A -.-> err
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    root = roots[0]
    # err node should NOT be a root.
    assert root._name == "fetch_data"  # type: ignore[attr-defined]
    # The success edge should carry the on_error node.
    assert len(root._successors) == 1  # type: ignore[attr-defined]
    _successor, _fan_in_key, on_error = root._successors[0]  # type: ignore[attr-defined]
    assert on_error is not None
    assert on_error._name == "notify_failure"  # type: ignore[attr-defined]
    assert on_error._parameters == {"channel": "ops"}  # type: ignore[attr-defined]


def test_parse_multiple_error_callbacks_raises() -> None:
    text = """
    flowchart TD
        A["fetch_data"]
        err1["handler_a"]
        err2["handler_b"]
        A -.-> err1
        A -.-> err2
    """
    with pytest.raises(MermaidParseError, match="multiple"):
        parse_mermaid_dag(text)


# ── parse_mermaid_dag — node with queue and params ───────────────────────────


def test_parse_queue_and_params() -> None:
    text = """
    flowchart TD
        A["fetch_data:heavy_queue(limit=50,dry_run=false)"]
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    root = roots[0]
    assert root._name == "fetch_data"  # type: ignore[attr-defined]
    assert root._queue == "heavy_queue"  # type: ignore[attr-defined]
    assert root._parameters == {"limit": 50, "dry_run": False}  # type: ignore[attr-defined]


def test_parse_default_queue() -> None:
    text = 'flowchart TD\n    A["my_task"]'
    roots = parse_mermaid_dag(text)
    assert roots[0]._queue == "default"  # type: ignore[attr-defined]


def test_parse_version() -> None:
    text = 'flowchart TD\n    A["my_task@3"]'
    roots = parse_mermaid_dag(text)
    assert roots[0]._version == 3  # type: ignore[attr-defined]


def test_parse_version_defaults_to_zero() -> None:
    text = 'flowchart TD\n    A["my_task"]'
    roots = parse_mermaid_dag(text)
    assert roots[0]._version == 0  # type: ignore[attr-defined]


def test_parse_version_with_queue_and_params() -> None:
    text = 'flowchart TD\n    A["my_task@2:heavy(limit=10)"]'
    roots = parse_mermaid_dag(text)
    root = roots[0]
    assert root._version == 2  # type: ignore[attr-defined]
    assert root._queue == "heavy"  # type: ignore[attr-defined]
    assert root._parameters == {"limit": 10}  # type: ignore[attr-defined]


# ── parse_mermaid_dag — status suffix stripped ───────────────────────────────


def test_parse_strips_status_suffix() -> None:
    """Output diagrams with {STATUS} sections should round-trip cleanly."""
    text = """
    flowchart TD
        01JXXX["fetch_data:heavy{COMPLETED|2026-03-30}"]:::completed
        01JYYY["process_data{STARTED}"]:::running
        01JXXX --> 01JYYY
    """
    roots = parse_mermaid_dag(text)
    assert len(roots) == 1
    assert roots[0]._name == "fetch_data"  # type: ignore[attr-defined]
    assert roots[0]._queue == "heavy"  # type: ignore[attr-defined]


# ── parse_mermaid_dag — error handling ───────────────────────────────────────


def test_parse_empty_text_raises() -> None:
    with pytest.raises(MermaidParseError, match="No nodes"):
        parse_mermaid_dag("flowchart TD\n")


def test_parse_invalid_label_raises() -> None:
    with pytest.raises(MermaidParseError, match="Invalid node label"):
        parse_mermaid_dag('flowchart TD\n    A["123badname"]')


# ── dag_spec_to_mermaid ───────────────────────────────────────────────────────


def _simple_spec(name: str, queue: str = "default") -> DAGTaskSpec:
    return DAGTaskSpec(id=ULID(), name=name, queue=queue)


def test_generator_single_node() -> None:
    spec = _simple_spec("fetch_data")
    diagram = dag_spec_to_mermaid(spec)
    assert "flowchart TD" in diagram
    assert "fetch_data" in diagram
    assert str(spec.id) in diagram
    assert "classDef" in diagram


def test_generator_queue_omitted_when_default() -> None:
    spec = _simple_spec("fetch_data", queue="default")
    diagram = dag_spec_to_mermaid(spec)
    assert ":default" not in diagram


def test_generator_queue_included_when_not_default() -> None:
    spec = _simple_spec("fetch_data", queue="heavy")
    diagram = dag_spec_to_mermaid(spec)
    assert ":heavy" in diagram


def test_generator_version_omitted_when_zero() -> None:
    spec = DAGTaskSpec(id=ULID(), name="my_task", version=0)
    diagram = dag_spec_to_mermaid(spec)
    assert "@" not in diagram


def test_generator_version_included_when_nonzero() -> None:
    spec = DAGTaskSpec(id=ULID(), name="my_task", version=3)
    diagram = dag_spec_to_mermaid(spec)
    assert "@3" in diagram


def test_generator_linear_chain() -> None:
    a = _simple_spec("step_a")
    b = _simple_spec("step_b")
    a_with_cb = DAGTaskSpec(
        id=a.id,
        name=a.name,
        queue=a.queue,
        dag_callbacks=[SimpleCallback(task=b)],
    )
    diagram = dag_spec_to_mermaid(a_with_cb)
    assert f"{a.id} --> {b.id}" in diagram
    assert "step_a" in diagram
    assert "step_b" in diagram


def test_generator_error_callback_edge() -> None:
    err = _simple_spec("notify_failure")
    a = DAGTaskSpec(
        id=ULID(),
        name="fetch_data",
        dag_callbacks=[SimpleCallback(task=_simple_spec("process"), error_callback=err)],
    )
    diagram = dag_spec_to_mermaid(a)
    assert f"{a.id} -.-> {err.id}" in diagram


def test_generator_status_overlay() -> None:
    spec = _simple_spec("my_task")
    statuses = {str(spec.id): TaskStatus.COMPLETED}
    diagram = dag_spec_to_mermaid(spec, task_statuses=statuses)
    assert "{completed}" in diagram
    assert ":::completed" in diagram


def test_generator_status_class_defs_always_present() -> None:
    """ClassDef block is always emitted even without status data."""
    spec = _simple_spec("my_task")
    diagram = dag_spec_to_mermaid(spec)
    assert "classDef completed" in diagram
    assert "classDef failed" in diagram


def test_generator_no_duplicate_nodes() -> None:
    """Fan-in collector must appear only once as a node definition."""
    collector = _simple_spec("merge")
    branch_a = _simple_spec("branch_a")
    branch_b = _simple_spec("branch_b")
    fan_in_key = f"dag:fan-in:{collector.id}"
    root = DAGTaskSpec(
        id=ULID(),
        name="split",
        dag_callbacks=[
            FanInCallback(task=branch_a, fan_in_key=fan_in_key),
            FanInCallback(task=branch_b, fan_in_key=fan_in_key),
        ],
    )
    # branch_a and branch_b both point to collector
    branch_a_with_cb = DAGTaskSpec(
        id=branch_a.id,
        name=branch_a.name,
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_in_key)],
    )
    branch_b_with_cb = DAGTaskSpec(
        id=branch_b.id,
        name=branch_b.name,
        dag_callbacks=[FanInCallback(task=collector, fan_in_key=fan_in_key)],
    )
    root_with_branches = DAGTaskSpec(
        id=root.id,
        name=root.name,
        dag_callbacks=[
            SimpleCallback(task=branch_a_with_cb),
            SimpleCallback(task=branch_b_with_cb),
        ],
    )
    diagram = dag_spec_to_mermaid(root_with_branches)
    # collector node id should appear exactly once as a node definition line
    collector_id = str(collector.id)
    node_def_count = diagram.count(f'{collector_id}["')
    assert node_def_count == 1


# ── Round-trip ────────────────────────────────────────────────────────────────


def test_round_trip_linear() -> None:
    """Parse → generate → parse should produce equivalent structure."""
    original = """
    flowchart TD
        A["fetch_data:heavy(limit=10)"]
        B["process_data"]
        C["save_results"]
        A --> B
        B --> C
    """
    roots = parse_mermaid_dag(original)
    spec = roots[0]._to_spec()  # type: ignore[attr-defined]
    diagram = dag_spec_to_mermaid(spec)
    roots2 = parse_mermaid_dag(diagram)

    assert len(roots2) == 1
    root2 = roots2[0]
    assert root2._name == "fetch_data"  # type: ignore[attr-defined]
    assert root2._queue == "heavy"  # type: ignore[attr-defined]
    assert root2._parameters == {"limit": 10}  # type: ignore[attr-defined]
    # Three nodes: A → B → C
    assert len(root2._successors) == 1  # type: ignore[attr-defined]
    assert root2._successors[0][0]._name == "process_data"  # type: ignore[attr-defined]


def test_round_trip_complex_params() -> None:
    """Complex param values survive a generate → parse cycle."""
    spec = DAGTaskSpec(
        id=ULID(),
        name="my_task",
        parameters={"limit": 10, "ids": [1, 2, 3], "cfg": {"retries": 3}, "flag": None},
    )
    diagram = dag_spec_to_mermaid(spec)
    roots = parse_mermaid_dag(diagram)
    assert roots[0]._parameters == {"limit": 10, "ids": [1, 2, 3], "cfg": {"retries": 3}, "flag": None}  # type: ignore[attr-defined]


def test_round_trip_diamond() -> None:
    """Diamond DAG survives a generate → parse cycle with correct fan-in."""
    original = """
    flowchart TD
        A["root"]
        B["branch_a"]
        C["branch_b"]
        D["collector"]
        A --> B
        A --> C
        B --> D
        C --> D
    """
    roots = parse_mermaid_dag(original)
    spec = roots[0]._to_spec()  # type: ignore[attr-defined]
    diagram = dag_spec_to_mermaid(spec)
    roots2 = parse_mermaid_dag(diagram)

    fan_in_map = roots2[0].fan_in_predecessors()
    assert len(fan_in_map) == 1
    preds = next(iter(fan_in_map.values()))
    assert len(preds) == 2

