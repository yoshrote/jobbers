"""
Parser and generator for the Jobbers mermaid DAG dialect.

Jobbers represents task dependency graphs as ``flowchart TD`` mermaid diagrams.
Each node encodes a task definition; edges encode dependencies.

Node label grammar
==================

Every node uses a **quoted rectangular-bracket label**::

    node_id["task_name[@version][:queue][(param=val, ...)]"]

.. list-table::
   :header-rows: 1

   * - Section
     - Required
     - Meaning
   * - ``task_name``
     - yes
     - Registered task name (must match a name returned by the registry)
   * - ``@version``
     - no
     - Integer task version; defaults to ``0`` when omitted
   * - ``:queue``
     - no
     - Target queue name; defaults to ``"default"``
   * - ``(key=val, …)``
     - no
     - Task parameters passed verbatim to the task function; values are
       type-coerced (see below)
   * - ``{…}``
     - **reserved**
     - Output-only section for status / timestamps / metrics; stripped
       silently on parse so UI-generated diagrams can be re-submitted as-is

Parameter value coercion
------------------------

Values inside ``(...)`` are coerced in this order:

1. ``~<base64>`` — base64-decoded JSON value (list, dict, or any JSON type)
2. ``null`` (case-insensitive) → ``None``
3. ``true`` / ``false`` (case-insensitive) → :class:`bool`
4. Decimal integer literal → :class:`int`
5. Decimal float literal → :class:`float`
6. Quoted string (``"..."`` or ``'...'``) → :class:`str` (quotes stripped)
7. Anything else → :class:`str`

The serializer emits human-readable ``key=val`` for scalars and ``None``, and
``key=~<base64-JSON>`` for any complex value (list, dict, etc.).

Edge semantics
==============

+----------+-----------------------------------------------------------------------+
| Arrow    | Meaning                                                               |
+==========+=======================================================================+
| ``-->``  | **Success callback.** Automatically promoted to ``FanInCallback``     |
|          | when the destination node has **≥ 2** incoming ``-->`` edges.        |
+----------+-----------------------------------------------------------------------+
| ``-.->`` | **Error callback.** The source node fires this task on any permanent  |
|          | failure (``FAILED``, ``CANCELLED``, ``STALLED``, ``DROPPED``).       |
|          | Maps to the ``on_error`` argument of ``.then()`` / ``.merge()``.     |
|          | Each source node may have **at most one** ``-.->`` target.           |
+----------+-----------------------------------------------------------------------+
| ``--o``  | **Fan-in edge.** Used exclusively inside dynamic fan-out branches to  |
|          | mark the collector (the last branch node points to the collector).    |
|          | Only valid when the branch is rooted at a decision ``{...}`` node.   |
+----------+-----------------------------------------------------------------------+

Decision nodes
==============

A diamond-shaped decision node declares a dynamic fan-out::

    D{"router_name[(key=val, ...)]"}

The registered routing function is called inline by the processor (not as a
full task) with the dispatcher's results plus any ``router_params``; it returns
a list of parameter dicts — one per child instance.  All children fan into the
collector once they complete.

Full syntax::

    A["fetch_records"]
    D{"fanout"}
    B["process_record"]
    C["aggregate_results"]

    A --> D
    D --> B
    B --o C

Or with a chain inside the branch (``B --> E --o C`` — each branch runs
``B → E``, all ``E`` instances fan into ``C``)::

    A --> D
    D --> B
    B --> E
    E --o C

Fan-in detection
================

Fan-in is detected automatically from edge structure: if two or more ``-->``
edges point at the same destination node, all of those predecessors are wired
as ``FanInCallback`` predecessors via ``DAGNode.merge(*predecessors,
into=collector)``.  No special syntax is required in the diagram.

Reserved output annotations
============================

When the generator emits a diagram for a live DAG it appends a ``{STATUS}``
section inside the label and emits ``:::classname`` / ``classDef`` blocks::

    node_id["task_name:queue{COMPLETED|2026-03-30T10:00|43ms}"]:::completed

These annotations are **read-only** — the parser strips them silently, so a
diagram copied from the UI can be re-submitted without any editing.

Implementation note
===================

The grammar is simple enough that a custom regex-based parser is used rather
than a third-party mermaid library.  The only well-maintained Python mermaid
parser (``mermaid-parser-py``) requires Node.js as a runtime dependency, which
is undesirable in a pure-Python backend.  If the grammar becomes substantially
more complex, switching to ``mermaid-parser-py`` or an ``lark``-based grammar
is straightforward.

Example
=======

.. code-block:: mermaid

    flowchart TD
        A["fetch_data:heavy(url=https://api.example.com,limit=100)"]
        B["process_chunk_a"]
        C["process_chunk_b"]
        D["merge_results"]
        E["notify_slack(channel=ops)"]
        err["notify_failure"]

        A --> B
        A --> C
        B --> D
        C --> D
        D --> E
        A -.-> err

``B`` and ``C`` fan in to ``D`` (detected automatically from the two incoming
``-->`` edges).  ``A`` fires ``err`` on permanent failure.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any

from ulid import ULID

from jobbers.models.dag import DAGNode, DAGTaskSpec, DynamicFanOutCallback
from jobbers.models.task_status import TaskStatus


class MermaidParseError(ValueError):
    """Raised when mermaid DAG text cannot be parsed into a DAGNode graph."""


# ── Regex patterns ─────────────────────────────────────────────────────────────

# Strip trailing reserved {status_info} from a label before grammar parsing.
_STATUS_SUFFIX_RE = re.compile(r"\{[^}]*\}\s*$")

# Parses: task_name[@version][:queue][(params)]
_LABEL_RE = re.compile(
    r"^(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(?:@(?P<version>\d+))?(?::(?P<queue>[a-zA-Z0-9_-]+))?(?:\((?P<params>[^)]*)\))?$"
)

# Node definition — no line anchor so finditer scans inline definitions.
# Handles: id["label"]  id['label']  id[label]  id["label"]:::class
_NODE_RE = re.compile(r'(\w+)\[(?:"([^"]*)"|\'([^\']*)\'|([^\[\]"\']+))\](?:::[\w]+)?')

# Decision (diamond) node: id{"label"} or id{'label'} or id{label}
# Run on the *cleaned* line after _NODE_RE has stripped rectangular nodes so
# that status-suffix {…} tokens inside ["label{status}"] are already gone.
_DECISION_NODE_RE = re.compile(
    r'(\w+)\{(?:"([^"{}]*)"|\'([^\'{}]*)\'|([^"\'{}]*))\}(?:::[\w]+)?'
)

# Router label grammar inside a decision node: router_name[(key=val, ...)]
_ROUTER_LABEL_RE = re.compile(
    r"^(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(?:\((?P<params>[^)]*)\))?$"
)

# Strip :::classname suffixes that remain after node-label removal.
_CLASS_SUFFIX_RE = re.compile(r":::\w+")

# Lines that carry no node or edge information.
_SKIP_PREFIXES = (
    "flowchart",
    "graph ",
    "subgraph",
    "end",
    "classDef",
    "classRef",
    "%%",
    "style ",
    "linkStyle ",
)

# ── Status → CSS class ─────────────────────────────────────────────────────────

_STATUS_CLASS: dict[TaskStatus, str] = {
    TaskStatus.UNSUBMITTED: "pending",
    TaskStatus.SUBMITTED: "pending",
    TaskStatus.SCHEDULED: "pending",
    TaskStatus.STARTED: "running",
    TaskStatus.HEARTBEAT: "running",
    TaskStatus.COMPLETED: "completed",
    TaskStatus.FAILED: "failed",
    TaskStatus.DROPPED: "failed",
    TaskStatus.CANCELLED: "cancelled",
    TaskStatus.STALLED: "stalled",
}

_CLASS_DEFS = (
    "    classDef completed fill:#90EE90,stroke:#2D862D,color:#000\n"
    "    classDef running   fill:#87CEEB,stroke:#0066CC,color:#000\n"
    "    classDef pending   fill:#F0F0F0,stroke:#999,color:#000\n"
    "    classDef failed    fill:#FFB3B3,stroke:#CC0000,color:#000\n"
    "    classDef cancelled fill:#E0E0E0,stroke:#666,color:#000\n"
    "    classDef stalled   fill:#FFD580,stroke:#CC8800,color:#000"
)


# ── Label helpers ──────────────────────────────────────────────────────────────


def _parse_param_value(raw: str) -> Any:
    """
    Coerce a raw parameter value token to a Python value.

    Coercion order:

    1. ``~<base64>`` — base64-decoded JSON (for complex values: lists, dicts, etc.)
    2. ``null`` (case-insensitive) → ``None``
    3. ``true`` / ``false`` (case-insensitive) → ``bool``
    4. Integer literal → ``int``
    5. Float literal → ``float``
    6. Quoted string (``"..."`` or ``'...'``) → ``str`` (quotes stripped)
    7. Anything else → ``str``
    """
    s = raw.strip()
    if s.startswith("~"):
        return json.loads(base64.b64decode(s[1:]))
    lower = s.lower()
    if lower == "null":
        return None
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    if len(s) >= 2 and s[0] in ('"', "'") and s[-1] == s[0]:
        return s[1:-1]
    return s


def _parse_params(raw: str) -> dict[str, Any]:
    """
    Parse ``'key=val, key=val, ...'`` into a dict with type-coerced values.

    Handles quoted values that contain commas or spaces.
    """
    result: dict[str, Any] = {}
    pos = 0
    s = raw.strip()
    while pos < len(s):
        # Skip whitespace / comma separators.
        while pos < len(s) and s[pos] in " \t,":
            pos += 1
        if pos >= len(s):
            break
        key_m = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*", s[pos:])
        if not key_m:
            break
        key = key_m.group(1)
        pos += key_m.end()
        # Read value: quoted or up to next comma.
        if pos < len(s) and s[pos] in ('"', "'"):
            quote = s[pos]
            end = s.find(quote, pos + 1)
            val_raw = s[pos : end + 1] if end != -1 else s[pos:]
            pos = end + 1 if end != -1 else len(s)
        else:
            comma = s.find(",", pos)
            if comma == -1:
                val_raw, pos = s[pos:], len(s)
            else:
                val_raw, pos = s[pos:comma], comma + 1
        result[key] = _parse_param_value(val_raw)
    return result


def _parse_label(label: str) -> tuple[str, int, str, dict[str, Any]]:
    """
    Parse a node label into ``(task_name, version, queue, parameters)``.

    Strips the reserved ``{status}`` suffix before parsing so that
    output diagrams can be fed back in without modification.
    """
    label = _STATUS_SUFFIX_RE.sub("", label).strip()
    m = _LABEL_RE.match(label)
    if not m:
        raise MermaidParseError(
            f"Invalid node label {label!r}. "
            "Expected: task_name[@version][:queue][(key=val, ...)]"
        )
    name = m.group("name")
    version = int(m.group("version")) if m.group("version") is not None else 0
    queue = m.group("queue") or "default"
    params_str = m.group("params") or ""
    params = _parse_params(params_str) if params_str.strip() else {}
    return name, version, queue, params


def _serialize_params(params: dict[str, Any]) -> str:
    """
    Serialize a parameters dict to ``'key=val, key=val'`` for mermaid labels.

    Scalar values (``bool``, ``int``, ``float``, ``str``) are emitted as
    human-readable ``key=val`` pairs.  ``None`` is emitted as ``key=null``.
    Complex values (lists, dicts, and anything else) are encoded as
    ``key=~<base64-JSON>`` to keep the label Mermaid-safe.
    """
    if not params:
        return ""
    parts: list[str] = []
    for k, v in params.items():
        if v is None:
            parts.append(f"{k}=null")
        elif isinstance(v, bool):
            parts.append(f"{k}={str(v).lower()}")
        elif isinstance(v, (int, float)):
            parts.append(f"{k}={v}")
        elif isinstance(v, str) and any(c in v for c in (",", " ", '"')):
            escaped = v.replace('"', '\\"')
            parts.append(f'{k}="{escaped}"')
        elif isinstance(v, str):
            parts.append(f"{k}={v}")
        else:
            blob = base64.b64encode(json.dumps(v, separators=(",", ":")).encode()).decode()
            parts.append(f"{k}=~{blob}")
    return ", ".join(parts)


# ── Mermaid lexer ──────────────────────────────────────────────────────────────


def _extract_edges_from_line(
    line: str,
    success_edges: list[tuple[str, str]],
    error_edges: list[tuple[str, str]],
    fanin_edges: list[tuple[str, str]],
) -> None:
    """
    Parse ``-->``, ``-.->``, and ``--o`` edge patterns from a cleaned mermaid line.

    Handles chained edges on one line (``A --> B --> C``) and mixed
    inline node+edge syntax after node labels have been stripped.
    """
    # Split on edge operators, preserving the operator tokens.
    tokens = re.split(r"\s*(-->|-\.->|--o)\s*", line)
    for i in range(1, len(tokens) - 1, 2):
        op = tokens[i]
        src_m = re.search(r"\b(\w+)\s*$", tokens[i - 1])
        dst_m = re.match(r"\s*(\w+)", tokens[i + 1])
        if not src_m or not dst_m:
            continue
        src, dst = src_m.group(1), dst_m.group(1)
        if op == "-->":
            success_edges.append((src, dst))
        elif op == "-.->":
            error_edges.append((src, dst))
        elif op == "--o":
            fanin_edges.append((src, dst))


def _lex_mermaid(
    text: str,
) -> tuple[
    dict[str, str],
    dict[str, str],
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[tuple[str, str]],
]:
    """
    Extract node definitions and edges from mermaid flowchart text.

    Returns ``(node_labels, decision_node_labels, success_edges, error_edges,
    fanin_edges)`` where:

    - ``node_labels`` maps node identifier → raw label string (rectangular nodes)
    - ``decision_node_labels`` maps node identifier → raw label string (diamond nodes)
    - ``success_edges`` is a list of ``(src, dst)`` for ``-->`` edges
    - ``error_edges`` is a list of ``(src, dst)`` for ``-.->`` edges
    - ``fanin_edges`` is a list of ``(src, dst)`` for ``--o`` edges
    """
    node_labels: dict[str, str] = {}
    decision_node_labels: dict[str, str] = {}
    success_edges: list[tuple[str, str]] = []
    error_edges: list[tuple[str, str]] = []
    fanin_edges: list[tuple[str, str]] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or any(line.startswith(p) for p in _SKIP_PREFIXES):
            continue
        # Extract rectangular node label definitions.
        for m in _NODE_RE.finditer(line):
            node_labels[m.group(1)] = (m.group(2) or m.group(3) or m.group(4) or "").strip()
        # Strip rectangular nodes and class suffixes.
        cleaned = _NODE_RE.sub(lambda m: m.group(1), line)
        cleaned = _CLASS_SUFFIX_RE.sub("", cleaned)
        # Extract diamond (decision) nodes from the cleaned line.
        # Status-suffix {…} tokens inside rectangular labels are now gone,
        # so only genuine diamond-node braces remain.
        for m in _DECISION_NODE_RE.finditer(cleaned):
            label = (m.group(2) or m.group(3) or m.group(4) or "").strip()
            decision_node_labels[m.group(1)] = label
        # Strip decision nodes before edge extraction.
        cleaned = _DECISION_NODE_RE.sub(lambda m: m.group(1), cleaned)
        _extract_edges_from_line(cleaned, success_edges, error_edges, fanin_edges)

    return node_labels, decision_node_labels, success_edges, error_edges, fanin_edges


# ── Parser ────────────────────────────────────────────────────────────────────


def parse_mermaid_dag(text: str) -> list[DAGNode]:
    """
    Parse a mermaid flowchart into a list of root :class:`~jobbers.models.dag.DAGNode` objects.

    The returned nodes are fully wired (``then`` / ``merge`` / ``fanout``) and
    ready for :meth:`~jobbers.state_manager.StateManager.submit_dag`.

    :param text: Raw mermaid ``flowchart TD`` text.
    :returns: Root ``DAGNode`` instances — nodes with no incoming ``-->`` edges
        that are not error targets, branch-template starts, or fan-in collectors.
    :raises MermaidParseError: If the text contains invalid node labels, multiple
        error callbacks on the same source, decision nodes with ambiguous wiring,
        ``--o`` edges not connected to any decision node, or no reachable root nodes.
    """
    node_labels, decision_node_labels, success_edges, error_edges, fanin_edges = _lex_mermaid(text)

    decision_ids: set[str] = set(decision_node_labels)

    if not node_labels and not success_edges and not decision_node_labels:
        raise MermaidParseError("No nodes found in the mermaid text.")

    # ── Classify success edges ────────────────────────────────────────────────
    # Edges involving decision nodes are routing metadata, not task edges.
    dispatcher_for: dict[str, str] = {}    # decision_id → dispatcher rectangular node id
    branch_start_for: dict[str, str] = {}  # decision_id → first template node id
    regular_success_edges: list[tuple[str, str]] = []

    for src, dst in success_edges:
        if dst in decision_ids:
            # A --> D  (A dispatches, D is the routing node)
            if dst in dispatcher_for:
                raise MermaidParseError(
                    f"Decision node '{dst}' has multiple incoming '-->' edges; "
                    "only one dispatcher is allowed."
                )
            dispatcher_for[dst] = src
        elif src in decision_ids:
            # D --> B  (B is the first node in each branch)
            if src in branch_start_for:
                raise MermaidParseError(
                    f"Decision node '{src}' has multiple outgoing '-->' edges; "
                    "only a single branch template is supported per decision node."
                )
            branch_start_for[src] = dst
        else:
            regular_success_edges.append((src, dst))

    # ── Fan-in map (--o edges) ────────────────────────────────────────────────
    fanin_map: dict[str, str] = {}  # last_branch_node → collector_id
    collector_ids: set[str] = set()
    for src, dst in fanin_edges:
        fanin_map[src] = dst
        collector_ids.add(dst)

    branch_start_ids: set[str] = set(branch_start_for.values())

    # ── Auto-register nodes that appear only in edges ─────────────────────────
    all_ids: set[str] = set(node_labels)
    for src, dst in regular_success_edges + error_edges:
        for nid in (src, dst):
            if nid not in all_ids and nid not in decision_ids:
                node_labels[nid] = nid
                all_ids.add(nid)
    for src, dst in fanin_edges:
        for nid in (src, dst):
            if nid not in all_ids:
                node_labels[nid] = nid
                all_ids.add(nid)
    for b_id in branch_start_for.values():
        if b_id not in all_ids:
            node_labels[b_id] = b_id
            all_ids.add(b_id)
    for a_id in dispatcher_for.values():
        if a_id not in all_ids:
            node_labels[a_id] = a_id
            all_ids.add(a_id)

    if not node_labels:
        raise MermaidParseError("No nodes found in the mermaid text.")

    # ── Parse rectangular node labels ─────────────────────────────────────────
    parsed: dict[str, tuple[str, int, str, dict[str, Any]]] = {}
    for nid, label in node_labels.items():
        try:
            parsed[nid] = _parse_label(label)
        except MermaidParseError as exc:
            raise MermaidParseError(f"Node '{nid}': {exc}") from exc

    # TODO: validate queue names against the database.
    # All queue names referenced by this DAG are known at this point; collect them
    # with a set comprehension and resolve in a single query rather than per-node:
    #
    #   queue_names = {queue for _, _, queue, _ in parsed.values()}
    #   unknown = queue_names - await state_manager.get_known_queue_names(queue_names)
    #   if unknown:
    #       raise MermaidParseError(f"Unknown queues: {', '.join(sorted(unknown))}")
    #
    # parse_mermaid_dag would need to become async and accept a StateManager (or a
    # callable) to do this.  Alternatively, validation can be deferred to the route
    # handler, which already has a StateManager in scope, by exposing the queue set
    # as a separate helper function.

    # ── Parse decision node router labels ─────────────────────────────────────
    decision_routers: dict[str, tuple[str, dict[str, Any]]] = {}
    for d_id, label in decision_node_labels.items():
        m = _ROUTER_LABEL_RE.match(label.strip())
        if not m:
            raise MermaidParseError(
                f"Decision node '{d_id}': invalid router label {label!r}. "
                "Expected: router_name[(key=val, ...)]"
            )
        router_name = m.group("name")
        router_params = _parse_params(m.group("params") or "")
        decision_routers[d_id] = (router_name, router_params)

    # ── Build DAGNode objects ─────────────────────────────────────────────────
    dag_nodes: dict[str, DAGNode] = {
        nid: DAGNode(name, version=version, queue=queue, parameters=params, task_id=ULID())
        for nid, (name, version, queue, params) in parsed.items()
    }

    # ── Compute in-degrees for fan-in detection (regular edges only) ──────────
    predecessors: dict[str, list[str]] = {nid: [] for nid in all_ids}
    for src, dst in regular_success_edges:
        predecessors[dst].append(src)

    # ── Error callback map ────────────────────────────────────────────────────
    error_map: dict[str, str] = {}
    for src, dst in error_edges:
        if src in error_map and error_map[src] != dst:
            raise MermaidParseError(
                f"Node '{src}' has multiple '-.->'' error edges; at most one is allowed."
            )
        error_map[src] = dst

    # ── Fan-in collectors (regular multi-predecessor edges) ───────────────────
    fan_in_collectors: set[str] = {
        dst for dst, srcs in predecessors.items() if len(srcs) >= 2
    }

    # ── Wire regular success edges ────────────────────────────────────────────
    for src, dst in regular_success_edges:
        error_nid = error_map.get(src)
        on_error = dag_nodes[error_nid] if error_nid else None
        if dst in fan_in_collectors:
            DAGNode.merge(dag_nodes[src], into=dag_nodes[dst], on_error=on_error)
        else:
            dag_nodes[src].then(dag_nodes[dst], on_error=on_error)

    # ── Wire dynamic fan-out (decision nodes) ─────────────────────────────────
    consumed_fanin_sources: set[str] = set()
    for d_id, (router_name, router_params) in decision_routers.items():
        dispatcher_id = dispatcher_for.get(d_id)
        branch_start_id = branch_start_for.get(d_id)
        if dispatcher_id is None:
            raise MermaidParseError(
                f"Decision node '{d_id}' has no incoming '-->' edge (no dispatcher)."
            )
        if branch_start_id is None:
            raise MermaidParseError(
                f"Decision node '{d_id}' has no outgoing '-->' edge (no branch start)."
            )

        # Walk from the branch start through regular --> edges to find the leaf
        # node that carries the --o edge to the collector.
        visited_walk: set[str] = {branch_start_id}
        last_branch_id = branch_start_id
        while last_branch_id not in fanin_map:
            next_nodes = [dst for src, dst in regular_success_edges if src == last_branch_id]
            if not next_nodes:
                raise MermaidParseError(
                    f"Branch from decision node '{d_id}' starting at '{branch_start_id}' "
                    "has no '--o' edge to a collector."
                )
            if len(next_nodes) > 1:
                raise MermaidParseError(
                    f"Branch from decision node '{d_id}': node '{last_branch_id}' "
                    "has multiple outgoing '-->' edges within the branch (ambiguous)."
                )
            next_id = next_nodes[0]
            if next_id in visited_walk:
                raise MermaidParseError(
                    f"Cycle detected in branch from decision node '{d_id}'."
                )
            visited_walk.add(next_id)
            last_branch_id = next_id

        consumed_fanin_sources.add(last_branch_id)
        collector_id = fanin_map[last_branch_id]

        error_nid = error_map.get(dispatcher_id)
        on_error = dag_nodes[error_nid] if error_nid else None
        dag_nodes[dispatcher_id].fanout(
            dag_nodes[branch_start_id],
            into=dag_nodes[collector_id],
            router=router_name,
            router_params=router_params,
            on_error=on_error,
        )

    # Validate: every --o edge must be part of a decision-node branch.
    unconsumed = set(fanin_map) - consumed_fanin_sources
    if unconsumed:
        raise MermaidParseError(
            f"'--o' edge(s) from {sorted(unconsumed)!r} are not reachable from any "
            "decision node. '--o' may only be used inside dynamic fan-out branches."
        )

    # ── Determine root nodes ──────────────────────────────────────────────────
    # Roots have no incoming regular success edges and are not special nodes.
    error_targets: set[str] = set(error_map.values())
    roots = [
        dag_nodes[nid]
        for nid in all_ids
        if not predecessors.get(nid)
        and nid not in error_targets
        and nid not in branch_start_ids
        and nid not in collector_ids
    ]

    if not roots:
        raise MermaidParseError(
            "No root nodes found — the graph may contain a cycle or all nodes "
            "are reachable from another node."
        )

    return roots


# ── Generator ────────────────────────────────────────────────────────────────


def _spec_label(spec: DAGTaskSpec, status: TaskStatus | None) -> str:
    """Build the mermaid label string for a DAGTaskSpec node."""
    label = spec.name
    if spec.version != 0:
        label += f"@{spec.version}"
    if spec.queue != "default":
        label += f":{spec.queue}"
    params_str = _serialize_params(spec.parameters)
    if params_str:
        label += f"({params_str})"
    if status is not None:
        label += f"{{{status.value}}}"
    return label


def dag_spec_to_mermaid(
    spec: DAGTaskSpec,
    task_statuses: dict[str, TaskStatus] | None = None,
) -> str:
    """
    Generate a mermaid ``flowchart TD`` diagram from a :class:`~jobbers.models.dag.DAGTaskSpec` tree.

    :param spec: Root task spec; the tree is walked depth-first.
    :param task_statuses: Optional mapping of task-id string →
        :class:`~jobbers.models.task_status.TaskStatus` for colour-coding.
        When provided, a ``{STATUS}`` section is appended to each label and
        ``:::classname`` suffixes are added.
    :returns: Mermaid text including a ``classDef`` block (always present for
        frontend rendering convenience, even when no statuses are supplied).
    """
    node_lines: dict[str, str] = {}
    edge_lines: list[str] = []
    edge_seen: set[tuple[str, str, str]] = set()
    visited: set[str] = set()

    def _walk_template(s: DAGTaskSpec, collector_id: str) -> None:
        """Walk a branch template chain; emit ``--o`` at the leaf instead of ``-->``."""
        spec_id = str(s.id)
        if spec_id in visited:
            return
        visited.add(spec_id)

        status = task_statuses.get(spec_id) if task_statuses else None
        label = _spec_label(s, status)
        class_sfx = f":::{_STATUS_CLASS[status]}" if status else ""
        node_lines[spec_id] = f'    {spec_id}["{label}"]{class_sfx}'

        if not s.dag_callbacks:
            # Leaf node — emit the fan-in edge to the collector.
            edge_key = (spec_id, collector_id, "--o")
            if edge_key not in edge_seen:
                edge_seen.add(edge_key)
                edge_lines.append(f"    {spec_id} --o {collector_id}")
            return

        for cb in s.dag_callbacks:
            if not isinstance(cb, DynamicFanOutCallback):
                child_id = str(cb.task.id)
                edge_key = (spec_id, child_id, "-->")
                if edge_key not in edge_seen:
                    edge_seen.add(edge_key)
                    edge_lines.append(f"    {spec_id} --> {child_id}")
                _walk_template(cb.task, collector_id)

    def _walk(s: DAGTaskSpec) -> None:
        sid = str(s.id)
        if sid in visited:
            return
        visited.add(sid)

        status = task_statuses.get(sid) if task_statuses else None
        label = _spec_label(s, status)
        class_sfx = f":::{_STATUS_CLASS[status]}" if status else ""
        node_lines[sid] = f'    {sid}["{label}"]{class_sfx}'

        for cb in s.dag_callbacks:
            if isinstance(cb, DynamicFanOutCallback):
                collector_id = str(cb.collector.id)
                # Synthetic diamond node — deterministic short ID.
                diamond_id = f"d_{collector_id[:8]}"
                router_label = cb.router_name
                if cb.router_params:
                    router_label += f"({_serialize_params(cb.router_params)})"
                if diamond_id not in node_lines:
                    node_lines[diamond_id] = f'    {diamond_id}{{"{router_label}"}}'

                # Dispatcher → diamond
                d_key = (sid, diamond_id, "-->")
                if d_key not in edge_seen:
                    edge_seen.add(d_key)
                    edge_lines.append(f"    {sid} --> {diamond_id}")

                # Diamond → template root
                template_root_id = str(cb.child_template.id)
                t_key = (diamond_id, template_root_id, "-->")
                if t_key not in edge_seen:
                    edge_seen.add(t_key)
                    edge_lines.append(f"    {diamond_id} --> {template_root_id}")

                # Walk the branch template (emits --o at leaf) then the collector.
                _walk_template(cb.child_template, collector_id)
                _walk(cb.collector)

                if cb.error_callback is not None:
                    err_id = str(cb.error_callback.id)
                    err_key = (sid, err_id, "-.->")
                    if err_key not in edge_seen:
                        edge_seen.add(err_key)
                        edge_lines.append(f"    {sid} -.-> {err_id}")
                    _walk(cb.error_callback)
            else:
                # SimpleCallback or FanInCallback — both expose .task
                child_id = str(cb.task.id)
                edge_key = (sid, child_id, "-->")
                if edge_key not in edge_seen:
                    edge_seen.add(edge_key)
                    edge_lines.append(f"    {sid} --> {child_id}")

                if cb.error_callback is not None:
                    err_id = str(cb.error_callback.id)
                    err_key = (sid, err_id, "-.->")
                    if err_key not in edge_seen:
                        edge_seen.add(err_key)
                        edge_lines.append(f"    {sid} -.-> {err_id}")
                    _walk(cb.error_callback)

                _walk(cb.task)

    _walk(spec)

    lines = ["flowchart TD", ""]
    lines.extend(node_lines.values())
    lines.append("")
    lines.extend(edge_lines)
    lines.append("")
    lines.append(_CLASS_DEFS)
    return "\n".join(lines)
