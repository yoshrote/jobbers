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
from typing import Any, NamedTuple

from ulid import ULID

from jobbers.models.dag import DAGNode, DAGTaskSpec, DynamicFanOutCallback, FanInCallback, SimpleCallback
from jobbers.models.task_status import TaskStatus


class MermaidParseError(ValueError):
    """Raised when mermaid DAG text cannot be parsed into a DAGNode graph."""


class Edge(NamedTuple):
    """A directed edge between two mermaid node identifiers."""

    src: str
    dst: str


class ParsedLabel(NamedTuple):
    """Parsed components of a mermaid node label."""

    name: str
    version: int
    queue: str
    params: dict[str, Any]


class FanOutEdge(NamedTuple):
    """A ``-->>`` edge: dispatcher fans out into arm_root; items_key names the results list."""

    src: str
    dst: str
    items_key: str = "items"


class FanInEdge(NamedTuple):
    """A ``--o`` edge: arm terminal fans into collector."""

    src: str
    dst: str


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


def _parse_label(label: str) -> ParsedLabel:
    """
    Parse a node label into a :class:`ParsedLabel`.

    Strips the reserved ``{status}`` suffix before parsing so that
    output diagrams can be fed back in without modification.
    """
    label = _STATUS_SUFFIX_RE.sub("", label).strip()
    m = _LABEL_RE.match(label)
    if not m:
        raise MermaidParseError(
            f"Invalid node label {label!r}. Expected: task_name[@version][:queue][(key=val, ...)]"
        )
    name = m.group("name")
    version = int(m.group("version")) if m.group("version") is not None else 0
    queue = m.group("queue") or "default"
    params_str = m.group("params") or ""
    params = _parse_params(params_str) if params_str.strip() else {}
    return ParsedLabel(name, version, queue, params)


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
    success_edges: list[Edge],
    error_edges: list[Edge],
    fanout_edges: list[FanOutEdge],
    fanin_edges: list[FanInEdge],
) -> None:
    """
    Parse edge patterns from a cleaned mermaid line.

    Handles chained edges on one line (``A --> B --> C``) and mixed
    inline node+edge syntax after node labels have been stripped.

    Supported operators:

    - ``-->``         success callback (or fan-in when dest has ≥ 2 sources)
    - ``-.->``        error callback
    - ``-->>``        dynamic fan-out (dispatcher → arm root); optional label sets items_key
    - ``--"key">>``   dynamic fan-out with explicit items_key
    - ``--o``         fan-in boundary (arm terminal → collector)
    """
    # Split on edge operators in longest-match order so -->> is matched before -->.
    tokens = re.split(r'\s*(--"([^"]*)">>|-->>|--o|-\.->|-->)\s*', line)
    # re.split with a group produces: [pre, full_match, group1, post, full_match, group1, post, ...]
    # stride is 3 (full_match + one capture group).
    i = 1
    while i < len(tokens) - 1:
        op = tokens[i]
        label_cap = tokens[i + 1]  # the captured label inside --"...">> or None
        src_m = re.search(r"\b(\w+)\s*$", tokens[i - 1] if i >= 1 else "")
        dst_m = re.match(r"\s*(\w+)", tokens[i + 2])
        i += 3
        if not src_m or not dst_m:
            continue
        src, dst = src_m.group(1), dst_m.group(1)
        if op == "-->" :
            success_edges.append(Edge(src, dst))
        elif op == "-.->":
            error_edges.append(Edge(src, dst))
        elif op == "-->>":
            fanout_edges.append(FanOutEdge(src, dst, "items"))
        elif op.startswith('--"') and op.endswith(">>"):
            items_key = label_cap or "items"
            fanout_edges.append(FanOutEdge(src, dst, items_key))
        elif op == "--o":
            fanin_edges.append(FanInEdge(src, dst))


def _lex_mermaid(
    text: str,
) -> tuple[dict[str, str], list[Edge], list[Edge], list[FanOutEdge], list[FanInEdge]]:
    """
    Extract node definitions and edges from mermaid flowchart text.

    Returns ``(node_labels, success_edges, error_edges, fanout_edges, fanin_edges)`` where:

    - ``node_labels`` maps node identifier → raw label string
    - ``success_edges`` is a list of ``Edge(src, dst)`` for ``-->`` edges
    - ``error_edges`` is a list of ``Edge(src, dst)`` for ``-.->`` edges
    - ``fanout_edges`` is a list of ``FanOutEdge(src, dst, items_key)`` for ``-->>`` edges
    - ``fanin_edges`` is a list of ``FanInEdge(src, dst)`` for ``--o`` edges
    """
    node_labels: dict[str, str] = {}
    success_edges: list[Edge] = []
    error_edges: list[Edge] = []
    fanout_edges: list[FanOutEdge] = []
    fanin_edges: list[FanInEdge] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or any(line.startswith(p) for p in _SKIP_PREFIXES):
            continue
        # Extract node label definitions anywhere on the line.
        for m in _NODE_RE.finditer(line):
            node_labels[m.group(1)] = (m.group(2) or m.group(3) or m.group(4) or "").strip()
        # Strip node definitions and class suffixes, then extract edges.
        cleaned = _NODE_RE.sub(lambda m: m.group(1), line)
        cleaned = _CLASS_SUFFIX_RE.sub("", cleaned)
        _extract_edges_from_line(cleaned, success_edges, error_edges, fanout_edges, fanin_edges)

    return node_labels, success_edges, error_edges, fanout_edges, fanin_edges


# ── Parser ────────────────────────────────────────────────────────────────────


def parse_mermaid_dag(text: str) -> list[DAGNode]:
    """
    Parse a mermaid flowchart into a list of root :class:`~jobbers.models.dag.DAGNode` objects.

    The returned nodes are fully wired (``then`` / ``merge``) and ready for
    :meth:`~jobbers.state_manager.StateManager.submit_dag`.

    :param text: Raw mermaid ``flowchart TD`` text.
    :returns: Root ``DAGNode`` instances — nodes with no incoming ``-->`` edges.
    :raises MermaidParseError: If the text contains invalid node labels, multiple
        error callbacks on the same source, or no reachable root nodes.
    """
    node_labels, success_edges, error_edges, fanout_edges, fanin_edges = _lex_mermaid(text)

    all_edge_count = len(success_edges) + len(fanout_edges) + len(fanin_edges)
    if not node_labels and not all_edge_count:
        raise MermaidParseError("No nodes found in the mermaid text.")

    # Validate fanout/fanin pairings up-front.
    # Each dispatcher may have at most one -->> edge; each --o target identifies one collector.
    fanout_dispatchers: dict[str, FanOutEdge] = {}
    for fo in fanout_edges:
        if fo.src in fanout_dispatchers:
            raise MermaidParseError(
                f"Node '{fo.src}' has multiple '-->>'' fan-out edges; at most one is allowed."
            )
        fanout_dispatchers[fo.src] = fo

    fanin_map: dict[str, FanInEdge] = {}  # arm_terminal → FanInEdge
    for fi in fanin_edges:
        if fi.src in fanin_map:
            raise MermaidParseError(
                f"Node '{fi.src}' has multiple '--o'' fan-in edges; at most one is allowed."
            )
        fanin_map[fi.src] = fi

    # Auto-register nodes that appear only in edges (no explicit label definition).
    all_ids: set[str] = set(node_labels)
    for edge in (
        list(success_edges)
        + list(error_edges)
        + [Edge(fo.src, fo.dst) for fo in fanout_edges]
        + [Edge(fi.src, fi.dst) for fi in fanin_edges]
    ):
        for nid in (edge.src, edge.dst):
            if nid not in all_ids:
                node_labels[nid] = nid  # label defaults to the node identifier
                all_ids.add(nid)

    # Parse each label → ParsedLabel(name, version, queue, params).
    parsed: dict[str, ParsedLabel] = {}
    for nid, label in node_labels.items():
        try:
            parsed[nid] = _parse_label(label)
        except MermaidParseError as exc:
            raise MermaidParseError(f"Node '{nid}': {exc}") from exc

    # TODO: validate queue names against the database.
    # All queue names referenced by this DAG are known at this point; collect them
    # with a set comprehension and resolve in a single query rather than per-node:
    #
    #   queue_names = {pl.queue for pl in parsed.values()}
    #   unknown = queue_names - await state_manager.get_known_queue_names(queue_names)
    #   if unknown:
    #       raise MermaidParseError(f"Unknown queues: {', '.join(sorted(unknown))}")
    #
    # parse_mermaid_dag would need to become async and accept a StateManager (or a
    # callable) to do this.  Alternatively, validation can be deferred to the route
    # handler, which already has a StateManager in scope, by exposing the queue set
    # as a separate helper function.

    # Build DAGNode objects with pre-assigned ULIDs.
    dag_nodes: dict[str, DAGNode] = {
        nid: DAGNode(pl.name, version=pl.version, queue=pl.queue, parameters=pl.params, task_id=ULID())
        for nid, pl in parsed.items()
    }

    # Nodes that are part of a fanout arm subgraph: arm roots and any nodes reachable
    # from them via --> edges up to (but not including) the collector.
    arm_root_ids: set[str] = {fo.dst for fo in fanout_edges}
    collector_ids: set[str] = {fi.dst for fi in fanin_edges}

    # Identify arm-internal edges: --> edges whose source is an arm root or
    # reachable from one, and whose destination is not a collector.
    # We do a BFS from each arm root through --> edges to find all arm nodes.
    arm_node_ids: set[str] = set(arm_root_ids)
    changed = True
    while changed:
        changed = False
        for edge in success_edges:
            if edge.src in arm_node_ids and edge.dst not in collector_ids:
                if edge.dst not in arm_node_ids:
                    arm_node_ids.add(edge.dst)
                    changed = True

    # Compute in-degrees for fan-in detection among non-arm, non-fanout edges.
    predecessors: dict[str, list[str]] = {nid: [] for nid in all_ids}
    for edge in success_edges:
        if edge.src not in arm_node_ids and edge.dst not in arm_node_ids:
            predecessors[edge.dst].append(edge.src)

    # Build error-callback map: source → error target (at most one per source).
    error_map: dict[str, str] = {}
    for edge in error_edges:
        if edge.src in error_map and error_map[edge.src] != edge.dst:
            raise MermaidParseError(
                f"Node '{edge.src}' has multiple '-.->'' error edges; at most one is allowed."
            )
        error_map[edge.src] = edge.dst

    # Fan-in collectors: destinations with ≥ 2 incoming non-arm success edges.
    fan_in_collectors: set[str] = {dst for dst, srcs in predecessors.items() if len(srcs) >= 2}

    # Wire non-arm success edges.
    for edge in success_edges:
        if edge.src in arm_node_ids or edge.dst in arm_node_ids:
            continue  # arm-internal edges are wired into the arm spec below
        error_nid = error_map.get(edge.src)
        on_error = dag_nodes[error_nid] if error_nid else None
        if edge.dst in fan_in_collectors:
            DAGNode.merge(dag_nodes[edge.src], into=dag_nodes[edge.dst], on_error=on_error)
        else:
            dag_nodes[edge.src].then(dag_nodes[edge.dst], on_error=on_error)

    # Wire arm-internal success edges.
    arm_predecessors: dict[str, list[str]] = {nid: [] for nid in arm_node_ids}
    for edge in success_edges:
        if edge.src in arm_node_ids and edge.dst in arm_node_ids:
            arm_predecessors[edge.dst].append(edge.src)

    arm_fan_in_collectors: set[str] = {
        dst for dst, srcs in arm_predecessors.items() if len(srcs) >= 2
    }
    for edge in success_edges:
        if edge.src not in arm_node_ids or edge.dst not in arm_node_ids:
            continue
        error_nid = error_map.get(edge.src)
        on_error = dag_nodes[error_nid] if error_nid else None
        if edge.dst in arm_fan_in_collectors:
            DAGNode.merge(dag_nodes[edge.src], into=dag_nodes[edge.dst], on_error=on_error)
        else:
            dag_nodes[edge.src].then(dag_nodes[edge.dst], on_error=on_error)

    # Build DynamicFanOutCallback on each dispatcher node.
    # The arm_root spec and collector spec are serialised from their DAGNodes.
    for dispatcher_id, fo in fanout_dispatchers.items():
        arm_root_node = dag_nodes[fo.dst]
        # Find the arm terminal: the arm node with a --o edge from fanin_map.
        arm_terminal_id: str | None = next(
            (fi.src for fi in fanin_edges if fi.src in arm_node_ids and fi.dst in collector_ids),
            None,
        )
        if arm_terminal_id is None:
            raise MermaidParseError(
                f"Fan-out from '{dispatcher_id}' has no '--o'' fan-in boundary edge. "
                "Add a '--o' edge from the arm terminal to a collector node."
            )
        fi = fanin_map[arm_terminal_id]
        collector_node = dag_nodes[fi.dst]
        error_nid = error_map.get(dispatcher_id)
        on_error_spec = dag_nodes[error_nid].to_spec() if error_nid else None
        cb = DynamicFanOutCallback(
            arm_root=arm_root_node.to_spec(),
            collector=collector_node.to_spec(),
            items_key=fo.items_key,
            error_callback=on_error_spec,
        )
        dag_nodes[dispatcher_id].add_fanout_callback(cb)

    # Roots: dispatcher nodes and top-level nodes with no incoming non-arm success edges
    # that are not error targets and not arm nodes and not collectors.
    error_targets: set[str] = set(error_map.values())
    excluded = arm_node_ids | collector_ids | error_targets
    roots = [
        dag_nodes[nid]
        for nid in all_ids
        if not predecessors.get(nid) and nid not in excluded
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
                arm_id = str(cb.arm_root.id)
                col_id = str(cb.collector.id)
                for fo_src, fo_dst, fo_arrow in [
                    (sid, arm_id, "-->>"),
                    (arm_id, col_id, "--o"),
                ]:
                    key = (fo_src, fo_dst, fo_arrow)
                    if key not in edge_seen:
                        edge_seen.add(key)
                        edge_lines.append(f"    {fo_src} {fo_arrow} {fo_dst}")
                if cb.error_callback is not None:
                    err_id = str(cb.error_callback.id)
                    err_key = (sid, err_id, "-.->")
                    if err_key not in edge_seen:
                        edge_seen.add(err_key)
                        edge_lines.append(f"    {sid} -.-> {err_id}")
                    _walk(cb.error_callback)
                _walk(cb.arm_root)
                _walk(cb.collector)
                continue

            if not isinstance(cb, (SimpleCallback, FanInCallback)):
                continue
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
