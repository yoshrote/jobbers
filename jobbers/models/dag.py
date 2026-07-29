"""
DAG node classes for describing task dependency graphs.

- `DAGTaskSpec` — pre-configured task specification with a pre-assigned ULID.
- `SimpleCallback` — submit a task immediately when the parent completes.
- `FanInCallback` — submit a task only after *all* fan-in predecessors complete;
  uses a Redis set to track remaining predecessor IDs.
- `DAGCallback` — discriminated union of the callback types.
- `DAGNode` — fluent builder for constructing the DAG graph; call `to_task()` on
  root nodes to get `Task` objects ready for submission via `StateManager.submit_dag`.

## Usage

**Linear chain:**

```python
a = DAGNode("fetch_data")
b = DAGNode("process_data")
c = DAGNode("save_results")
a.then(b)
b.then(c)
```

**Fan-out then fan-in (diamond):**

```python
root = DAGNode("split_work")
branch_a = DAGNode("process_chunk_a")
branch_b = DAGNode("process_chunk_b")
collector = DAGNode("merge_results")

root.then(branch_a, branch_b)
DAGNode.merge(branch_a, branch_b, into=collector)
```

**Error callbacks:**

Pass `on_error` to `then()` or `merge()` to submit a task when a node fails
permanently (status `FAILED`, `CANCELLED`, `STALLED`, or `DROPPED`). The error
task receives `parent_ids=[failing_task.id]` so it can look up the failure
details via `await get_current_task().parent_results()`.

```python
# Fire a notification task if "process_data" fails:
err = DAGNode("notify_failure", parameters={"channel": "ops"})
a.then(b, on_error=err)

# Fire a shared error handler if any fan-in predecessor fails:
err = DAGNode("handle_pipeline_error")
DAGNode.merge(branch_a, branch_b, into=collector, on_error=err)
```

Error callbacks only fire on *permanent* failure — tasks that are being retried
do not trigger them. The error node itself is a plain `DAGNode` and can have its
own `then()` chain if further steps are needed on failure.
"""

from __future__ import annotations

import datetime as dt  # noqa: TC003 -- resolved at runtime by Pydantic (DAGRunSummary.submitted_at)
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Literal, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field, field_serializer
from ulid import ULID

# ---------------------------------------------------------------------------
# Serialisable specs (stored inside Task.dag_callbacks in Redis)
# ---------------------------------------------------------------------------


class DAGTaskSpec(BaseModel):
    """
    Serialisable specification for a single task node.

    The `id` is pre-assigned at DAG construction time so that fan-in sets can
    reference it before the task is ever submitted.
    """

    id: ULID = Field(default_factory=ULID)
    name: str
    queue: str = "default"
    version: int = 0
    parameters: dict[str, Any] = {}
    dag_callbacks: list[DAGCallback] = []

    @field_serializer("id", when_used="json")
    def serialize_id(self, value: ULID) -> str:
        return str(value)

    def fresh_copy(self) -> tuple[DAGTaskSpec, dict[ULID, ULID]]:
        """
        Return a copy of this spec tree with brand-new ULIDs for every node.

        Fan-in keys (`dag:fan-in:{old_id}`) are rewritten to reference the new
        collector IDs so runs of the same cron entry never share Redis keys.

        Returns `(fresh_spec, id_map)` where `id_map` maps old → new ULID.
        """
        id_map: dict[ULID, ULID] = {}
        return self._remap(id_map), id_map

    def _remap(self, id_map: dict[ULID, ULID]) -> DAGTaskSpec:
        """Recursively rebuild this spec with remapped ULIDs and fan_in_keys."""
        new_id = id_map.setdefault(self.id, ULID())
        new_callbacks: list[DAGCallback] = []
        for cb in self.dag_callbacks:
            if isinstance(cb, SimpleCallback):
                new_err = cb.error_callback._remap(id_map) if cb.error_callback else None
                new_callbacks.append(
                    SimpleCallback(
                        task=cb.task._remap(id_map),
                        error_callback=new_err,
                    )
                )
            elif isinstance(cb, FanInCallback):
                new_err = cb.error_callback._remap(id_map) if cb.error_callback else None
                new_child = cb.task._remap(id_map)
                new_collector_id = id_map.setdefault(cb.task.id, new_child.id)
                new_fan_in_key = f"dag:fan-in:{new_collector_id}"
                new_callbacks.append(
                    FanInCallback(
                        task=new_child,
                        fan_in_key=new_fan_in_key,
                        error_callback=new_err,
                    )
                )
            else:
                # DynamicFanOutCallback: remap arm_root and collector specs
                new_err = cb.error_callback._remap(id_map) if cb.error_callback else None
                new_callbacks.append(
                    DynamicFanOutCallback(
                        arm_root=cb.arm_root._remap(id_map),
                        collector=cb.collector._remap(id_map),
                        items_key=cb.items_key,
                        fan_in_ttl=cb.fan_in_ttl,
                        propagate_fan_in=cb.propagate_fan_in,
                        error_callback=new_err,
                    )
                )
        return DAGTaskSpec(
            id=new_id,
            name=self.name,
            queue=self.queue,
            version=self.version,
            parameters=self.parameters,
            dag_callbacks=new_callbacks,
        )


class SimpleCallback(BaseModel):
    """Submit `task` immediately when the parent task completes."""

    type: Literal["simple"] = "simple"
    task: DAGTaskSpec
    error_callback: DAGTaskSpec | None = None  # Submit when the parent task fails permanently


class FanInCallback(BaseModel):
    """
    Submit `task` only once all fan-in predecessors have completed.

    `fan_in_key` is a Redis key for a set of pending predecessor task IDs.
    Each predecessor removes its own ID via `TaskStateProtocol.fan_in_complete`;
    when the set becomes empty the collector task is submitted.
    """

    type: Literal["fan_in"] = "fan_in"
    task: DAGTaskSpec
    fan_in_key: str  # Redis SSET key tracking remaining predecessors
    error_callback: DAGTaskSpec | None = None  # Submit when this predecessor fails permanently


class DynamicFanOutCallback(BaseModel):
    """
    Declarative dynamic fan-out driven by the dispatcher task's result data.

    The processor reads arm parameters from the dispatcher task's results and
    spawns one arm instance per entry.

    ``arm_root`` is a template ``DAGTaskSpec`` for the root of each arm chain.
    Each entry in ``dispatcher.results[items_key]`` (a list of dicts) is
    shallow-merged into the arm root's parameters (entry values take precedence
    over the template's static parameters).

    ``collector`` is submitted once all arm terminals have completed (fan-in).

    ``propagate_fan_in`` mirrors the same field on ``DynamicFanOut``: when
    ``True`` (the default), if this dispatcher is itself an arm of an outer
    fan-in, the outer fan-in responsibility is transferred to ``collector``
    so the outer fan-in waits for the collector rather than the dispatcher.

    This callback type is produced by the mermaid parser when it encounters a
    ``-->>`` fan-out edge.  Task functions that need runtime control over arm
    structure should continue to use ``DynamicFanOut`` / ``TaskResult`` instead.
    """

    type: Literal["dynamic_fanout"] = "dynamic_fanout"
    arm_root: DAGTaskSpec
    collector: DAGTaskSpec
    items_key: str = "items"
    fan_in_ttl: int = 86400
    propagate_fan_in: bool = True
    error_callback: DAGTaskSpec | None = None


# Pydantic discriminated union – serialises/deserialises by the ``type`` field.
DAGCallback = Annotated[SimpleCallback | FanInCallback | DynamicFanOutCallback, Field(discriminator="type")]

# Allow self-referential DAGTaskSpec.dag_callbacks.
DAGTaskSpec.model_rebuild()


def collect_fan_in_keys(spec: DAGTaskSpec) -> dict[str, set[ULID]]:
    """
    Walk a DAGTaskSpec tree and return a mapping of fan_in_key → set of predecessor task IDs.

    Used to pre-populate Redis fan-in tracking sets before submitting a DAG rooted at *spec*.
    """
    result: dict[str, set[ULID]] = {}
    visited: set[ULID] = set()

    def _walk(s: DAGTaskSpec) -> None:
        if s.id in visited:
            return
        visited.add(s.id)
        for cb in s.dag_callbacks:
            if isinstance(cb, DynamicFanOutCallback):
                # Arm fan-in sets are initialised at runtime by the processor, so
                # arm_root is intentionally not walked here. The collector, however,
                # may itself feed into further static fan-ins (e.g. `collector --> D`
                # in a mermaid diagram) — those need pre-populating like any other
                # static edge, so walk into it.
                _walk(cb.collector)
                continue
            if isinstance(cb, FanInCallback):
                result.setdefault(cb.fan_in_key, set()).add(s.id)
            _walk(cb.task)

    _walk(spec)
    return result


# ---------------------------------------------------------------------------
# Builder (not stored in Redis directly – used to construct Tasks)
# ---------------------------------------------------------------------------


class DAGNode:
    """
    Fluent builder for constructing a task DAG.

    Each node is assigned a `ULID` at construction time.  Nodes are linked
    via `then` (fan-out / chain) and `merge` (fan-in).  When the graph is fully
    described, call `to_task` on each root node to obtain a `Task` ready for
    submission.  Pass all root nodes to `StateManager.submit_dag` so that
    fan-in Redis sets are initialised before execution begins.
    """

    def __init__(
        self,
        name: str,
        *,
        queue: str = "default",
        version: int = 0,
        parameters: dict[str, Any] | None = None,
        task_id: ULID | None = None,
    ) -> None:
        self._id: ULID = task_id or ULID()
        self._name = name
        self._queue = queue
        self._version = version
        self._parameters: dict[str, Any] = parameters or {}
        # (successor_node, fan_in_key or None, error_node or None)
        self._successors: list[tuple[DAGNode, str | None, DAGNode | None]] = []
        # DynamicFanOutCallback entries declared via the mermaid parser (-->> / --o edges).
        self._fanout_callbacks: list[DynamicFanOutCallback] = []

    @property
    def id(self) -> ULID:
        """Pre-assigned task ULID for this node."""
        return self._id

    # ------------------------------------------------------------------
    # Graph construction helpers
    # ------------------------------------------------------------------

    def then(
        self,
        *nodes: DAGNode,
        on_error: DAGNode | None = None,
    ) -> DAGNode:
        """
        Chain: each of *nodes* runs immediately after *this* node completes.

        Pass `on_error` to also submit an error task when *this* node fails permanently.

        Each successor is a chain-position task with exactly one parent (this
        node); annotate its function's parameters with ``FromParent(key)`` to
        pull specific values out of this node's results.

        Returns *self* for fluent chaining:

        ```python
        a.then(b).then(c)  # same as a.then(b); b.then(c)
        ```
        """
        for node in nodes:
            self._successors.append((node, None, on_error))
        return self

    @classmethod
    def find_terminals(cls, roots: list[DAGNode]) -> list[DAGNode]:
        """
        Return all leaf nodes (nodes with no successors) reachable from *roots*.

        Used by the processor to determine which nodes should decrement the fan-in
        set when dynamic fanout arms are multi-step chains rather than single tasks.

        Deliberately looks only at `_successors`, not `_fanout_callbacks`: an arm
        that is itself a nested dispatcher is still the correct initial terminal
        here. Its provisional FanInCallback is dynamically delegated to its own
        nested collector at runtime (see `_handle_declarative_fanout` /
        `delegate_fan_in`) when that arm's `DynamicFanOutCallback.propagate_fan_in`
        is True (the default). When it is False, this node completing — not its
        nested tree completing — is exactly what should close the outer fan-in.
        """
        terminals: list[DAGNode] = []
        visited: set[int] = set()

        def _walk(node: DAGNode) -> None:
            if id(node) in visited:
                return
            visited.add(id(node))
            if not node._successors:
                terminals.append(node)
            else:
                for successor, _, _ in node._successors:
                    _walk(successor)

        for root in roots:
            _walk(root)
        return terminals

    @classmethod
    def merge(
        cls,
        *predecessors: DAGNode,
        into: DAGNode,
        on_error: DAGNode | None = None,
    ) -> DAGNode:
        """
        Fan-in: `into` runs only after *all* `predecessors` have completed.

        Pass `on_error` to submit an error task when any predecessor fails permanently.

        `into` always receives 2+ parents (one per predecessor), so any of its
        function's parameters annotated ``FromParent(key)`` must use
        ``many=True`` to collect values across all of them — see
        ``validate_fan_in_cardinality``, which raises ``FanInCardinalityError``
        at this call site if `into` declares a singular (``many=False``)
        ``FromParent`` param, since that annotation can never be satisfied here.

        A shared Redis set key derived from `into`'s task ID is stored on
        each predecessor's callback so the worker knows which set to decrement.
        Returns `into` for further chaining:

        ```python
        DAGNode.merge(branch_a, branch_b, into=collector).then(next_step)
        ```
        """
        validate_fan_in_cardinality(predecessors, into)
        fan_in_key = f"dag:fan-in:{into._id}"
        for pred in predecessors:
            pred._successors.append((into, fan_in_key, on_error))
        return into

    # ------------------------------------------------------------------
    # Conversion to serialisable / submittable form
    # ------------------------------------------------------------------

    def to_spec(self) -> DAGTaskSpec:
        """Recursively build a `DAGTaskSpec` with embedded callbacks."""
        return DAGTaskSpec(
            id=self._id,
            name=self._name,
            queue=self._queue,
            version=self._version,
            parameters=self._parameters,
            dag_callbacks=self._callbacks_recursive(),
        )

    def add_fanout_callback(self, cb: DynamicFanOutCallback) -> None:
        """Attach a declarative ``DynamicFanOutCallback`` to this node (used by the mermaid parser)."""
        self._fanout_callbacks.append(cb)

    def _callbacks_recursive(self) -> list[DAGCallback]:
        """Return the list of `DAGCallback` objects for this node's successors."""
        callbacks: list[DAGCallback] = []
        for successor, fan_in_key, error_node in self._successors:
            spec = successor.to_spec()
            error_spec = error_node.to_spec() if error_node is not None else None
            if fan_in_key is None:
                callbacks.append(
                    SimpleCallback(
                        task=spec,
                        error_callback=error_spec,
                    )
                )
            else:
                callbacks.append(
                    FanInCallback(
                        task=spec,
                        fan_in_key=fan_in_key,
                        error_callback=error_spec,
                    )
                )
        callbacks.extend(self._fanout_callbacks)
        return callbacks

    def to_task(
        self,
        *,
        parent_id: ULID | None = None,
        dag_run_id: ULID | None = None,
        dag_run_name: str | None = None,
    ) -> Task:
        """Return a `Task` for this node ready for submission."""
        from jobbers.models.task import Task

        return Task(
            id=self._id,
            name=self._name,
            queue=self._queue,
            version=self._version,
            parameters=self._parameters,
            dag_callbacks=self._callbacks_recursive(),
            parent_ids=[parent_id] if parent_id is not None else [],
            dag_run_id=dag_run_id,
            dag_run_name=dag_run_name,
        )

    def fan_in_predecessors(self) -> dict[str, set[ULID]]:
        """
        Walk the full subgraph and return a mapping of fan-in key → predecessor IDs.

        Used by `StateManager.submit_dag` to pre-populate Redis sets before any task runs.
        """
        result: dict[str, set[ULID]] = {}
        visited: set[int] = set()

        def _walk(node: DAGNode) -> None:
            if id(node) in visited:
                return
            visited.add(id(node))
            for successor, fan_in_key, _error_node in node._successors:
                if fan_in_key is not None:
                    result.setdefault(fan_in_key, set()).add(node._id)
                _walk(successor)
            # A DynamicFanOutCallback's collector is a fully-resolved DAGTaskSpec
            # subtree (not a DAGNode), so any static fan-in it feeds into has to be
            # collected via collect_fan_in_keys rather than this DAGNode-based walk.
            for cb in node._fanout_callbacks:
                for key, ids in collect_fan_in_keys(cb.collector).items():
                    result.setdefault(key, set()).update(ids)

        _walk(self)
        return result


class FanInCardinalityError(ValueError):
    """Raised when a DAG wires a singular ``FromParent`` param to a fan-in edge."""


class FromParent:
    """
    Marker for pulling a value out of parent task results.

    ```python
    Annotated[int, FromParent("rows")]  # exactly one parent — scalar, or error
    Annotated[int, FromParent("rows")] = 0  # ...falling back to 0 if the key is absent
    Annotated[list[int], FromParent("rows", many=True)]  # every parent that produced "rows" — a list, always
    Annotated[int, FromParent()]  # key defaults to the param name
    ```

    ``many=False`` (the default) requires the task to have exactly one parent
    when it has *any* — a structural (DAG-shape) contract, not a data one. If
    the key is absent from that one parent's results, the parameter is simply
    left unset so the function's own Python default (if any) applies; see
    ``docs/task-definition-reference.md``.

    ``many=True`` resolves to a list — every parent that produced the key, in
    no particular order — including ``[]`` when the task has one or more
    parents but none of them produced the key.

    **Root nodes (zero parents) are not a shape violation for either mode.**
    There is nothing to pull from, so ``FromParent`` leaves the parameter
    unset entirely — the same as a missing key — rather than raising (singular
    mode) or forcing an empty list (``many=True``). This is what makes a
    ``FromParent``-annotated task usable as a root node, or called/submitted
    directly in a test without fabricating a parent: submit it with the value
    as an ordinary parameter (``DAGNode(name, parameters={"rows": 5})`` or
    ``my_task.submit(rows=5)``), or give the function its own Python default.
    Only a *wrong* parent count — 2+ parents on a singular slot — is a real
    structural bug and still raises unconditionally, regardless of what the
    parents' results contain.

    **Fragility warning:** don't stack multiple ``many=True`` params on one
    task expecting their lists to line up positionally (e.g. ``zip(count,
    name)``). Each param is filtered independently by its own key's presence,
    so the lists only correspond entry-for-entry when every parent
    contributing to one also contributes to the other — and nothing detects
    or errors when that assumption breaks; a partial mismatch just silently
    truncates via ``zip()`` and pairs the wrong parents' data. When you need
    several fields from the *same* parent kept together, fetch the raw dicts
    with ``await task.parent_results()`` instead and destructure each entry
    directly — correctness then follows from reading multiple keys off one
    dict, not from independently-resolved lists happening to stay aligned.
    """

    __slots__ = ("key", "many")

    def __init__(self, key: str | None = None, *, many: bool = False) -> None:
        self.key = key
        self.many = many

    def __repr__(self) -> str:
        return f"FromParent({self.key!r}, many={self.many!r})"


def _extract_from_parent(hint: Any) -> FromParent | None:
    """Return the FromParent marker from an Annotated hint, or None."""
    if get_origin(hint) is not Annotated:
        return None
    for meta in get_args(hint)[1:]:
        if isinstance(meta, FromParent):
            return meta
    return None


def validate_fan_in_cardinality(predecessors: tuple[DAGNode, ...], into: DAGNode) -> None:
    """
    Raise ``FanInCardinalityError`` if `into` declares a singular FromParent param.

    ``DAGNode.merge()`` always supplies 2+ parents to `into` by construction,
    so a ``FromParent(..., many=False)`` param on `into`'s function can never
    be satisfied — it always raises at execution time. Catch it here instead,
    at graph-construction time, when the task is already registered.
    """
    if len(predecessors) < 2:
        return

    from jobbers.registry import get_task_config  # deferred: registry imports this module

    task_config = get_task_config(into._name, into._version)
    if task_config is None:
        return  # not registered yet (graph built before task modules imported) — can't validate

    try:
        hints = get_type_hints(task_config.function, include_extras=True)
    except Exception:
        return

    for param_name, hint in hints.items():
        if param_name == "return":
            continue
        fp = _extract_from_parent(hint)
        if fp is not None and not fp.many:
            raise FanInCardinalityError(
                f"DAGNode.merge(): {into._name!r} has a singular FromParent param "
                f"({param_name!r}) but merge() always supplies {len(predecessors)} parents. "
                "Use FromParent(..., many=True)."
            )


@dataclass
class DynamicFanOut:
    """
    Describes runtime fan-out: a dynamic set of arms and a collector.

    Each element of *arms* may be either a single ``DAGNode`` or the root of a
    multi-step sub-chain built with ``.then()`` and ``.merge()``.  The processor
    automatically discovers the terminal (leaf) nodes of each arm and wires the
    fan-in to those terminals, so the collector fires only after every arm has
    fully completed.

    Do NOT call `DAGNode.merge` yourself — the processor does it.
    Embed this in a `TaskResult` to trigger fan-out processing.

    ``propagate_fan_in`` controls behaviour when this task is itself an arm of an
    outer dynamic fanout (i.e. it has a ``FanInCallback`` in its own
    ``dag_callbacks``).  When ``True`` (the default), the processor transfers
    those outer callbacks to the collector so the outer fan-in waits for the
    collector to complete rather than firing as soon as this task dispatches.
    Set to ``False`` only when you explicitly want the outer fan-in to fire the
    moment this task returns, before its own nested fanout finishes.
    """

    arms: list[DAGNode]
    collector: DAGNode
    fan_in_ttl: int = 86400
    propagate_fan_in: bool = True


@dataclass
class TaskResult:
    """
    Return value for all jobber task functions.

    `results` is stored on the task record and made available to downstream
    tasks via `Task.parent_results`.

    `parent_ids` records the immediate parent task ID(s) — one for simple
    chains, many for fan-in collectors.  Use `Task.make_result` to have this
    populated automatically from the running task's context.

    Set `fanout` to trigger dynamic fan-out: the processor wires the fan-in
    automatically, initialises the Redis tracking set, and submits all children.

    **Example — plain result (auto-populated parent_ids):**

    ```python
    @register_task(name="fetch_data")
    async def fetch_data(**kwargs):
        task = get_current_task()
        data = await load()
        return task.make_result(results={"rows": len(data)})
    ```

    **Example — dynamic fan-out:**

    ```python
    @register_task(name="dispatch_records")
    async def dispatch_records(**kwargs):
        task = get_current_task()
        records = await fetch_records()
        arms = [DAGNode("process_record", parameters={"id": r}) for r in records]
        collector = DAGNode("aggregate_results")
        return task.make_result(
            results={"count": len(records)},
            fanout=DynamicFanOut(arms=arms, collector=collector),
        )
    ```
    """

    results: dict[str, Any] = field(default_factory=dict)
    fanout: DynamicFanOut | None = None
    parent_ids: list[ULID] = field(default_factory=list)


class DAGRunPagination(BaseModel):
    "Pagination details for DAG run listings."

    limit: int = Field(default=50, gt=0, le=100)
    offset: int = Field(default=0, ge=0)


class DagRunStatus(StrEnum):
    """Aggregate status of a DAG run, derived from its tasks' terminal outcomes."""

    RUNNING = "running"
    COMPLETE = "complete"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"
    # Cancellation was requested (StateManager.request_dag_cancellation) but at least
    # one task hasn't reached a terminal status yet.
    CANCELLING = "cancelling"
    # Cancellation was requested and every task in the run has reached a terminal status.
    CANCELLED = "cancelled"


# Outcome recorded against a run's aggregate counters when one of its tasks reaches a
# terminal status -- "completed" for TaskStatus.COMPLETED, "failed" for any status in
# TaskStatus.stuck_statuses() (FAILED/STALLED/CANCELLED/DROPPED).
DagRunOutcome = Literal["completed", "failed"]


class DAGRunSummary(BaseModel):
    """One row of a DAG run listing: identity, name, status, submission time."""

    dag_run_id: ULID
    name: str
    status: DagRunStatus
    submitted_at: dt.datetime

    @field_serializer("dag_run_id", when_used="json")
    def serialize_dag_run_id(self, value: ULID) -> str:
        return str(value)


class DAGRunDetail(DAGRunSummary):
    """DAGRunSummary plus the full list of task IDs belonging to the run."""

    task_ids: list[ULID]

    @field_serializer("task_ids", when_used="json")
    def serialize_task_ids(self, value: list[ULID]) -> list[str]:
        return [str(v) for v in value]


# Per-task outcome bucket from a StateManager.request_dag_cancellation sweep.
DAGCancelTaskStatus = Literal["already_terminal", "cancelled", "signalled"]


class DAGCancelTaskResult(BaseModel):
    """One task's outcome from a DAG-run cancellation sweep."""

    task_id: ULID
    status: DAGCancelTaskStatus

    @field_serializer("task_id", when_used="json")
    def serialize_task_id(self, value: ULID) -> str:
        return str(value)


class DAGCancelResult(BaseModel):
    """
    Result of StateManager.request_dag_cancellation.

    ``already_terminal`` + ``cancelled_immediately`` + ``signalled_running`` always
    equals ``len(tasks)``. ``signalled_running`` tasks were not individually cancelled
    here -- a single ``publish_dag_cancellation`` broadcast was sent for the whole run
    regardless of how many tasks were STARTED, so this count does not imply that many
    pub/sub messages were published.
    """

    dag_run_id: ULID
    already_terminal: int
    cancelled_immediately: int
    signalled_running: int
    tasks: list[DAGCancelTaskResult]

    @field_serializer("dag_run_id", when_used="json")
    def serialize_dag_run_id(self, value: ULID) -> str:
        return str(value)


# Avoid circular import at module level – Task is only referenced inside methods.
if TYPE_CHECKING:
    from jobbers.models.task import Task
