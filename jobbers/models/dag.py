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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated, Any, Literal

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
                        inject_parent_results=cb.inject_parent_results,
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
                        inject_parent_results=cb.inject_parent_results,
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
    inject_parent_results: bool = False  # Inject parent results as `parent_results` kwarg


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
    inject_parent_results: bool = False  # Inject all predecessor results as `parent_results` kwarg


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
                # Arm fan-in sets are initialised at runtime by the processor,
                # not at static submission time, so skip here.
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
        # (successor_node, fan_in_key or None, error_node or None, inject_parent_results)
        self._successors: list[tuple[DAGNode, str | None, DAGNode | None, bool]] = []
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
        inject_parent_results: bool = False,
    ) -> DAGNode:
        """
        Chain: each of *nodes* runs immediately after *this* node completes.

        Pass `on_error` to also submit an error task when *this* node fails permanently.
        Pass `inject_parent_results=True` to have the worker fetch this node's results
        and inject them as a ``parent_results`` kwarg into each successor's task function.

        Returns *self* for fluent chaining:

        ```python
        a.then(b).then(c)  # same as a.then(b); b.then(c)
        ```
        """
        for node in nodes:
            self._successors.append((node, None, on_error, inject_parent_results))
        return self

    @classmethod
    def find_terminals(cls, roots: list[DAGNode]) -> list[DAGNode]:
        """
        Return all leaf nodes (nodes with no successors) reachable from *roots*.

        Used by the processor to determine which nodes should decrement the fan-in
        set when dynamic fanout arms are multi-step chains rather than single tasks.
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
                for successor, _, _, _ in node._successors:
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
        inject_parent_results: bool = False,
    ) -> DAGNode:
        """
        Fan-in: `into` runs only after *all* `predecessors` have completed.

        Pass `on_error` to submit an error task when any predecessor fails permanently.
        Pass `inject_parent_results=True` to have the worker fetch all predecessors'
        results and inject them as a ``parent_results`` list kwarg into `into`'s function.

        A shared Redis set key derived from `into`'s task ID is stored on
        each predecessor's callback so the worker knows which set to decrement.
        Returns `into` for further chaining:

        ```python
        DAGNode.merge(branch_a, branch_b, into=collector).then(next_step)
        ```
        """
        fan_in_key = f"dag:fan-in:{into._id}"
        for pred in predecessors:
            pred._successors.append((into, fan_in_key, on_error, inject_parent_results))
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
        for successor, fan_in_key, error_node, inject_parent_results in self._successors:
            spec = successor.to_spec()
            error_spec = error_node.to_spec() if error_node is not None else None
            if fan_in_key is None:
                callbacks.append(
                    SimpleCallback(
                        task=spec,
                        error_callback=error_spec,
                        inject_parent_results=inject_parent_results,
                    )
                )
            else:
                callbacks.append(
                    FanInCallback(
                        task=spec,
                        fan_in_key=fan_in_key,
                        error_callback=error_spec,
                        inject_parent_results=inject_parent_results,
                    )
                )
        callbacks.extend(self._fanout_callbacks)
        return callbacks

    def to_task(self, *, parent_id: ULID | None = None, dag_run_id: ULID | None = None) -> Task:
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
            for successor, fan_in_key, _error_node, _ipr in node._successors:
                if fan_in_key is not None:
                    result.setdefault(fan_in_key, set()).add(node._id)
                _walk(successor)

        _walk(self)
        return result


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


# Avoid circular import at module level – Task is only referenced inside methods.
if TYPE_CHECKING:
    from jobbers.models.task import Task
