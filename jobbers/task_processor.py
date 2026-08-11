import asyncio
import logging
from typing import TYPE_CHECKING, Annotated, Any, NamedTuple, cast, get_args, get_origin, get_type_hints

from opentelemetry import metrics
from ulid import ULID

from jobbers.context import _current_task as _current_task_cv
from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOut,
    DynamicFanOutCallback,
    FanInCallback,
    FromParent,
    SimpleCallback,
    TaskResult,
    validate_fan_in_cardinality,
)
from jobbers.models.task import Task
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_task_config
from jobbers.state_manager import (
    CancelReason,
    StaleTaskCancelledError,
    StateManager,
    TaskRateLimitedError,
    UserCancellationError,
)
from jobbers.utils.di import DependencyResolver
from jobbers.utils.di import Depends as _Depends

if TYPE_CHECKING:
    from collections.abc import Awaitable


logger = logging.getLogger(__name__)


class _FanInPred(NamedTuple):
    node: DAGNode
    err_node: DAGNode | None


def _spec_to_dag_node(root: DAGTaskSpec) -> DAGNode:
    """
    Reconstruct a ``DAGNode`` builder tree from a ``DAGTaskSpec`` subtree.

    Walks all ``SimpleCallback`` and ``FanInCallback`` entries recursively,
    creates one ``DAGNode`` per spec (preserving its pre-assigned ULID), and
    wires them with ``.then()`` / ``DAGNode.merge()`` to match the original
    graph structure.  ``DynamicFanOutCallback`` entries are reattached as-is
    via ``add_fanout_callback`` (not recursed into) — nested declarative
    fanouts are driven by the processor when those tasks execute.
    """
    # First pass: collect every reachable spec and create a matching DAGNode.
    all_specs: dict[ULID, DAGTaskSpec] = {}
    all_nodes: dict[ULID, DAGNode] = {}

    def _collect(s: DAGTaskSpec) -> None:
        if s.id in all_specs:
            return
        all_specs[s.id] = s
        all_nodes[s.id] = DAGNode(
            s.name, queue=s.queue, version=s.version, parameters=dict(s.parameters), task_id=s.id
        )
        for cb in s.dag_callbacks:
            if isinstance(cb, (SimpleCallback, FanInCallback)):
                _collect(cb.task)
                if cb.error_callback:
                    _collect(cb.error_callback)

    _collect(root)

    # Second pass: wire edges, collecting fan-in predecessor groups.
    fan_in_preds: dict[ULID, list[_FanInPred]] = {}
    for spec_id, spec in all_specs.items():
        node = all_nodes[spec_id]
        for cb in spec.dag_callbacks:
            if isinstance(cb, SimpleCallback):
                successor = all_nodes[cb.task.id]
                err_node = all_nodes.get(cb.error_callback.id) if cb.error_callback else None
                node.then(successor, on_error=err_node)
            elif isinstance(cb, FanInCallback):
                err_node = all_nodes.get(cb.error_callback.id) if cb.error_callback else None
                fan_in_preds.setdefault(cb.task.id, []).append(_FanInPred(node, err_node))
            elif isinstance(cb, DynamicFanOutCallback):
                node.add_fanout_callback(cb)

    for collector_id, preds in fan_in_preds.items():
        collector_node = all_nodes[collector_id]
        # DAGNode.merge() is called once per predecessor below (to allow each
        # predecessor its own on_error node), so it never sees the full group and
        # can't run its own cardinality check — validate the full group here instead.
        validate_fan_in_cardinality(tuple(pred.node for pred in preds), collector_node)
        for pred in preds:
            DAGNode.merge(
                pred.node,
                into=collector_node,
                on_error=pred.err_node,
            )

    return all_nodes[root.id]


meter = metrics.get_meter(__name__)
tasks_processed = meter.create_counter("tasks_processed", unit="1")
tasks_retried = meter.create_counter("tasks_retried", unit="1")
execution_time = meter.create_histogram("task_execution_time", unit="ms")
end_to_end_latency = meter.create_histogram("task_end_to_end_latency", unit="ms")
post_process_failures = meter.create_counter("post_process_failures", unit="1")
tasks_completed_after_stale = meter.create_counter("tasks_completed_after_stale", unit="1")

_OMIT = object()  # sentinel: key legitimately absent — let the function's own Python default apply


def _resolve_from_parent(
    task: Task, param_name: str, spec: FromParent, parent_results_map: dict[ULID, dict[Any, Any]]
) -> Any:
    """
    Resolve a single ``FromParent``-annotated parameter for *task*.

    Zero parents (a root node) is not a shape violation for either mode -- there is
    nothing to pull from, so this returns ``_OMIT`` so the caller leaves the kwarg
    unset. That lets a submitted ``task.parameters`` value or the function's own
    Python default apply, which is what makes a ``FromParent``-annotated task usable
    as a root node (or called directly in a test) without fabricating a parent.

    ``many=True`` with 1+ parents always returns a list (possibly empty -- when none
    of the parents produced the key). ``many=False`` with 2+ parents is a genuine
    shape violation -- a fan-in wired to a singular slot -- and raises unconditionally,
    regardless of what the parents' results contain, since no data could ever make
    that wiring correct. With exactly one parent, a missing key returns ``_OMIT`` so
    the function's own Python default (if any) applies.
    """
    key = spec.key or param_name

    if not task.parent_ids:
        return _OMIT

    if spec.many:
        return [r[key] for r in parent_results_map.values() if key in r]

    if len(task.parent_ids) > 1:
        raise ValueError(
            f"FromParent({key!r}) on task {task.name!r} (param {param_name!r}, id={task.id}) requires "
            f"exactly one parent (chain position); this task has {len(task.parent_ids)}. Use "
            "FromParent(..., many=True) for fan-in."
        )
    result = next(iter(parent_results_map.values()), {})
    return result[key] if key in result else _OMIT


class TaskProcessor:
    """TaskProcessor to process tasks from a TaskGenerator."""

    def __init__(self, state_manager: StateManager) -> None:
        self.state_manager = state_manager
        self._current_promise: Awaitable[Any] | None = None

    async def run(self, task: Task) -> None:
        with self.state_manager.cancel_event(task.id, task.dag_run_id):
            try:
                async with asyncio.TaskGroup() as tg:
                    process_task = tg.create_task(self.process(task))
                    monitor_task = tg.create_task(self.monitor_task_cancellation(task))
                    monitor_task.add_done_callback(lambda t: process_task.cancel())
                    process_task.add_done_callback(lambda t: monitor_task.cancel())
            except ExceptionGroup as eg:
                for exc in eg.exceptions:
                    # Treat UserCancellationError/StaleTaskCancelledError as normal control
                    # flow signals to exit the TaskGroup; re-raise any other exceptions.
                    if not isinstance(exc, (UserCancellationError, StaleTaskCancelledError)):
                        raise  # Re-raise to exit the TaskGroup in run()

    async def process(self, task: Task) -> Task:
        """Process the task and return the result."""
        logger.debug("Task %s details: %s", task.id, task)
        task.task_config = get_task_config(task.name, task.version)
        ex: BaseException | None = None

        dynamic_fanout: DynamicFanOut | None = None
        if task.task_config is None:
            await self.handle_dropped_task(task)
        else:
            self.mark_task_as_started(task)
            await self.state_manager.save_task(task)

            with self.state_manager.task_in_registry(task):
                await self.state_manager.update_task_heartbeat(task)
                task._adapter = self.state_manager.task_state
                _token = _current_task_cv.set(task)

                try:
                    hints = get_type_hints(task.task_config.function, include_extras=True)
                except Exception:
                    hints = {}

                kwargs = dict(task.parameters)

                from_parent_specs: dict[str, FromParent] = {}
                for param_name, hint in hints.items():
                    if param_name == "return" or get_origin(hint) is not Annotated:
                        continue
                    for meta in get_args(hint)[1:]:
                        if isinstance(meta, FromParent):
                            from_parent_specs[param_name] = meta
                            break

                parent_results_map: dict[ULID, dict[Any, Any]] = {}
                if task.parent_ids and from_parent_specs:
                    parent_results_map = await task.parent_results()

                for param_name, spec in from_parent_specs.items():
                    value = _resolve_from_parent(task, param_name, spec, parent_results_map)
                    if value is not _OMIT:
                        kwargs[param_name] = value

                resolver = DependencyResolver(task.task_config.dependency_graph)
                async with resolver:
                    # Resolve DI deps and map them to their kwarg names
                    dep_cache = await resolver.resolve_all()
                    for param_name, hint in hints.items():
                        if param_name == "return":
                            continue
                        if get_origin(hint) is Annotated:
                            for meta in get_args(hint)[1:]:
                                if isinstance(meta, _Depends) and meta.dependency in dep_cache:
                                    kwargs[param_name] = dep_cache[meta.dependency]

                    self._current_promise = task.task_config.function(**kwargs)
                    if task.task_config.on_shutdown == TaskShutdownPolicy.CONTINUE:
                        self._current_promise = asyncio.shield(self._current_promise)

                    # Run the task and handle exceptions
                    try:
                        async with asyncio.timeout(task.task_config.timeout):
                            raw_result: dict[Any, Any] | TaskResult | None = await self._current_promise
                        if isinstance(raw_result, TaskResult):
                            task.results = raw_result.results
                            dynamic_fanout = raw_result.fanout
                            if raw_result.parent_ids:
                                task.parent_ids = raw_result.parent_ids
                        else:
                            if task.parent_ids:
                                task.parent_ids = list(set(task.parent_ids))  # deduplicate parent IDs
                            task.results = raw_result or {}
                    except TimeoutError:
                        task = await self.handle_timeout_exception(task)
                    except asyncio.CancelledError as exc:
                        if task.status == TaskStatus.CANCELLED:
                            pass  # user cancellation already handled; keep CANCELLED status
                        elif self.state_manager.cancel_reason(task.id) == CancelReason.STALE:
                            pass  # cleaner already marked this task STALLED; nothing to persist
                        else:
                            ex = exc
                            await self.handle_system_cancelled_task(task)
                    except Exception as exc:
                        if (
                            task.task_config
                            and task.task_config.expected_exceptions
                            and isinstance(exc, task.task_config.expected_exceptions)
                        ):
                            task = await self.handle_expected_exception(task, exc)
                        else:
                            await self.handle_unexpected_exception(task, exc)
                    else:
                        await self.handle_success(task)
                    finally:
                        _current_task_cv.reset(_token)

            await self.state_manager.remove_task_heartbeat(task)

        # Metrics recording
        tasks_processed.add(1, {"queue": task.queue, "task": task.name, "status": task.status})
        # Use retried_at (set at the start of the most recent retry attempt) instead of
        # started_at (set once, at the very first attempt) when present, so a retried task's
        # execution_time reflects only the attempt that actually finished -- not the full
        # first-start-to-completion span, which would otherwise include every retry's backoff wait.
        execution_start = task.retried_at or task.started_at
        if execution_start and task.completed_at:
            execution_time.record(
                (task.completed_at - execution_start).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status},
            )
        if task.submitted_at and task.completed_at:
            end_to_end_latency.record(
                (task.completed_at - task.submitted_at).total_seconds() * 1000,
                {"queue": task.queue, "task": task.name, "status": task.status},
            )

        if task.status == TaskStatus.COMPLETED:
            try:
                await self.post_process(task, dynamic_fanout)
            except Exception as exc:
                await self._handle_post_process_failure(task, exc)
        else:
            # Only FAILED triggers error callbacks. CANCELLED means the user
            # deliberately stopped the task; STALLED and DROPPED are system-level
            # outcomes where the task function never ran to completion — firing an
            # error callback would be surprising and is intentionally not supported.
            if task.status == TaskStatus.FAILED:
                try:
                    await self.post_process_error(task)
                except Exception as exc:
                    await self._handle_post_process_failure(task, exc)

        # Runs after post_process/post_process_error so any fan-out arms this task
        # spawned are already registered in the DAG run before this task is closed
        # out of it — otherwise a dispatcher could look like the last active task
        # and trigger cleanup before its own arms exist.
        #
        # Only terminal statuses close the task out of DAG-run tracking. A task
        # that is retrying (SCHEDULED / UNSUBMITTED) is not done — closing it here
        # would desync the DAG-run pending counter and could trigger the sibling
        # sweep while this task is still going to run again.
        if task.status in TaskStatus.terminal_statuses():
            await self._maybe_cleanup(task)

        if task.status != TaskStatus.COMPLETED and ex is not None:
            raise ex

        return task

    async def monitor_task_cancellation(self, task: Task) -> None:
        """Monitor for task cancellation and handle it."""
        try:
            await self.state_manager.monitor_task_cancellation(task.id)
        except StaleTaskCancelledError:
            raise  # nothing to persist -- the cleaner's STALLED write is already authoritative
        except UserCancellationError:
            await self.handle_user_cancelled_task(task)
            raise  # Re-raise to exit the TaskGroup in run()

    async def _maybe_cleanup(self, task: Task) -> None:
        """
        Delete the task record if its final status is in cleanup_on.

        For standalone tasks this is a direct check. For DAG tasks, delegates
        entirely to ``StateManager.finalize_dag_run_task``, which owns both the
        aggregate-status bookkeeping and the pending-counter/sweep logic (and the
        ordering constraint between them — see its docstring). TaskProcessor just
        needs to call it exactly once per DAG-task completion, regardless of
        whether the task's terminal status is stuck or not.
        """
        if task.dag_run_id is None:
            await self._maybe_delete_self(task)
            return
        await self.state_manager.finalize_dag_run_task(task)

    async def _maybe_delete_self(self, task: Task) -> None:
        """Delete a standalone (non-DAG) task's record if its status matches cleanup_on."""
        if task.task_config is None or not task.task_config.cleanup_on:
            return
        if task.status in task.task_config.cleanup_on:
            await self.state_manager.delete_task(task)

    def mark_task_as_started(self, task: Task) -> None:
        task.set_status(TaskStatus.STARTED)
        logger.info("Task %s started (attempt %d).", task.id, task.retry_attempt + 1)

    async def post_process(self, task: Task, dynamic_fanout: DynamicFanOut | None = None) -> None:
        if task.dag_run_id and await self.state_manager.is_dag_run_cancelling(task.dag_run_id):
            # Run is being cancelled; don't spawn new descendants (fan-out arms,
            # fan-in collectors, SimpleCallback chains). The task's own terminal
            # bookkeeping (finalize_dag_run_task) still runs normally afterwards.
            return

        # Detect outer fan-in callbacks that should be delegated to a grandcollector
        # instead of being decremented by this task, from either fan-out mechanism:
        # a declarative DynamicFanOutCallback in the spec (mermaid `-->>`), or a
        # programmatic DynamicFanOut returned by the task function. Only applies
        # when propagate_fan_in is True (the default). Keyed by fan_in_key so
        # skip_fan_in_keys below reflects whichever mechanism actually delegated it
        # — the declarative path performs its own delegation inside
        # _handle_declarative_fanout, so it must still be reflected here or
        # generate_callbacks would redundantly try to decrement an already-delegated key.
        outer_fan_in_cbs_by_key: dict[str, FanInCallback] = {}

        # Handle declarative DynamicFanOutCallbacks embedded in the task spec.
        # These are produced by the mermaid parser; task functions that return
        # DynamicFanOut directly use the dynamic_fanout path below instead.
        for cb in task.dag_callbacks:
            if isinstance(cb, DynamicFanOutCallback):
                await self._handle_declarative_fanout(task, cb)
                if cb.propagate_fan_in:
                    for fc in task.dag_callbacks:
                        if isinstance(fc, FanInCallback):
                            outer_fan_in_cbs_by_key[fc.fan_in_key] = fc

        if dynamic_fanout is not None:
            if dynamic_fanout.propagate_fan_in:
                for fc in task.dag_callbacks:
                    if isinstance(fc, FanInCallback):
                        outer_fan_in_cbs_by_key[fc.fan_in_key] = fc
            await self._handle_dynamic_fanout(task, dynamic_fanout, list(outer_fan_in_cbs_by_key.values()))

        if task.has_callbacks():
            skip_keys = frozenset(outer_fan_in_cbs_by_key)
            callbacks = await task.generate_callbacks(
                self.state_manager.task_state, skip_fan_in_keys=skip_keys
            )
            for callback in callbacks:
                callback.queue = await self.state_manager.resolve_queue(callback)
            await self.state_manager.submit_tasks_batch(callbacks)

    async def post_process_error(self, task: Task) -> None:
        """Submit error callback tasks for a permanently-failed task."""
        error_callbacks = task.generate_error_callbacks()
        if error_callbacks:
            for cb in error_callbacks:
                cb.queue = await self.state_manager.resolve_queue(cb)
            await self.state_manager.submit_tasks_batch(error_callbacks)

    async def _handle_post_process_failure(self, task: Task, exc: Exception) -> None:
        """
        Record a failure from post_process/post_process_error without disturbing status.

        The task's terminal status (COMPLETED/FAILED) is already correctly persisted, so
        this must not change it. Without this handler, an exception here (a transient
        store error, or generate_callbacks()'s ValueError for a FanInCallback missing
        dag_run_id) would propagate out of process() uncaught, leaving a task record that
        looks COMPLETED/FAILED with no indication its DAG continuation (fan-out arms,
        callbacks) never actually got wired up.
        """
        logger.exception("Post-processing failed for task %s (status=%s): %s", task.id, task.status, exc)
        task.errors.append(f"post_process failed: {exc}")
        post_process_failures.add(1, {"queue": task.queue, "task": task.name, "status": task.status})
        await self.state_manager.save_task(task)

    async def _handle_declarative_fanout(self, parent: Task, cb: DynamicFanOutCallback) -> None:
        """
        Drive a declarative fan-out declared in a ``DynamicFanOutCallback``.

        Reads ``parent.results[cb.items_key]`` (a list of dicts) and spawns one
        arm instance per entry by cloning ``cb.arm_root`` with the entry's params
        shallow-merged in (entry values override the template's static params).
        ``cb.collector`` is used as-is for the fan-in collector.

        Delegates to ``_handle_dynamic_fanout`` so that all fan-in wiring,
        pre-save, and arm submission are handled identically to the programmatic
        path.
        """
        items = parent.results.get(cb.items_key)
        if not isinstance(items, list):
            logger.warning(
                "DynamicFanOutCallback on task %s: results[%r] is missing or not a list; "
                "submitting collector immediately.",
                parent.id,
                cb.items_key,
            )
            items = []

        # Build arm DAGNodes from the template, merging per-item params.
        arm_nodes: list[DAGNode] = []
        fresh_root, _ = cb.arm_root.fresh_copy()
        for item_params in items:
            cloned, _ = cb.arm_root.fresh_copy()
            merged_params = {
                **fresh_root.parameters,
                **(item_params if isinstance(item_params, dict) else {}),
            }
            cloned = cloned.model_copy(update={"parameters": merged_params})
            # Rebuild a DAGNode from the spec so _handle_dynamic_fanout can walk it.
            arm_nodes.append(_spec_to_dag_node(cloned))

        collector_node = _spec_to_dag_node(cb.collector.fresh_copy()[0])

        outer_fan_in_cbs: list[FanInCallback] = (
            [fc for fc in parent.dag_callbacks if isinstance(fc, FanInCallback)]
            if cb.propagate_fan_in
            else []
        )
        fanout = DynamicFanOut(
            arms=arm_nodes,
            collector=collector_node,
            fan_in_ttl=cb.fan_in_ttl,
            propagate_fan_in=cb.propagate_fan_in,
        )
        await self._handle_dynamic_fanout(parent, fanout, outer_fan_in_cbs)

    async def _delegate_outer_fan_in(
        self,
        collector_task: Task,
        dag_run_id: ULID,
        parent_id: ULID,
        collector_id: ULID,
        outer_fan_in_cbs: list[FanInCallback],
    ) -> None:
        """
        Transfer outer_fan_in_cbs onto collector_task and delegate them to collector_id.

        Atomically swaps parent_id for collector_id in each outer fan-in set, so the
        outer fan-in waits for the collector rather than the task that just
        dispatched it. No-op if outer_fan_in_cbs is empty.
        """
        if not outer_fan_in_cbs:
            return
        collector_task.dag_callbacks = list(collector_task.dag_callbacks) + cast(
            "list[SimpleCallback | FanInCallback | DynamicFanOutCallback]", outer_fan_in_cbs
        )
        await asyncio.gather(
            *(
                self.state_manager.task_state.delegate_fan_in(
                    dag_run_id, cb.fan_in_key, parent_id, collector_id
                )
                for cb in outer_fan_in_cbs
            )
        )

    async def _handle_dynamic_fanout(
        self,
        parent: Task,
        fanout: DynamicFanOut,
        outer_fan_in_cbs: list[FanInCallback],
    ) -> None:
        """
        Wire and submit a runtime fan-out produced by a task function.

        Finds the terminal (leaf) nodes of each arm, calls `DAGNode.merge` to
        attach `FanInCallback`s to those terminals, initialises all fan-in sets
        (both intermediate sets within multi-step arms and the collector set),
        pre-saves the collector so it exists when the first terminal finishes,
        then submits all arm-root tasks atomically.

        When *outer_fan_in_cbs* is non-empty the collector is a grandcollector:
        those callbacks are transferred to it and the parent's ID is swapped for
        the collector's ID in each outer fan-in set so the outer fan-in waits for
        the grandcollector rather than this task.

        **Best practice:** assign arm tasks to queues without rate limiting.
        Once a DAG is executing there is no safe recourse if a submission is
        rejected — the fan-in would be initialised but never complete.
        Rate limits on arm queues are bypassed with a warning logged.
        """
        # Fan-in tracking is scoped per DAG run. A task fanning out without already
        # being part of one starts a fresh run for the fanned-out sub-graph, with no
        # user-supplied name available to inherit -- default to a name derived from
        # the dispatching task.
        dag_run_id = parent.dag_run_id or ULID()
        dag_run_name = parent.dag_run_name or f"{parent.name} (fan-out)"

        if not fanout.arms:
            # Degenerate case: no arms — submit the collector immediately. Outer fan-in
            # callbacks still need to be delegated to it, exactly as in the normal path
            # below, otherwise an outer fan-in waiting on *parent* never gets closed.
            solo = fanout.collector.to_task(dag_run_id=dag_run_id, dag_run_name=dag_run_name)
            await self._delegate_outer_fan_in(
                solo, dag_run_id, parent.id, fanout.collector.id, outer_fan_in_cbs
            )
            try:
                await self.state_manager.submit_task(solo)
            except TaskRateLimitedError:
                logger.error(
                    "Collector %s for dynamic fan-out from task %s was rejected by rate "
                    "limiting; the fan-out cannot complete. Assign arm/collector queues "
                    "without rate limiting.",
                    solo.id,
                    parent.id,
                )
            return

        # 1. Find the terminal (leaf) nodes of each arm before wiring the collector.
        #    For single-step arms these are the arm roots themselves.
        terminals = DAGNode.find_terminals(fanout.arms)

        # 2. Collect intermediate fan-in sets within multi-step arm sub-chains.
        #    fan_in_predecessors() walks the builder graph and returns all FanInCallback
        #    key→predecessor-id mappings present *before* we wire the collector.
        all_fan_ins: dict[str, set[ULID]] = {}
        for arm in fanout.arms:
            for k, v in arm.fan_in_predecessors().items():
                all_fan_ins.setdefault(k, set()).update(v)

        # 3. Wire the collector fan-in to the terminal nodes (not the arm roots).
        fan_in_key = f"dag:fan-in:{fanout.collector.id}"
        DAGNode.merge(*terminals, into=fanout.collector)
        terminal_ids = {t.id for t in terminals}
        all_fan_ins[fan_in_key] = terminal_ids

        # 4. Build tasks: submit arm roots, collector waits for terminal IDs.
        arm_tasks = [
            arm.to_task(parent_id=parent.id, dag_run_id=dag_run_id, dag_run_name=dag_run_name)
            for arm in fanout.arms
        ]
        collector_task = fanout.collector.to_task(dag_run_id=dag_run_id, dag_run_name=dag_run_name)
        collector_task.parent_ids = list(terminal_ids)

        # 5. Delegation: transfer outer fan-in callbacks to the collector and
        #    atomically swap parent's ID → collector's ID in each outer fan-in set.
        #    Outer callbacks only exist when the parent was already part of dag_run_id.
        await self._delegate_outer_fan_in(
            collector_task, dag_run_id, parent.id, fanout.collector.id, outer_fan_in_cbs
        )

        # 6. Initialise all fan-in sets, pre-save the collector, submit arm roots.
        await asyncio.gather(
            *(
                self.state_manager.init_fan_in(dag_run_id, k, ids, ttl=fanout.fan_in_ttl)
                for k, ids in all_fan_ins.items()
            )
        )
        # Pre-save the collector so it exists in the store when the first terminal completes.
        await self.state_manager.save_task(collector_task)

        # Warn if any arm queue has rate limiting — we bypass it below.
        arm_queues = list({at.queue for at in arm_tasks})
        configs = await asyncio.gather(*(self.state_manager.get_queue_config(q) for q in arm_queues))
        for queue, config in zip(arm_queues, configs):
            if config and config.rate_numerator and config.rate_denominator and config.rate_period:
                logger.warning(
                    "Queue '%s' has rate limiting configured but DAG arm task submission "
                    "bypasses rate limits. Assign arm tasks to queues without rate limiting.",
                    queue,
                )

        await self.state_manager.submit_tasks_batch(arm_tasks)

    async def handle_dropped_task(self, task: Task) -> None:
        logger.error("Dropping unknown task %s v%s id=%s.", task.name, task.version, task.id)
        task.set_status(TaskStatus.DROPPED)
        await self.state_manager.save_task(task)

    async def handle_system_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled.", task.id)
        task.shutdown()
        if task.status == TaskStatus.SUBMITTED:
            # RESUBMIT policy: put it back in its queue rather than just saving the blob.
            await self.state_manager.requeue_task(task)
        else:
            await self.state_manager.save_task(task)

    async def handle_user_cancelled_task(self, task: Task) -> None:
        logger.info("Task %s was cancelled by user.", task.id)
        task.set_status(TaskStatus.CANCELLED)
        applied = await self.state_manager.task_state.save_task_if_status(task, TaskStatus.STARTED)
        if not applied:
            logger.warning("Task %s cancelled after being marked stale; discarding this result.", task.id)
            tasks_completed_after_stale.add(1, {"queue": task.queue, "task": task.name})

    async def handle_unexpected_exception(self, task: Task, exc: Exception) -> None:
        logger.exception("Exception occurred while processing task %s: %s", task.id, exc)
        task.set_status(TaskStatus.FAILED)
        task.errors.append(str(exc))
        await self.state_manager.fail_task(task)

    async def _handle_retry(self, task: Task, error_message: str) -> Task:
        task.errors.append(error_message)
        if task.dag_run_id and await self.state_manager.is_dag_run_cancelling(task.dag_run_id):
            # Cancellation wins over "retries remaining" -- don't resubmit work into
            # a run that's supposed to be stopping. See "Cancelling DAG runs" in docs/interacting-with-dags.md.
            await self.handle_user_cancelled_task(task)
            return task
        if not task.should_retry():
            task.set_status(TaskStatus.FAILED)
            await self.state_manager.fail_task(task)
            return task

        tasks_retried.add(1, {"queue": task.queue, "task": task.name, "version": task.version})
        if task.should_schedule():
            run_at = task.task_config.compute_retry_at(task.retry_attempt)  # type: ignore[union-attr]
            task.set_status(TaskStatus.SCHEDULED)
            return await self.state_manager.schedule_retry_task(task, run_at)
        else:
            task.set_status(TaskStatus.UNSUBMITTED)
            return await self.state_manager.queue_retry_task(task)

    async def handle_expected_exception(self, task: Task, exc: Exception) -> Task:
        logger.warning("Task %s failed with error: %s", task.id, exc)
        return await self._handle_retry(task, str(exc))

    async def handle_timeout_exception(self, task: Task) -> Task:
        timeout: int | None = None if task.task_config is None else task.task_config.timeout
        logger.warning("Task %s timed out after %s seconds.", task.id, timeout)
        return await self._handle_retry(task, f"Task {task.id} timed out after {timeout} seconds")

    async def handle_success(self, task: Task) -> None:
        logger.info("Task %s completed.", task.id)
        task.set_status(TaskStatus.COMPLETED)
        if task.cron_id is not None:
            await self.state_manager.complete_cron_task(task)
        else:
            applied = await self.state_manager.task_state.save_task_if_status(task, TaskStatus.STARTED)
            if not applied:
                logger.warning("Task %s completed after being marked stale; discarding this result.", task.id)
                tasks_completed_after_stale.add(1, {"queue": task.queue, "task": task.name})
