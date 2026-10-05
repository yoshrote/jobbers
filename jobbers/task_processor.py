import asyncio
import datetime as dt
import logging
from typing import TYPE_CHECKING, Annotated, Any, NamedTuple, cast, get_args, get_origin, get_type_hints

from opentelemetry import metrics
from ulid import ULID

from jobbers.constants import RERUN_ROUTER_TASK
from jobbers.context import _current_task as _current_task_cv
from jobbers.models.dag import (
    DAGNode,
    DAGTaskSpec,
    DynamicFanOut,
    DynamicFanOutCallback,
    FanInCallback,
    FromParent,
    RouterCallback,
    RouterSpec,
    SimpleCallback,
    TaskResult,
    collect_fan_in_keys,
    validate_fan_in_cardinality,
)
from jobbers.models.router import RouteTo
from jobbers.models.task import Task
from jobbers.models.task_shutdown_policy import TaskShutdownPolicy
from jobbers.models.task_status import TaskStatus
from jobbers.registry import get_router_config, get_task_config
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


class RouterError(Exception):
    """
    A router was unregistered, raised, or made a selection that matched no single candidate.

    Subclasses carry *why*, because the right response differs. A router is a pure
    function of results that are already persisted, so re-running it on a timer fails
    identically -- only a deploy changes the outcome. The one exception is an
    unregistered router, which is genuinely transient mid-rolling-deploy: a different
    worker may have the router and succeed. ``retryable`` is what distinguishes the two.

    ``reason`` is the metric tag (see ``router_failures``) and is recorded on the
    placeholder so an operator can tell an unregistered-router spike during a deploy
    from a real logic bug.
    """

    reason = "router_error"
    retryable = False


class UnknownRouterError(RouterError):
    """The router is not in this worker's registry -- possibly a rolling deploy in progress."""

    reason = "unregistered"
    retryable = True


class RouterRaisedError(RouterError):
    """The router function itself raised."""

    reason = "raised"


class NoCandidateMatchedError(RouterError):
    """The router's selection matched none of its declared candidates."""

    reason = "no_candidate"


class AmbiguousSelectionError(RouterError):
    """The router's selection matched more than one candidate."""

    reason = "ambiguous"


class InvalidRouterReturnError(RouterError):
    """The router returned something that is not a task name, a RouteTo, or None."""

    reason = "invalid_return"


meter = metrics.get_meter(__name__)
tasks_processed = meter.create_counter("tasks_processed", unit="1")
tasks_retried = meter.create_counter("tasks_retried", unit="1")
execution_time = meter.create_histogram("task_execution_time", unit="ms")
end_to_end_latency = meter.create_histogram("task_end_to_end_latency", unit="ms")
post_process_failures = meter.create_counter("post_process_failures", unit="1")
router_decisions = meter.create_counter("router_decisions", unit="1")
# Router failures get their own counters rather than sharing post_process_failures: a
# fallback is not a post-process failure (the run continued), and a halt is specifically
# a *router* failure, worth separating from the generic bucket that also holds store
# errors and fan-in misconfiguration. halted = router_failures - router_fallbacks.
router_failures = meter.create_counter("router_failures", unit="1")
router_fallbacks = meter.create_counter("router_fallbacks", unit="1")
tasks_completed_after_stale = meter.create_counter("tasks_completed_after_stale", unit="1")

_OMIT = object()  # sentinel: key legitimately absent — let the function's own Python default apply


def select_candidate(
    router: RouterSpec,
    results: dict[Any, Any],
    parent: Task,
    mode: str,
) -> DAGTaskSpec | None:
    """
    Run *router* over *results* and return the candidate spec it selects.

    Returns ``None`` when the router declines to route (returns ``None``) -- a decision,
    not a failure, and in per-item mode the documented way to drop an item.

    Raises a :class:`RouterError` subclass when the router is unregistered, raises, or
    names a selection that does not resolve to exactly one candidate.

    Module-level rather than a method because the ``jobbers__rerun_router`` system task
    re-runs the identical selection on a resume (see ``jobbers/system_tasks.py``); sharing
    one function is what stops the retry path and the inline path from diverging.
    """
    config = get_router_config(router.router, router.version)
    if config is None:
        raise UnknownRouterError(
            f"Unknown router '{router.router}@{router.version}'. "
            "Register it with @register_router before submitting."
        )
    try:
        choice = config.function(results, **router.parameters)
    except Exception as exc:
        raise RouterRaisedError(f"Router '{router.router}' raised: {exc}") from exc

    if choice is None:
        router_decisions.add(1, {"router": router.router, "target": "<none>", "mode": mode})
        logger.debug("Router %s on task %s selected nothing.", router.router, parent.id)
        return None

    selector = RouteTo(choice) if isinstance(choice, str) else choice
    if not isinstance(selector, RouteTo):
        raise InvalidRouterReturnError(
            f"Router '{router.router}' returned {type(choice).__name__}; "
            "expected a task name, a RouteTo, or None"
        )

    matches = [
        c
        for c in router.candidates
        if c.name == selector.task
        and (selector.queue is None or c.queue == selector.queue)
        and (selector.version is None or c.version == selector.version)
    ]
    if not matches:
        raise NoCandidateMatchedError(
            f"Router '{router.router}' selected {selector!r}, which matches none of its "
            f"candidates: {[f'{c.name}@{c.version}:{c.queue}' for c in router.candidates]}"
        )
    if len(matches) > 1:
        raise AmbiguousSelectionError(
            f"Router '{router.router}' selected {selector!r}, which is ambiguous across "
            f"queues {sorted(c.queue for c in matches)}. Return RouteTo(task, queue=...) "
            "to say which one."
        )
    chosen = matches[0]
    router_decisions.add(
        1, {"router": router.router, "target": f"{chosen.name}:{chosen.queue}", "mode": mode}
    )
    return chosen


async def submit_router_choice(
    state_manager: "StateManager",
    parent: Task,
    router: RouterSpec,
    chosen: DAGTaskSpec,
) -> Task:
    """
    Submit the candidate a router picked, initialising any fan-ins inside that branch.

    The selected candidate spec is used as-is, keeping its pre-assigned ULID so the live
    diagram lines up with the submitted task and so a re-run after a fix is idempotent --
    picking the same candidate writes the same task id.

    Shared by the inline path (``TaskProcessor._handle_router``) and the
    ``jobbers__rerun_router`` system task.
    """
    task = parent._build_callback_task(chosen, [parent.id])
    # Fan-in sets inside the chosen branch are initialised now rather than at
    # submit time: the unchosen branches never run, so pre-populating their
    # sets would leave collectors waiting forever.
    fan_ins = collect_fan_in_keys(chosen)
    if fan_ins:
        if task.dag_run_id is None:
            raise RouterError(
                f"Router '{router.router}' selected a branch containing a fan-in but "
                f"task {parent.id} has no dag_run_id. Submit via submit_dag()."
            )
        await asyncio.gather(
            *(state_manager.init_fan_in(task.dag_run_id, key, ids) for key, ids in fan_ins.items())
        )
    await state_manager.submit_tasks_batch([task])
    return task


def build_router_placeholder(
    parent: Task,
    cb: RouterCallback,
    exc: "RouterError",
    *,
    fell_back: bool,
) -> Task:
    """
    Build the task that stands in for a router node whose selection failed.

    It takes **the router node's own pre-assigned id**, not a fresh one. That is what lets
    the router's rhombus in the emitted diagram carry this task's state with no extra
    mapping, keeps a second failure of the same router an overwrite rather than a
    duplicate, and means the placeholder needs no diagram node of its own -- it *is* the
    router node.

    Status: ``FAILED`` when halting, which puts the run in ``stuck_statuses()`` and so
    makes it resumable; ``COMPLETED`` when a degraded path was taken, because
    ``terminal_statuses() - stuck_statuses() == {COMPLETED}`` -- every other terminal
    status would claim the run needs operator intervention when it does not.

    Runs on the parent's queue: a router has no queue of its own (``:queue`` on a router
    label is a parse error), and the parent's is where the work already was.
    """
    placeholder = Task(
        id=cb.router.id,
        name=RERUN_ROUTER_TASK,
        version=0,
        queue=parent.queue,
        parameters={
            "router_spec": cb.router.model_dump(mode="json"),
            "parent_id": str(parent.id),
            "reason": exc.reason,
            "retryable": exc.retryable,
        },
        parent_ids=[parent.id],
        dag_run_id=parent.dag_run_id,
        dag_run_name=parent.dag_run_name,
        task_config=get_task_config(RERUN_ROUTER_TASK, 0),
    )
    placeholder.errors.append(str(exc))
    placeholder.submitted_at = dt.datetime.now(dt.UTC)
    placeholder.set_status(TaskStatus.COMPLETED if fell_back else TaskStatus.FAILED)
    return placeholder


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

        # Router callbacks: run the router and submit whichever candidate it picks. A
        # router that fails hands its branch to a placeholder or a degraded node, which
        # takes over this task's fan-in obligation -- those keys come back here so
        # generate_callbacks below doesn't also try to discharge them.
        delegated_by_routers: set[str] = set()
        for cb in task.dag_callbacks:
            if isinstance(cb, RouterCallback):
                delegated_by_routers |= await self._handle_router(task, cb)

        if task.has_callbacks():
            skip_keys = frozenset(outer_fan_in_cbs_by_key) | delegated_by_routers
            callbacks = await task.generate_callbacks(
                self.state_manager.task_state, skip_fan_in_keys=skip_keys
            )
            await self.state_manager.submit_tasks_batch(callbacks)

    async def post_process_error(self, task: Task) -> None:
        """Submit error callback tasks for a permanently-failed task."""
        error_callbacks = task.generate_error_callbacks()
        if error_callbacks:
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

    def _select_candidate(
        self,
        router: RouterSpec,
        results: dict[Any, Any],
        parent: Task,
        mode: str,
    ) -> DAGTaskSpec | None:
        """Thin instance wrapper over :func:`select_candidate` (kept for call-site brevity)."""
        return select_candidate(router, results, parent, mode)

    async def _handle_router(self, parent: Task, cb: RouterCallback) -> set[str]:
        """
        Run a router declared in a ``RouterCallback`` and submit the task it picks.

        Returns the set of the parent's ``fan_in_key``s that this router delegated to a
        successor, which the caller must not then decrement itself.

        A ``RouterError`` is contained here rather than allowed to escape
        ``post_process``: the parent's own unrelated callbacks are none of this router's
        business, and letting the exception through would silently drop them.
        """
        try:
            chosen = select_candidate(cb.router, parent.results, parent, mode="simple")
        except RouterError as exc:
            return await self._handle_router_failure(parent, cb, exc)
        if chosen is None:
            return set()
        try:
            await submit_router_choice(self.state_manager, parent, cb.router, chosen)
        except RouterError as exc:
            return await self._handle_router_failure(parent, cb, exc)
        return set()

    async def _handle_router_failure(self, parent: Task, cb: RouterCallback, exc: RouterError) -> set[str]:
        """
        Turn a router failure into a placeholder task, and optionally a degraded path.

        Without this a router failure is the only failure in jobbers with no task record:
        nothing in the UI, nothing in the DLQ, and no way to retry the routing short of
        re-running the parent. The placeholder is that record, and because it carries the
        router's own pre-assigned id it *is* the router node as far as the diagram and the
        DAG-run index are concerned.

        Two shapes, chosen by whether the diagram declared a ``-.->`` edge:

        - no ``-.->`` (**halt**) -- the placeholder is ``FAILED``, inherits the parent's
          fan-in obligation, and the run becomes resumable via ``POST /dags/{id}/resume``.
        - ``-.->`` present (**fall back**) -- the degraded node is submitted and inherits
          the obligation instead; the placeholder is written ``COMPLETED`` as an inert
          record, and the run reports ``degraded``.

        Returns the parent fan_in_keys delegated away, for the caller's skip set.
        """
        router_failures.add(1, {"router": cb.router.router, "reason": exc.reason})
        logger.warning("Router '%s' on task %s failed (%s): %s", cb.router.router, parent.id, exc.reason, exc)
        fell_back = cb.error_callback is not None
        placeholder = build_router_placeholder(parent, cb, exc, fell_back=fell_back)

        # The successor that takes over the branch inherits the parent's fan-in obligation:
        # whoever stands in for the routing must be what the collector waits on, or the
        # collector either fires early (nothing waits) or never (parent already discharged).
        fan_in_cbs = [c for c in parent.dag_callbacks if isinstance(c, FanInCallback)]

        if not fell_back:
            placeholder.dag_callbacks = list(fan_in_cbs)
            await self.state_manager.record_terminal_task(placeholder)
            await self._delegate_fan_ins(parent, placeholder.id, fan_in_cbs)
            return {c.fan_in_key for c in fan_in_cbs}

        router_fallbacks.add(1, {"router": cb.router.router})
        assert cb.error_callback is not None  # noqa: S101 — fell_back is exactly this check
        fallback = parent._build_callback_task(cb.error_callback, [parent.id])
        fallback.dag_callbacks = list(fallback.dag_callbacks) + list(fan_in_cbs)
        # Submit the degraded path *before* closing the placeholder: closing it can drive
        # the run's pending count to zero, and a zero there marks the run complete and
        # sweeps it. The fallback must already be pending when that check happens.
        await self.state_manager.submit_tasks_batch([fallback])
        await self._delegate_fan_ins(parent, fallback.id, fan_in_cbs)
        await self.state_manager.record_terminal_task(placeholder, outcome="degraded")
        return {c.fan_in_key for c in fan_in_cbs}

    async def _delegate_fan_ins(
        self, parent: Task, successor_id: ULID, fan_in_cbs: list[FanInCallback]
    ) -> None:
        """Swap *parent* for *successor_id* in each fan-in set, so the collector waits on it."""
        if not fan_in_cbs or parent.dag_run_id is None:
            return
        await asyncio.gather(
            *(
                self.state_manager.task_state.delegate_fan_in(
                    parent.dag_run_id, c.fan_in_key, parent.id, successor_id
                )
                for c in fan_in_cbs
            )
        )

    async def _handle_declarative_fanout(self, parent: Task, cb: DynamicFanOutCallback) -> None:
        """
        Drive a declarative fan-out declared in a ``DynamicFanOutCallback``.

        Reads ``parent.results[cb.items_key]`` (a list of dicts) and spawns one
        arm instance per entry. With ``cb.arm_root`` set, every arm clones that
        one template. With ``cb.arm_router`` set instead (mermaid ``A -->> R``),
        each item is routed on its own, so different items can start different
        tasks on different queues. Either way the entry's params are
        shallow-merged into the chosen template (entry values win), and
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

        # Build arm DAGNodes, merging per-item params into the arm template.
        arm_nodes: list[DAGNode] = []
        for item_params in items:
            merge_from = item_params if isinstance(item_params, dict) else {}
            if cb.arm_router is not None:
                template = self._select_candidate(cb.arm_router, merge_from, parent, mode="per_item")
                if template is None:
                    continue  # router declined this item; it contributes no arm
            else:
                template = cb.arm_root
            assert template is not None  # noqa: S101 -- guaranteed by the model validator
            cloned, _ = template.fresh_copy()
            merged_params = {**template.parameters, **merge_from}
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
            "list[SimpleCallback | FanInCallback | DynamicFanOutCallback | RouterCallback]",
            outer_fan_in_cbs,
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
