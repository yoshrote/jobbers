"""
Tasks jobbers registers itself, under the reserved ``jobbers__`` name prefix.

These are not application tasks. They exist so that things the framework does to a DAG
have a task record an operator can see, retry and reason about, instead of happening
invisibly inside the worker.

Registration is imperative (``register_system_tasks()``) rather than import-time, so
``registry.reset_registry()`` can re-seed the baseline after clearing user tasks. It is
idempotent — re-registering the same function for the same name is allowed.
"""

from __future__ import annotations

import logging
from typing import Any

from ulid import ULID

from jobbers import db
from jobbers.constants import RERUN_ROUTER_TASK
from jobbers.models.dag import RouterSpec
from jobbers.models.task_config import DeadLetterPolicy
from jobbers.registry import register_task
from jobbers.task_processor import RouterError, select_candidate, submit_router_choice

logger = logging.getLogger(__name__)


async def rerun_router(**params: Any) -> dict[str, Any]:
    """
    Re-run a router that failed, against its parent's stored results.

    Created in a terminal status by ``TaskProcessor._handle_router_failure`` and only ever
    executed on a resume, so reaching this function means an operator deliberately retried
    the routing — presumably after deploying a fix.

    Nothing here re-runs the parent. A router is a pure function of results that are
    already persisted, which is the whole reason a placeholder is worth having: the
    expensive upstream work stays done and only the decision is retried. Siblings are
    likewise untouched, since this task is not the parent.

    Fan-in bookkeeping is deliberately absent. The placeholder carries the parent's
    ``FanInCallback``s in its own ``dag_callbacks``, so the ordinary ``post_process`` path
    discharges them when this task completes, exactly as it would for any other task
    holding those callbacks.
    """
    spec = RouterSpec.model_validate(params["router_spec"])
    parent_id = ULID.from_str(params["parent_id"])
    state_manager = db.get_state_manager()

    parent = await state_manager.task_state.get_task(parent_id)
    if parent is None:
        # The parent's blob has aged out (Cleaner's completed_task_age) — its results are
        # gone, so there is nothing to route on and no fix can recover this.
        raise RouterError(
            f"Cannot re-run router '{spec.router}': parent task {parent_id} no longer exists, "
            "so its results are unavailable. The DAG run must be re-submitted."
        )

    chosen = select_candidate(spec, parent.results, parent, mode="simple")
    if chosen is None:
        logger.info("Router '%s' re-run on task %s declined to route.", spec.router, parent_id)
        return {"routed": None}

    submitted = await submit_router_choice(state_manager, parent, spec, chosen)
    logger.info(
        "Router '%s' re-run on task %s routed to %s (%s).",
        spec.router,
        parent_id,
        submitted.name,
        submitted.id,
    )
    return {"routed": str(submitted.id), "task": submitted.name, "queue": submitted.queue}


def register_system_tasks() -> None:
    """
    Register jobbers' own tasks.

    Idempotent; called at worker/manager startup and by ``reset_registry``.
    """
    register_task(
        name=RERUN_ROUTER_TASK,
        version=0,
        # A router is pure over persisted results, so an automatic retry on a timer fails
        # identically — only a deploy changes the outcome. "Retry" here means an operator
        # resume, not a retry budget. The one transient case (an unregistered router
        # mid-rolling-deploy) is recorded as `retryable` on the placeholder for an operator
        # to act on, rather than burned through here.
        max_retries=0,
        # SAVE so a halted routing is visible in the DLQ alongside the DAG run view. The
        # degraded (COMPLETED) placeholder never reaches the DLQ regardless — a completed
        # task in a dead-letter queue would invert what the DLQ means.
        dead_letter_policy=DeadLetterPolicy.SAVE,
        max_concurrent=None,
        _system=True,
    )(rerun_router)
