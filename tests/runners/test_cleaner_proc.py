"""Unit tests for jobbers/runners/cleaner_proc.py."""

import argparse
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobbers.runners.cleaner_proc import cleaner

# ── cleaner ───────────────────────────────────────────────────────────────────


def _make_args(**kwargs: object) -> argparse.Namespace:
    defaults = {
        "static_config": None,
        "rate_limit_age": None,
        "min_queue_age": None,
        "max_queue_age": None,
        "stale_time": None,
        "dlq_age": None,
        "completed_task_age": None,
        "recover_orphaned_scheduled": False,
        "drop_stale_indexes": False,
        "clean_orphaned_dlq": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@pytest.mark.asyncio
async def test_cleaner_calls_state_manager_clean():
    """cleaner() initializes state manager and calls clean() with the parsed args."""
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()

    args = _make_args()

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    state_manager.clean.assert_called_once_with(
        rate_limit_age=None,
        min_queue_age=None,
        max_queue_age=None,
        stale_time=None,
        dlq_age=None,
        completed_task_age=None,
        recover_orphaned_scheduled=False,
        drop_stale_indexes=False,
        clean_orphaned_dlq=False,
    )


@pytest.mark.asyncio
async def test_cleaner_passes_rate_limit_age():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()
    age = dt.timedelta(seconds=3600)

    args = _make_args(rate_limit_age=age)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    state_manager.clean.assert_called_once()
    _, kwargs = state_manager.clean.call_args
    assert kwargs["rate_limit_age"] == age


@pytest.mark.asyncio
async def test_cleaner_passes_stale_time():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()
    stale = dt.timedelta(seconds=120)

    args = _make_args(stale_time=stale)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["stale_time"] == stale


@pytest.mark.asyncio
async def test_cleaner_passes_dlq_age():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()
    age = dt.timedelta(days=7)

    args = _make_args(dlq_age=age)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["dlq_age"] == age


@pytest.mark.asyncio
async def test_cleaner_passes_completed_task_age():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()
    age = dt.timedelta(days=30)

    args = _make_args(completed_task_age=age)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["completed_task_age"] == age


@pytest.mark.asyncio
async def test_cleaner_passes_recover_orphaned_scheduled():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()

    args = _make_args(recover_orphaned_scheduled=True)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["recover_orphaned_scheduled"] is True


@pytest.mark.asyncio
async def test_cleaner_passes_drop_stale_indexes():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()

    args = _make_args(drop_stale_indexes=True)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["drop_stale_indexes"] is True


@pytest.mark.asyncio
async def test_cleaner_passes_clean_orphaned_dlq():
    state_manager = MagicMock()
    state_manager.clean = AsyncMock()

    args = _make_args(clean_orphaned_dlq=True)

    with patch("jobbers.db.init_state_manager", return_value=state_manager):
        await cleaner(args)

    _, kwargs = state_manager.clean.call_args
    assert kwargs["clean_orphaned_dlq"] is True
