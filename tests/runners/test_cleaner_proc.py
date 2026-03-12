"""Unit tests for jobbers/runners/cleaner_proc.py."""

import argparse
import datetime as dt
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobbers.runners.cleaner_proc import cleaner

# ── cleaner ───────────────────────────────────────────────────────────────────


def _make_args(**kwargs: object) -> argparse.Namespace:
    defaults = {
        "rate_limit_age": None,
        "min_queue_age": None,
        "max_queue_age": None,
        "stale_time": None,
        "dlq_age": None,
        "completed_task_age": None,
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
