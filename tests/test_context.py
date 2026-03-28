"""Tests for jobbers.context."""

import pytest

from jobbers.context import get_current_task


def test_get_current_task_raises_outside_task():
    """get_current_task() raises RuntimeError when called outside a running task."""
    with pytest.raises(RuntimeError, match="called outside"):
        get_current_task()
