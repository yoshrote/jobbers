"""Unit tests for jobbers/subworker/context.py."""

import pytest

from jobbers.subworker.context import _current_heartbeat_sender, heartbeat


def test_heartbeat_raises_outside_context():
    with pytest.raises(RuntimeError, match="outside of a running sync_subworker task"):
        heartbeat()


def test_heartbeat_calls_the_registered_sender():
    calls = []
    token = _current_heartbeat_sender.set(lambda: calls.append(1))
    try:
        heartbeat()
        heartbeat()
    finally:
        _current_heartbeat_sender.reset(token)
    assert calls == [1, 1]


def test_heartbeat_raises_again_after_context_reset():
    token = _current_heartbeat_sender.set(lambda: None)
    _current_heartbeat_sender.reset(token)
    with pytest.raises(RuntimeError):
        heartbeat()
