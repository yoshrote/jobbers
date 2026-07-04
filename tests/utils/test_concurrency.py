"""Unit tests for jobbers/utils/concurrency.py."""

import asyncio

import pytest

from jobbers.utils.concurrency import has_capacity


def test_has_capacity_true_on_fresh_semaphore():
    sem = asyncio.Semaphore(2)
    assert has_capacity(sem) is True


@pytest.mark.asyncio
async def test_has_capacity_false_after_acquiring_all_slots():
    sem = asyncio.Semaphore(2)
    await sem.acquire()
    await sem.acquire()
    assert has_capacity(sem) is False


@pytest.mark.asyncio
async def test_has_capacity_true_again_after_release():
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    assert has_capacity(sem) is False
    sem.release()
    assert has_capacity(sem) is True


@pytest.mark.asyncio
async def test_has_capacity_is_side_effect_free():
    sem = asyncio.Semaphore(1)
    assert has_capacity(sem) is True
    assert has_capacity(sem) is True
    # Calling it repeatedly must not consume the slot — a real acquire still succeeds.
    await sem.acquire()
