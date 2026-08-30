"""Unit tests for jobbers/subworker/wire.py framing helpers."""

import asyncio
import io

import pytest

from jobbers.subworker import wire


def test_pack_and_read_frame_sync_round_trip():
    payload = {"type": "dispatch", "request_id": "abc", "kwargs": {"a": 1, "b": [1, 2, 3]}}
    buf = io.BytesIO()
    wire.write_frame_sync(buf, payload)
    buf.seek(0)
    assert wire.read_frame_sync(buf) == payload


def test_read_frame_sync_returns_none_on_clean_eof():
    buf = io.BytesIO(b"")
    assert wire.read_frame_sync(buf) is None


def test_read_frame_sync_raises_on_truncated_frame():
    buf = io.BytesIO(b"\x00\x00\x00\x05ab")  # header claims 5 bytes, only 2 follow
    with pytest.raises(ConnectionError):
        wire.read_frame_sync(buf)


@pytest.mark.asyncio
async def test_pack_and_read_frame_async_round_trip():
    payload = {"type": "result", "request_id": "xyz", "ok": True, "result": None, "error": None}
    reader = asyncio.StreamReader()
    reader.feed_data(wire.pack_frame(payload))
    reader.feed_eof()
    assert await wire.read_frame_async(reader) == payload


@pytest.mark.asyncio
async def test_read_frame_async_returns_none_on_clean_eof():
    reader = asyncio.StreamReader()
    reader.feed_eof()
    assert await wire.read_frame_async(reader) is None


@pytest.mark.asyncio
async def test_read_frame_async_raises_on_truncated_frame():
    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00\x00\x05ab")
    reader.feed_eof()
    with pytest.raises(ConnectionError):
        await wire.read_frame_async(reader)
