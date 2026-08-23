"""
Length-prefixed MessagePack framing for the stdio subworker transport.

``Frame := u32_be(N) ++ msgpack_bytes[N]`` — see
``.claude/plans/cross-language-subworker-stdio-design.md`` §5. Messages are plain
MessagePack maps with a ``"type"`` field, not Python-specific objects: every value must
be one of MessagePack's native types (``None``/``bool``/``int``/``float``/``str``/
``bytes``/``list``/``dict``) so the framing is decodable by a subworker written in any
language, not just Python's own ``msgpack`` bindings with this project's custom ext
types (``jobbers.utils.serialization``, which is deliberately not used here).
"""

from __future__ import annotations

import asyncio
import struct
from typing import TYPE_CHECKING, Any, cast

import msgpack

if TYPE_CHECKING:
    from typing import BinaryIO

_LEN_STRUCT = struct.Struct(">I")


def pack_frame(payload: dict[str, Any]) -> bytes:
    body = msgpack.packb(payload, use_bin_type=True)
    return _LEN_STRUCT.pack(len(body)) + body


def unpack_body(body: bytes) -> dict[str, Any]:
    return cast("dict[str, Any]", msgpack.unpackb(body, raw=False))


async def read_frame_async(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    """Read one frame from an asyncio stream. Returns None on EOF before any bytes were read."""
    try:
        header = await reader.readexactly(_LEN_STRUCT.size)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise ConnectionError("subworker closed mid-frame-header") from exc
        return None
    (length,) = _LEN_STRUCT.unpack(header)
    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionError("subworker closed mid-frame-body") from exc
    return unpack_body(body)


def read_frame_sync(stream: BinaryIO) -> dict[str, Any] | None:
    """Read one frame from a blocking, buffered binary stream (e.g. sys.stdin.buffer)."""
    header = _read_exact_sync(stream, _LEN_STRUCT.size)
    if header is None:
        return None
    (length,) = _LEN_STRUCT.unpack(header)
    body = _read_exact_sync(stream, length)
    if body is None:
        raise ConnectionError("peer closed mid-frame-body")
    return unpack_body(body)


def write_frame_sync(stream: BinaryIO, payload: dict[str, Any]) -> None:
    stream.write(pack_frame(payload))
    stream.flush()


def _read_exact_sync(stream: BinaryIO, size: int) -> bytes | None:
    if size == 0:
        return b""
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            if remaining == size:
                return None
            raise ConnectionError("stream closed mid-read")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
