import datetime
from typing import Any

import msgpack
from ulid import ULID

EMPTY_DICT = b"\x80"
EMPTY_LIST = b"\x90"
NONE = b"\xc0"

EXT_DATETIME = 1
EXT_TIMEDELTA = 2
EXT_ULID = 3


def default(obj: object) -> Any:
    if isinstance(obj, datetime.datetime):
        return msgpack.ExtType(EXT_DATETIME, obj.isoformat().encode())
    elif isinstance(obj, datetime.timedelta):
        return msgpack.ExtType(EXT_TIMEDELTA, str(obj.total_seconds()).encode())
    elif isinstance(obj, ULID):
        return msgpack.ExtType(EXT_ULID, bytes(obj))
    raise TypeError(f"Unknown type: {obj!r}")


def ext_hook(code: int, data: bytes) -> Any:
    if code == EXT_DATETIME:
        return datetime.datetime.fromisoformat(data.decode())
    elif code == EXT_TIMEDELTA:
        return datetime.timedelta(seconds=float(data.decode()))
    elif code == EXT_ULID:
        return ULID.from_bytes(data)
    return msgpack.ExtType(code, data)


def serialize(obj: Any) -> bytes:
    return msgpack.packb(obj, default=default)


def deserialize(data: bytes) -> Any:
    return msgpack.unpackb(data, ext_hook=ext_hook)
