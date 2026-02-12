import datetime
from typing import Any

import msgpack

EMPTY_DICT = b"\x80"
NONE = b"\xc0"


def default(obj: object) -> Any:
    if isinstance(obj, datetime.datetime):
        return msgpack.ExtType(1, obj.isoformat().encode())
    elif isinstance(obj, datetime.timedelta):
        return msgpack.ExtType(2, str(obj.total_seconds()).encode())
    raise TypeError(f"Unknown type: {obj!r}")

def ext_hook(code: int, data: bytes) -> Any:
    if code == 1:
        return datetime.datetime.fromisoformat(data.decode())
    elif code == 2:
        return datetime.timedelta(seconds=float(data.decode()))
    return msgpack.ExtType(code, data)

def serialize(obj: Any) -> bytes:
    return msgpack.packb(obj, default=default)

def deserialize(data: bytes) -> Any:
    return msgpack.unpackb(data, ext_hook=ext_hook)
