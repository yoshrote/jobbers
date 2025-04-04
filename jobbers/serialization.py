import datetime

import msgpack

EMPTY_DICT = b"\x80"
NONE = b"\xc0"

def decode_optional_datetime(raw_data: bytes) -> datetime.datetime | None:
    if raw_data == NONE or not raw_data:
        return None
    return datetime.datetime.fromisoformat(raw_data.decode())

def decode_optional_string(raw_data: bytes) -> str | None:
    if raw_data == NONE or not raw_data:
        return None
    return raw_data.decode()

def default(obj):
    if isinstance(obj, datetime.datetime):
        return msgpack.ExtType(1, obj.isoformat().encode())
    raise TypeError(f"Unknown type: {obj!r}")

def ext_hook(code, data):
    if code == 1:
        return datetime.datetime.fromisoformat(data.decode())
    return msgpack.ExtType(code, data)

def serialize(obj):
    return msgpack.packb(obj, default=default)

def deserialize(data):
    return msgpack.unpackb(data, ext_hook=ext_hook)
