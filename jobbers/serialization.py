import datetime

import msgpack

EMPTY_DICT = b"\x80"
NONE = b"\xc0"


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
