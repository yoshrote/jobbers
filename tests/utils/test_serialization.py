import datetime

import msgpack
import pytest
from ulid import ULID

from jobbers.utils.serialization import default, deserialize, ext_hook, serialize


def test_default_with_datetime():
    obj = datetime.datetime(2023, 1, 1, 12, 0, 0)
    result = default(obj)
    assert isinstance(result, msgpack.ExtType)
    assert result.code == 1
    assert result.data == b"2023-01-01T12:00:00"


def test_default_with_unknown_type():
    class CustomType:
        pass

    obj = CustomType()
    with pytest.raises(TypeError, match=f"Unknown type: {obj!r}"):
        default(obj)


def test_ext_hook_with_datetime_code():
    code = 1
    data = b"2023-01-01T12:00:00"
    result = ext_hook(code, data)
    assert isinstance(result, datetime.datetime)
    assert result == datetime.datetime(2023, 1, 1, 12, 0, 0)


def test_ext_hook_with_unknown_code():
    code = 99
    data = b"some_data"
    result = ext_hook(code, data)
    assert isinstance(result, msgpack.ExtType)
    assert result.code == 99
    assert result.data == b"some_data"


def test_default_with_timedelta():
    obj = datetime.timedelta(seconds=90, microseconds=500000)
    result = default(obj)
    assert isinstance(result, msgpack.ExtType)
    assert result.code == 2
    assert result.data == str(obj.total_seconds()).encode()


def test_ext_hook_with_timedelta_code():
    data = b"90.5"
    result = ext_hook(2, data)
    assert isinstance(result, datetime.timedelta)
    assert result == datetime.timedelta(seconds=90, microseconds=500000)


def test_default_with_ulid():
    ulid = ULID()
    result = default(ulid)
    assert isinstance(result, msgpack.ExtType)
    assert result.code == 3
    assert result.data == bytes(ulid)
    assert len(result.data) == 16


def test_ext_hook_with_ulid_code():
    ulid = ULID()
    result = ext_hook(3, bytes(ulid))
    assert isinstance(result, ULID)
    assert result == ulid


def test_ulid_round_trip():
    ulid = ULID()
    packed = serialize(ulid)
    unpacked = deserialize(packed)
    assert isinstance(unpacked, ULID)
    assert unpacked == ulid


def test_ulid_round_trip_in_dict():
    ulid1 = ULID()
    ulid2 = ULID()
    data = {"parent_ids": [ulid1, ulid2], "cron_id": ulid1, "dag_run_id": None}
    packed = serialize(data)
    unpacked = deserialize(packed)
    assert unpacked["parent_ids"] == [ulid1, ulid2]
    assert unpacked["cron_id"] == ulid1
    assert unpacked["dag_run_id"] is None


def test_ulid_binary_smaller_than_string():
    ulid = ULID()
    binary_packed = serialize(ulid)
    string_packed = serialize(str(ulid))
    assert len(binary_packed) < len(string_packed)
