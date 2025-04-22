import datetime

import msgpack
import pytest

from jobbers.serialization import default, ext_hook


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
