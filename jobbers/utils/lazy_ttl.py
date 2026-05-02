import time
from typing import Any


class LazyTTL:
    """A utility class that provides lazy evaluation of values with a time-to-live (TTL) mechanism."""

    _NO_DEFAULT: object = object()

    def __init__(self, ttl: int) -> None:
        self.ttl = ttl
        self._values: dict[Any, Any] = {}
        self._last_updated: dict[Any, float] = {}

    def get(self, key: Any, default: Any = _NO_DEFAULT) -> Any:
        current_time = time.time()
        if key not in self._values or (current_time - self._last_updated.get(key, 0)) > self.ttl:
            self._values.pop(key, None)
            self._last_updated.pop(key, None)
            if default is not self._NO_DEFAULT:
                return default
            raise KeyError(f"Value for key '{key}' has expired or does not exist.")
        return self._values[key]

    def set(self, key: Any, value: Any) -> None:
        self._values[key] = value
        self._last_updated[key] = time.time()

    def pop(self, key: Any, default: Any = None) -> Any:
        self._last_updated.pop(key, None)
        return self._values.pop(key, default)

    def __contains__(self, key: object) -> bool:
        current_time = time.time()
        if key in self._values and (current_time - self._last_updated.get(key, 0)) <= self.ttl:
            return True
        self._values.pop(key, None)
        self._last_updated.pop(key, None)
        return False

    __getitem__ = get
    __setitem__ = set

    def __delitem__(self, key: Any) -> None:
        self.pop(key)
