import pytest

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy


def test_all_values() -> None:
    assert set(TaskShutdownPolicy) == {
        TaskShutdownPolicy.CONTINUE,
        TaskShutdownPolicy.STOP,
        TaskShutdownPolicy.RESUBMIT,
    }


@pytest.mark.parametrize("policy", list(TaskShutdownPolicy))
def test_bytes_roundtrip(policy: TaskShutdownPolicy) -> None:
    assert TaskShutdownPolicy.from_bytes(policy.to_bytes()) == policy


@pytest.mark.parametrize("policy", list(TaskShutdownPolicy))
def test_from_bytes_with_valid_bytes(policy: TaskShutdownPolicy) -> None:
    assert TaskShutdownPolicy.from_bytes(policy.value.encode()) == policy


@pytest.mark.parametrize("policy", list(TaskShutdownPolicy))
def test_to_bytes(policy: TaskShutdownPolicy) -> None:
    assert policy.to_bytes() == policy.value.encode()


def test_from_bytes_with_none() -> None:
    """Test from_bytes with None input returns default STOP policy."""
    result = TaskShutdownPolicy.from_bytes(None)
    assert result == TaskShutdownPolicy.STOP


def test_from_bytes_with_empty_bytes() -> None:
    """Test from_bytes with empty bytes input returns default STOP policy."""
    result = TaskShutdownPolicy.from_bytes(b"")
    assert result == TaskShutdownPolicy.STOP


def test_from_bytes_with_invalid_bytes() -> None:
    """Test from_bytes with invalid bytes input raises ValueError."""
    with pytest.raises(ValueError, match="'invalid_policy' is not a valid TaskShutdownPolicy"):
        TaskShutdownPolicy.from_bytes(b"invalid_policy")
