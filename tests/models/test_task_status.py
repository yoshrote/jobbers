import pytest

from jobbers.models.task_status import TaskStatus


def test_all_values() -> None:
    assert set(TaskStatus) == {
        TaskStatus.UNSUBMITTED,
        TaskStatus.SUBMITTED,
        TaskStatus.STARTED,
        TaskStatus.HEARTBEAT,
        TaskStatus.COMPLETED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.FAILED,
        TaskStatus.DROPPED,
        TaskStatus.SCHEDULED,
    }


@pytest.mark.parametrize("status", list(TaskStatus))
def test_bytes_roundtrip(status: TaskStatus) -> None:
    assert TaskStatus.from_bytes(status.to_bytes()) == status


def test_from_bytes_with_none() -> None:
    """Test from_bytes with None input."""
    result = TaskStatus.from_bytes(None)
    assert result == TaskStatus.UNSUBMITTED


def test_from_bytes_with_empty_bytes() -> None:
    """Test from_bytes with empty bytes input."""
    result = TaskStatus.from_bytes(b"")
    assert result == TaskStatus.UNSUBMITTED


def test_from_bytes_with_valid_bytes() -> None:
    """Test from_bytes with valid bytes input."""
    result = TaskStatus.from_bytes(b"completed")
    assert result == TaskStatus.COMPLETED


def test_from_bytes_with_invalid_bytes() -> None:
    """Test from_bytes with invalid bytes input."""
    with pytest.raises(ValueError, match="is not a valid TaskStatus"):
        TaskStatus.from_bytes(b"invalid_status")
