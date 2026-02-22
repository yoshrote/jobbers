import pytest

from jobbers.models.task_status import TaskStatus


def test_from_bytes_with_none():
    """Test from_bytes with None input."""
    result = TaskStatus.from_bytes(None)
    assert result == TaskStatus.UNSUBMITTED


def test_from_bytes_with_empty_bytes():
    """Test from_bytes with empty bytes input."""
    result = TaskStatus.from_bytes(b"")
    assert result == TaskStatus.UNSUBMITTED


def test_from_bytes_with_valid_bytes():
    """Test from_bytes with valid bytes input."""
    result = TaskStatus.from_bytes(b"completed")
    assert result == TaskStatus.COMPLETED


def test_from_bytes_with_invalid_bytes():
    """Test from_bytes with invalid bytes input."""
    with pytest.raises(ValueError, match="is not a valid TaskStatus"):
        TaskStatus.from_bytes(b"invalid_status")
