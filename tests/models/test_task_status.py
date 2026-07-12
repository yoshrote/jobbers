import pytest

from jobbers.models.task_status import TaskStatus


def test_all_values() -> None:
    assert set(TaskStatus) == {
        TaskStatus.UNSUBMITTED,
        TaskStatus.SUBMITTED,
        TaskStatus.STARTED,
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


def test_terminal_statuses() -> None:
    assert TaskStatus.terminal_statuses() == {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.STALLED,
        TaskStatus.DROPPED,
    }


def test_terminal_and_active_statuses_are_disjoint_and_exhaustive() -> None:
    """Every status is either terminal, active, or UNSUBMITTED — no status is both terminal and active."""
    assert TaskStatus.terminal_statuses() & TaskStatus.active_statuses() == set()
    assert TaskStatus.terminal_statuses() | TaskStatus.active_statuses() | {TaskStatus.UNSUBMITTED} == set(
        TaskStatus
    )


def test_stuck_statuses() -> None:
    assert TaskStatus.stuck_statuses() == {
        TaskStatus.FAILED,
        TaskStatus.STALLED,
        TaskStatus.CANCELLED,
        TaskStatus.DROPPED,
    }


def test_stuck_statuses_is_a_subset_of_terminal_statuses() -> None:
    assert TaskStatus.stuck_statuses() <= TaskStatus.terminal_statuses()
