import pytest

from jobbers.models.task_shutdown_policy import TaskShutdownPolicy


def test_enum_values():
    """Test that TaskShutdownPolicy has the expected enum values."""
    assert TaskShutdownPolicy.CONTINUE == "continue"
    assert TaskShutdownPolicy.STOP == "stop"
    assert TaskShutdownPolicy.RESUBMIT == "resubmit"


def test_from_bytes_with_none():
    """Test from_bytes with None input returns default STOP policy."""
    result = TaskShutdownPolicy.from_bytes(None)
    assert result == TaskShutdownPolicy.STOP


def test_from_bytes_with_empty_bytes():
    """Test from_bytes with empty bytes input returns default STOP policy."""
    result = TaskShutdownPolicy.from_bytes(b"")
    assert result == TaskShutdownPolicy.STOP


@pytest.mark.parametrize(("input_bytes", "expected_policy"), [
    (b"continue", TaskShutdownPolicy.CONTINUE),
    (b"stop", TaskShutdownPolicy.STOP),
    (b"resubmit", TaskShutdownPolicy.RESUBMIT),
])
def test_from_bytes_with_valid_bytes(input_bytes, expected_policy):
    """Test from_bytes with valid bytes input."""
    result = TaskShutdownPolicy.from_bytes(input_bytes)
    assert result == expected_policy


def test_from_bytes_with_invalid_bytes():
    """Test from_bytes with invalid bytes input raises ValueError."""
    with pytest.raises(ValueError, match="'invalid_policy' is not a valid TaskShutdownPolicy"):
        TaskShutdownPolicy.from_bytes(b"invalid_policy")


@pytest.mark.parametrize(("policy", "expected_bytes"), [
    (TaskShutdownPolicy.CONTINUE, b"continue"),
    (TaskShutdownPolicy.STOP, b"stop"),
    (TaskShutdownPolicy.RESUBMIT, b"resubmit"),
])
def test_to_bytes(policy, expected_bytes):
    """Test to_bytes method returns correct bytes representation."""
    result = policy.to_bytes()
    assert result == expected_bytes

def test_bytes_roundtrip():
    """Test that converting to bytes and back preserves the original value."""
    for policy in TaskShutdownPolicy:
        bytes_repr = policy.to_bytes()
        reconstructed = TaskShutdownPolicy.from_bytes(bytes_repr)
        assert reconstructed == policy
