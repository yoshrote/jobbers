import pytest
from pydantic import ValidationError

from jobbers.models.queue_config import QueueConfig, RatePeriod

# ---------------------------------------------------------------------------
# RatePeriod
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rate_period", "rate_denominator", "expected_seconds"),
    [
        (None, 1, None),
        (RatePeriod.SECOND, None, None),
        (RatePeriod.SECOND, 1, 1),
        (RatePeriod.MINUTE, 1, 60),
        (RatePeriod.HOUR, 1, 3600),
        (RatePeriod.DAY, 1, 86400),
    ],
)
def test_period_in_seconds(rate_period, rate_denominator, expected_seconds):
    config = QueueConfig(name="test_queue", rate_denominator=rate_denominator, rate_period=rate_period)
    assert config.period_in_seconds() == expected_seconds


# ---------------------------------------------------------------------------
# max_concurrent
# ---------------------------------------------------------------------------


def test_max_concurrent_zero_is_accepted():
    """0 is a valid value -- it means unlimited, same as None (not "blocked")."""
    config = QueueConfig(name="test_queue", max_concurrent=0)
    assert config.max_concurrent == 0


def test_max_concurrent_negative_is_rejected():
    with pytest.raises(ValidationError):
        QueueConfig(name="test_queue", max_concurrent=-1)


# ---------------------------------------------------------------------------
# has_sync_tasks / from_row
# ---------------------------------------------------------------------------


def test_has_sync_tasks_defaults_false():
    config = QueueConfig(name="test_queue")
    assert config.has_sync_tasks is False


def test_from_row_round_trips_has_sync_tasks():
    row = ("test_queue", 5, None, None, None, True)
    config = QueueConfig.from_row(row)
    assert config.name == "test_queue"
    assert config.max_concurrent == 5
    assert config.has_sync_tasks is True


def test_from_row_coerces_sql_falsy_has_sync_tasks():
    """SQLite stores Boolean as 0/1, not a Python bool -- from_row must coerce it."""
    row = ("test_queue", 5, None, None, None, 0)
    config = QueueConfig.from_row(row)
    assert config.has_sync_tasks is False
