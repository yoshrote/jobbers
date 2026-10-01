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
# name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["default", "heavy_jobs", "_internal", "shard_1", "A"])
def test_valid_queue_names_are_accepted(name):
    assert QueueConfig(name=name).name == name


@pytest.mark.parametrize(
    "name",
    [
        "priority-shard-a",  # hyphens: the common mistake, since queue names once allowed them
        "1_shard",  # leading digit
        "my.queue",  # dot
        "queue 1",  # space
        "",  # empty
        "qüeue",  # non-ascii
    ],
)
def test_invalid_queue_names_are_rejected(name):
    """A queue name must be nameable from a mermaid edge label -- see QUEUE_NAME_PATTERN."""
    with pytest.raises(ValidationError):
        QueueConfig(name=name)


def test_queue_name_error_names_the_hyphen_fix():
    """The message has to teach the fix: hyphens used to be legal in queue names."""
    with pytest.raises(ValidationError, match="underscores"):
        QueueConfig(name="priority-shard-a")
