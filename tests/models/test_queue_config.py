import pytest

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
