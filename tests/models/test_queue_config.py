import pytest

from jobbers.models.queue_config import QueueConfig, RatePeriod


@pytest.mark.parametrize(("rate_period", "rate_denominator", "expected_seconds"), [
    # bad configurations
    (None, 1, None),
    (RatePeriod.SECOND, None, None),
    # real configurations
    (RatePeriod.SECOND, 1, 1),
    (RatePeriod.MINUTE, 1, 60),
    (RatePeriod.HOUR, 1, 3600),
    (RatePeriod.DAY, 1, 86400),
])
def test_period_in_seconds(rate_period, rate_denominator, expected_seconds):
    config = QueueConfig(
        name="test_queue",
        rate_denominator=rate_denominator,
        rate_period=rate_period
    )
    assert config.period_in_seconds() == expected_seconds

@pytest.mark.parametrize(("input_value", "expected_result"), [
    (None, None),  # Test with None input
    (b"second", RatePeriod.SECOND),  # Valid input for SECOND
    (b"minute", RatePeriod.MINUTE),  # Valid input for MINUTE
    (b"hour", RatePeriod.HOUR),  # Valid input for HOUR
    (b"day", RatePeriod.DAY),  # Valid input for DAY
    (b"invalid", None),  # Invalid input
])
def test_from_bytes(input_value, expected_result):
    assert RatePeriod.from_bytes(input_value) == expected_result

@pytest.mark.parametrize(("rate_period", "expected_bytes"), [
    (RatePeriod.SECOND, b"second"),
    (RatePeriod.MINUTE, b"minute"),
    (RatePeriod.HOUR, b"hour"),
    (RatePeriod.DAY, b"day"),
])
def test_to_bytes(rate_period, expected_bytes):
    assert rate_period.to_bytes() == expected_bytes
