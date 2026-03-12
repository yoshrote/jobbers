import datetime as dt
from unittest.mock import patch

import pytest

from jobbers.models.task_config import BackoffStrategy, TaskConfig


async def dummy_task():
    pass


def make_config(**kwargs) -> TaskConfig:
    defaults = dict(
        name="test_task",
        function=dummy_task,
        retry_delay=10,
        max_retry_delay=3600,
        backoff_strategy=BackoffStrategy.CONSTANT,
    )
    defaults.update(kwargs)
    return TaskConfig(**defaults)


@pytest.fixture
def frozen_now():
    fixed = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.UTC)
    with patch("jobbers.models.task_config.dt.datetime") as mock_dt:
        mock_dt.now.return_value = fixed
        mock_dt.side_effect = lambda *a, **kw: dt.datetime(*a, **kw)
        yield fixed


class TestConstantBackoff:
    """Tests for the CONSTANT backoff strategy."""

    def test_returns_base_delay_regardless_of_attempt(self, frozen_now):
        config = make_config(retry_delay=10, backoff_strategy=BackoffStrategy.CONSTANT)
        for attempt in [1, 2, 5, 10]:
            result = config.compute_retry_at(attempt)
            assert result == frozen_now + dt.timedelta(seconds=10)

    def test_zero_delay(self, frozen_now):
        config = make_config(retry_delay=0, backoff_strategy=BackoffStrategy.CONSTANT)
        assert config.compute_retry_at(1) == frozen_now

    def test_none_delay_treated_as_zero(self, frozen_now):
        config = make_config(retry_delay=None, backoff_strategy=BackoffStrategy.CONSTANT)
        assert config.compute_retry_at(1) == frozen_now


class TestLinearBackoff:
    """Tests for the LINEAR backoff strategy."""

    def test_delay_scales_with_attempt(self, frozen_now):
        config = make_config(retry_delay=10, backoff_strategy=BackoffStrategy.LINEAR)
        assert config.compute_retry_at(1) == frozen_now + dt.timedelta(seconds=10)
        assert config.compute_retry_at(2) == frozen_now + dt.timedelta(seconds=20)
        assert config.compute_retry_at(5) == frozen_now + dt.timedelta(seconds=50)

    def test_attempt_zero_gives_zero_delay(self, frozen_now):
        config = make_config(retry_delay=10, backoff_strategy=BackoffStrategy.LINEAR)
        assert config.compute_retry_at(0) == frozen_now


class TestExponentialBackoff:
    """Tests for the EXPONENTIAL backoff strategy."""

    def test_delay_doubles_each_attempt(self, frozen_now):
        config = make_config(retry_delay=10, backoff_strategy=BackoffStrategy.EXPONENTIAL)
        assert config.compute_retry_at(0) == frozen_now + dt.timedelta(seconds=10)   # 10 * 2^0
        assert config.compute_retry_at(1) == frozen_now + dt.timedelta(seconds=20)   # 10 * 2^1
        assert config.compute_retry_at(2) == frozen_now + dt.timedelta(seconds=40)   # 10 * 2^2
        assert config.compute_retry_at(3) == frozen_now + dt.timedelta(seconds=80)   # 10 * 2^3

    def test_none_delay_treated_as_zero(self, frozen_now):
        config = make_config(retry_delay=None, backoff_strategy=BackoffStrategy.EXPONENTIAL)
        assert config.compute_retry_at(5) == frozen_now


class TestExponentialJitterBackoff:
    """Tests for the EXPONENTIAL_JITTER backoff strategy."""

    def test_result_is_within_bounds(self, frozen_now):
        config = make_config(retry_delay=10, backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER)
        attempt = 3  # max = 10 * 2^3 = 80
        result = config.compute_retry_at(attempt)
        assert frozen_now <= result <= frozen_now + dt.timedelta(seconds=80)

    def test_jitter_produces_variation(self):
        config = make_config(retry_delay=60, backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER)
        results = {config.compute_retry_at(3) for _ in range(20)}
        assert len(results) > 1, "Jitter should produce different values across calls"


class TestMaxRetryDelay:
    """Tests for the max_retry_delay parameter."""

    def test_delay_capped_at_max_retry_delay(self, frozen_now):
        config = make_config(
            retry_delay=100,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            max_retry_delay=500,
        )
        # 100 * 2^10 = 102400 >> 500
        result = config.compute_retry_at(10)
        assert result == frozen_now + dt.timedelta(seconds=500)

    def test_delay_below_cap_is_not_truncated(self, frozen_now):
        config = make_config(
            retry_delay=10,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            max_retry_delay=3600,
        )
        result = config.compute_retry_at(1)  # 10 * 2^1 = 20
        assert result == frozen_now + dt.timedelta(seconds=20)

    def test_constant_delay_exactly_at_cap(self, frozen_now):
        config = make_config(
            retry_delay=3600,
            backoff_strategy=BackoffStrategy.CONSTANT,
            max_retry_delay=3600,
        )
        assert config.compute_retry_at(1) == frozen_now + dt.timedelta(seconds=3600)

    def test_jitter_capped_at_max_retry_delay(self, frozen_now):
        config = make_config(
            retry_delay=10000,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            max_retry_delay=100,
        )
        for _ in range(10):
            result = config.compute_retry_at(10)
            assert result <= frozen_now + dt.timedelta(seconds=100)

    def test_exponential_exceeds_max_retry_delay_is_capped(self, frozen_now):
        """When exponential delay > max_retry_delay, result is capped at max."""
        config = make_config(
            retry_delay=1000,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            max_retry_delay=60,
        )
        result = config.compute_retry_at(5)  # 1000 * 2^5 = 32000 >> 60
        assert result <= frozen_now + dt.timedelta(seconds=60)
