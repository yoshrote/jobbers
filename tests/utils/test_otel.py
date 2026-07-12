"""Unit tests for jobbers/utils/otel.py's shutdown_otel()."""

from unittest.mock import MagicMock, patch

from jobbers.utils.otel import shutdown_otel


def test_shutdown_otel_shuts_down_all_three_providers():
    """shutdown_otel() calls shutdown() on the tracer, meter, and logger providers."""
    mock_tracer_provider = MagicMock()
    mock_meter_provider = MagicMock()
    mock_logger_provider = MagicMock()

    with (
        patch("jobbers.utils.otel.trace.get_tracer_provider", return_value=mock_tracer_provider),
        patch("jobbers.utils.otel.metrics.get_meter_provider", return_value=mock_meter_provider),
        patch("jobbers.utils.otel.get_logger_provider", return_value=mock_logger_provider),
    ):
        shutdown_otel()

    mock_tracer_provider.shutdown.assert_called_once()
    mock_meter_provider.shutdown.assert_called_once()
    mock_logger_provider.shutdown.assert_called_once()


def test_shutdown_otel_skips_providers_without_shutdown():
    """The SDK's default proxy providers (before enable_otel() runs) have no shutdown() -- must not raise."""

    class NoShutdown:
        pass

    with (
        patch("jobbers.utils.otel.trace.get_tracer_provider", return_value=NoShutdown()),
        patch("jobbers.utils.otel.metrics.get_meter_provider", return_value=NoShutdown()),
        patch("jobbers.utils.otel.get_logger_provider", return_value=NoShutdown()),
    ):
        shutdown_otel()  # must not raise
