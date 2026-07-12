"""Unit tests for jobbers/runners/manager_proc.py."""

from unittest.mock import patch

import pytest

from jobbers.runners.manager_proc import run

# ── run() otel shutdown ───────────────────────────────────────────────────────


def test_run_calls_shutdown_otel_even_on_failure():
    """run() must flush/shut down otel providers even if uvicorn.run() raises."""
    with (
        patch("sys.argv", ["jobbers_manager", "os"]),
        patch("jobbers.runners.manager_proc.enable_otel"),
        patch("jobbers.runners.manager_proc.FastAPIInstrumentor"),
        patch("jobbers.runners.manager_proc.uvicorn.run", side_effect=RuntimeError("boom")),
        patch("jobbers.runners.manager_proc.shutdown_otel") as mock_shutdown,
        pytest.raises(RuntimeError, match="boom"),
    ):
        run()

    mock_shutdown.assert_called_once()
