"""Unit tests for jobbers/runners/openapi_proc.py."""

import json
import tempfile
from pathlib import Path

import pytest

from jobbers.runners.openapi_proc import _write_spec


@pytest.mark.asyncio
async def test_write_spec_creates_file():
    """_write_spec writes a valid JSON file at the given path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "openapi.json"
        await _write_spec(output_path)
        assert output_path.exists()


@pytest.mark.asyncio
async def test_write_spec_output_is_valid_json():
    """The written file contains valid JSON."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "openapi.json"
        await _write_spec(output_path)
        content = output_path.read_text()
        data = json.loads(content)
        assert isinstance(data, dict)


@pytest.mark.asyncio
async def test_write_spec_contains_openapi_fields():
    """The output includes standard OpenAPI top-level keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "openapi.json"
        await _write_spec(output_path)
        data = json.loads(output_path.read_text())
        assert "openapi" in data
        assert "paths" in data


@pytest.mark.asyncio
async def test_write_spec_is_pretty_printed():
    """The output file is indented (pretty-printed) JSON."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "openapi.json"
        await _write_spec(output_path)
        content = output_path.read_text()
        # Pretty-printed JSON has newlines beyond the first line
        assert "\n" in content
