"""Unit tests for StaticRoutingBackend."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from unittest.mock import patch

import pytest
import pytest_asyncio

from jobbers.adapters.static import StaticRoutingBackend
from jobbers.models.queue_config import QueueConfig
from jobbers.protocols import RoutingBackendReadOnlyError


@pytest_asyncio.fixture
async def backend():
    return StaticRoutingBackend(
        queues=[
            QueueConfig(name="default", max_concurrent=10),
            QueueConfig(name="fast", max_concurrent=2),
        ],
        roles={"default": {"default"}, "fast-workers": {"fast", "default"}},
    )


# ── reads ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_queue_config(backend):
    result = await backend.get_queue_config("default")
    assert result is not None
    assert result.max_concurrent == 10


@pytest.mark.asyncio
async def test_get_queue_config_missing(backend):
    assert await backend.get_queue_config("nowhere") is None


@pytest.mark.asyncio
async def test_get_all_queues_sorted(backend):
    assert await backend.get_all_queues() == ["default", "fast"]


@pytest.mark.asyncio
async def test_get_queues_for_role(backend):
    assert await backend.get_queues("fast-workers") == {"fast", "default"}


@pytest.mark.asyncio
async def test_get_queues_for_unknown_role(backend):
    assert await backend.get_queues("nope") == set()


@pytest.mark.asyncio
async def test_get_all_roles_sorted(backend):
    assert await backend.get_all_roles() == ["default", "fast-workers"]


@pytest.mark.asyncio
async def test_get_roles_for_queue_returns_containing_roles(backend):
    roles = await backend.get_roles_for_queue("fast")
    assert set(roles) == {"fast-workers"}


@pytest.mark.asyncio
async def test_get_roles_for_queue_returns_multiple_roles(backend):
    roles = await backend.get_roles_for_queue("default")
    assert set(roles) == {"default", "fast-workers"}


@pytest.mark.asyncio
async def test_get_roles_for_queue_returns_empty_for_unknown(backend):
    roles = await backend.get_roles_for_queue("nonexistent")
    assert roles == []


# ── defaults ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_default_backend_has_default_queue_and_role():
    backend = StaticRoutingBackend()
    assert await backend.get_queue_config("default") is not None
    assert "default" in await backend.get_queues("default")


# ── write operations raise ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_queue_config_raises(backend):
    with pytest.raises(RoutingBackendReadOnlyError):
        await backend.save_queue_config(QueueConfig(name="x"))


@pytest.mark.asyncio
async def test_delete_queue_raises(backend):
    with pytest.raises(RoutingBackendReadOnlyError):
        await backend.delete_queue("default")


@pytest.mark.asyncio
async def test_save_role_raises(backend):
    with pytest.raises(RoutingBackendReadOnlyError):
        await backend.save_role("r", {"q"})


@pytest.mark.asyncio
async def test_delete_role_raises(backend):
    with pytest.raises(RoutingBackendReadOnlyError):
        await backend.delete_role("default")


# ── from_file ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_from_file_json():
    data = {
        "queues": [{"name": "file_q", "max_concurrent": 4}],
        "roles": {"file_role": ["file_q"]},
    }
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(data, f)
        path = f.name

    try:
        b = StaticRoutingBackend.from_file(path)
        assert await b.get_queue_config("file_q") is not None
        assert (await b.get_queue_config("file_q")).max_concurrent == 4
        assert await b.get_queues("file_role") == {"file_q"}
    finally:
        os.unlink(path)


def test_from_file_unknown_queue_in_role(tmp_path):
    data = {
        "queues": [{"name": "q1", "max_concurrent": 2}],
        "roles": {"r": ["q1", "typo_q"]},
    }
    p = tmp_path / "config.json"
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="unknown queue 'typo_q'"):
        StaticRoutingBackend.from_file(str(p))


def test_from_file_yaml_raises_when_pyyaml_missing(tmp_path):
    """from_file() raises ImportError with install instructions when loading YAML without PyYAML."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("queues: []\n")

    with patch.dict(sys.modules, {"yaml": None}):
        with pytest.raises(ImportError, match="pip install jobbers"):
            StaticRoutingBackend.from_file(str(yaml_file))


@pytest.mark.asyncio
async def test_from_file_yaml(tmp_path):
    """from_file() loads YAML config when PyYAML is available."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "queues:\n  - name: yaml_q\n    max_concurrent: 5\nroles:\n  yaml_role:\n    - yaml_q\nrouting: []\n"
    )
    b = StaticRoutingBackend.from_file(str(yaml_file))
    assert await b.get_queue_config("yaml_q") is not None
    assert (await b.get_queue_config("yaml_q")).max_concurrent == 5
    assert await b.get_queues("yaml_role") == {"yaml_q"}
