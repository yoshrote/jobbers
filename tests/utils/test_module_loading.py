"""Unit tests for jobbers/utils/module_loading.py."""

import sys
import tempfile
from unittest.mock import patch

import pytest

from jobbers.utils.module_loading import load_task_module


def test_load_task_module_by_dotted_name():
    """A dotted module name is imported via importlib.import_module."""
    with patch("importlib.import_module") as mock_import:
        load_task_module("some.module.path")
    mock_import.assert_called_once_with("some.module.path")


def test_load_task_module_by_file_path():
    """An absolute .py path is loaded as a module from file."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        f.write("LOADED = True\n")
        path = f.name

    load_task_module(path)
    assert "_user_tasks" in sys.modules
    assert sys.modules["_user_tasks"].LOADED is True

    del sys.modules["_user_tasks"]


def test_load_task_module_by_relative_py_extension():
    """A path ending in .py (relative) is also loaded from file."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as f:
        f.write("VALUE = 42\n")
        path = f.name

    load_task_module(path)
    assert sys.modules["_user_tasks"].VALUE == 42
    del sys.modules["_user_tasks"]


def test_load_task_module_invalid_path_raises():
    """A non-existent absolute path raises ImportError."""
    with pytest.raises((ImportError, FileNotFoundError)):
        load_task_module("/nonexistent/path/tasks.py")
