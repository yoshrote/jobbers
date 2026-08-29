"""Unit tests for jobbers/subworker/status_file.py."""

import os

from jobbers.subworker import status_file


def test_read_status_missing_file_returns_none(tmp_path):
    path = str(tmp_path / "does-not-exist.txt")
    assert status_file.read_status(path) is None


def test_write_then_read_round_trips_request_id(tmp_path):
    path = str(tmp_path / "status.txt")
    status_file.write_status(path, "01JQC31AJP7TSA9X8AEP64XG08")
    assert status_file.read_status(path) == "01JQC31AJP7TSA9X8AEP64XG08"


def test_write_none_reads_back_as_none(tmp_path):
    path = str(tmp_path / "status.txt")
    status_file.write_status(path, "some-request")
    status_file.write_status(path, None)
    assert status_file.read_status(path) is None


def test_literal_zero_on_disk_reads_back_as_none(tmp_path):
    """The on-disk idle sentinel is the literal string '0', per the wire contract."""
    path = str(tmp_path / "status.txt")
    with open(path, "w") as f:
        f.write("0")
    assert status_file.read_status(path) is None


def test_write_cleans_up_its_temp_file(tmp_path):
    path = str(tmp_path / "status.txt")
    status_file.write_status(path, "req-1")
    leftovers = [p for p in os.listdir(tmp_path) if p != "status.txt"]
    assert leftovers == []


def test_write_overwrites_longer_previous_content(tmp_path):
    """A shorter new value must not leave trailing bytes from a longer previous write."""
    path = str(tmp_path / "status.txt")
    status_file.write_status(path, "a-very-long-request-id-value")
    status_file.write_status(path, "0")
    assert status_file.read_status(path) is None
