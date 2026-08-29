"""
Out-of-band subworker occupancy signal, independent of the message pipe/socket.

A pure OS-level signal (``SIGUSR1``/``SIGUSR2``) has no acknowledgment channel of its
own — the parent can't tell whether the subworker actually received and acted on a
cancel signal, only whether a ``ResultMsg`` eventually shows up on the transport. If the
subworker is genuinely stuck (a C call that never checks for pending signals), that
message never arrives and the parent has no way to distinguish "still working on it,
give it a moment" from "never going to respond, kill it now" without just waiting out
the full grace period every time.

Each handle gives its subworker a small status file; the child writes the request id
it's currently processing (or ``"0"`` while idle) to it, independently of whatever it's
doing with the pipe/socket. ``SubworkerPool.cancel()`` reads it as a tie-breaker once its
grace-period wait on the actual ``ResultMsg`` times out: if the file confirms the
subworker already moved off the cancelled request, skip the kill instead of tearing down
a subworker that's about to report back cleanly.

Writes go through a temp-file-plus-``os.replace`` so a concurrent read never observes a
truncated/torn value — a plain ``open(path, "w")`` first truncates the file to empty
before writing the new content, which a reader could catch mid-way.
"""

from __future__ import annotations

import os


def write_status(path: str, request_id: str | None) -> None:
    """Atomically record the request id the subworker is currently processing (None = idle)."""
    content = request_id if request_id is not None else "0"
    tmp_path = f"{path}.tmp-{os.getpid()}"
    with open(tmp_path, "w", encoding="ascii") as f:
        f.write(content)
    os.replace(tmp_path, path)


def read_status(path: str) -> str | None:
    """Return the request id the subworker last reported processing, or None if idle/unreadable."""
    try:
        with open(path, encoding="ascii") as f:
            content = f.read().strip()
    except OSError:
        return None
    return None if content in ("", "0") else content
