"""Track and stop the shell processes of a run.

The ``Coder`` shell tool starts each command in its own session. The pid in the
``shell.command_started`` event is the session leader, so it is also the process
group id. A cancel stops the group, and so every child of the command.

The harness has no public stop call. The daemon sends the signals itself.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from collections.abc import Iterable
from typing import Any

# A command gets this long to leave after SIGTERM. Then it gets SIGKILL.
TOOL_STOP_GRACE_SECONDS = 2.0
# After SIGKILL, the daemon waits this long for the group to vanish.
TOOL_KILL_WAIT_SECONDS = 2.0
_POLL_SECONDS = 0.02


class ToolProcesses:
    """``run_id -> {tool_call_id: pid}`` for the commands a run started and did not finish.

    The event handler runs on the loop thread or on a workflow thread, and a
    cancel reads from the loop thread. A lock guards every access.

    A daemon restart loses this map. A cancel after a restart cannot stop a
    command that the old daemon started.
    """

    def __init__(self) -> None:
        self._pids: dict[str, dict[str, int]] = {}
        self._lock = threading.Lock()

    def observe(self, run_id: str, event: Any) -> None:
        """Update the map from one stream event. Ignore events that are not shell events."""
        kind = getattr(event, "kind", None)
        if kind not in ("shell.command_started", "shell.command_finished"):
            return
        tool_call_id = getattr(event, "tool_call_id", None)
        pid = getattr(event, "pid", None)
        if not isinstance(tool_call_id, str) or not isinstance(pid, int):
            return
        if kind == "shell.command_started":
            self.add(run_id, tool_call_id, pid)
        elif getattr(event, "exit_code", None) is not None:
            # ``command_finished`` also means that the tool stopped waiting for a
            # command that still runs (a timeout, or background mode). Only an
            # exit code proves the command is gone.
            self.remove(run_id, tool_call_id)

    def add(self, run_id: str, tool_call_id: str, pid: int) -> None:
        with self._lock:
            self._pids.setdefault(run_id, {})[tool_call_id] = pid

    def remove(self, run_id: str, tool_call_id: str) -> None:
        with self._lock:
            pids = self._pids.get(run_id)
            if pids is None:
                return
            pids.pop(tool_call_id, None)
            if not pids:
                del self._pids[run_id]

    def snapshot(self, run_id: str) -> dict[str, int]:
        with self._lock:
            return dict(self._pids.get(run_id, {}))

    def discard(self, run_id: str) -> None:
        """Forget a run. Call it when the run ends."""
        with self._lock:
            self._pids.pop(run_id, None)


def group_alive(pgid: int) -> bool:
    """Return ``True`` while the process group has a member that is not a zombie.

    ``killpg(pgid, 0)`` succeeds while a zombie remains. A zombie has exited and
    runs no code, but it stays until its parent reads its status. In some
    environments the parent is slow, so the check ignores zombies.
    """
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return _has_running_member(pgid) is not False


def _has_running_member(pgid: int) -> bool | None:
    """Scan ``/proc`` for a member of the group that is not a zombie.

    Return ``None`` when the scan cannot tell: there is no ``/proc``, or it holds
    no member of the group. The caller then trusts ``killpg``.
    """
    try:
        names = os.listdir("/proc")
    except OSError:
        return None
    found = False
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as handle:
                text = handle.read().decode("ascii", "replace")
        except OSError:
            continue
        # The command name sits in parentheses and can hold spaces. Read after the last one.
        fields = text[text.rfind(")") + 2 :].split()
        if len(fields) < 3 or int(fields[2]) != pgid:
            continue
        found = True
        if fields[0] not in ("Z", "X"):
            return True
    return False if found else None


def _signal_group(pgid: int, signum: int) -> None:
    try:
        os.killpg(pgid, signum)
    except ProcessLookupError:
        pass


async def _wait_gone(pgids: Iterable[int], timeout: float) -> list[int]:
    """Poll until every group is gone or the time ends. Return the groups that remain."""
    remaining = list(pgids)
    deadline = time.monotonic() + timeout
    while True:
        remaining = [pgid for pgid in remaining if group_alive(pgid)]
        if not remaining or time.monotonic() >= deadline:
            return remaining
        await asyncio.sleep(_POLL_SECONDS)


async def stop_process_groups(
    pgids: Iterable[int],
    *,
    grace: float = TOOL_STOP_GRACE_SECONDS,
    kill_wait: float = TOOL_KILL_WAIT_SECONDS,
) -> list[int]:
    """Stop process groups. Return the groups that still have a member.

    Send SIGTERM to each group that exists. Wait up to ``grace`` seconds. Send
    SIGKILL to each group that remains, and wait up to ``kill_wait`` seconds.
    A group that is already gone gets no signal, so a reused pid is safe unless
    another group has taken the id.
    """
    live = [pgid for pgid in pgids if group_alive(pgid)]
    for pgid in live:
        _signal_group(pgid, signal.SIGTERM)
    live = await _wait_gone(live, grace)
    for pgid in live:
        _signal_group(pgid, signal.SIGKILL)
    return await _wait_gone(live, kill_wait)


__all__ = [
    "TOOL_KILL_WAIT_SECONDS",
    "TOOL_STOP_GRACE_SECONDS",
    "ToolProcesses",
    "group_alive",
    "stop_process_groups",
]
