"""Systemd socket activation stays at the daemon transport boundary."""

from __future__ import annotations

import os

import pytest

from agentman import daemon
from agentman.config import Config, Value


def _config() -> Config:
    return Config(
        agent=Value("default", "test"),
        model=Value(None, "test"),
        database_url=Value("postgresql://test", "test"),
        socket=Value("/tmp/agentman-test.sock", "test"),
        router_url=Value(None, "test"),
    )


def test_systemd_listen_fd_requires_one_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LISTEN_FDS", raising=False)
    monkeypatch.delenv("LISTEN_PID", raising=False)
    assert daemon.systemd_listen_fd() is None
    with pytest.raises(RuntimeError, match="requires one LISTEN_FDS"):
        daemon.systemd_listen_fd(required=True)


@pytest.mark.parametrize("count", ["0", "2", "not-a-number"])
def test_systemd_listen_fd_rejects_wrong_descriptor_count(monkeypatch: pytest.MonkeyPatch, count: str) -> None:
    monkeypatch.setenv("LISTEN_FDS", count)
    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))

    with pytest.raises(RuntimeError, match="LISTEN_FDS|exactly one"):
        daemon.systemd_listen_fd()


def test_systemd_listen_fd_rejects_a_different_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LISTEN_FDS", "1")
    monkeypatch.setenv("LISTEN_PID", str(os.getpid() + 1))

    with pytest.raises(RuntimeError, match="belongs to pid"):
        daemon.systemd_listen_fd()


def test_systemd_service_uses_fd_without_binding_socket_path(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    captured: dict[str, object] = {}

    class FakeConfig:
        def __init__(self, app: object, **kwargs: object) -> None:
            captured["app"] = app
            captured.update(kwargs)

    class FakeServer:
        def __init__(self, config: object) -> None:
            captured["config"] = config

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(uvicorn, "Config", FakeConfig)
    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    monkeypatch.setattr(daemon, "build_app", lambda config: "app")
    monkeypatch.setenv("LISTEN_FDS", "1")
    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))

    daemon.serve(_config(), systemd_socket=True)

    assert captured["fd"] == 3
    assert "uds" not in captured
    assert captured["ran"] is True


def test_development_service_keeps_unix_socket_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    captured: dict[str, object] = {}

    class FakeConfig:
        def __init__(self, app: object, **kwargs: object) -> None:
            captured.update(kwargs)

    class FakeServer:
        def __init__(self, config: object) -> None:
            pass

        def run(self) -> None:
            pass

    monkeypatch.setattr(uvicorn, "Config", FakeConfig)
    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    monkeypatch.setattr(daemon, "build_app", lambda config: "app")
    monkeypatch.delenv("LISTEN_FDS", raising=False)
    monkeypatch.delenv("LISTEN_PID", raising=False)

    daemon.serve(_config())

    assert captured["uds"] == "/tmp/agentman-test.sock"
    assert "fd" not in captured


def test_systemd_mode_without_fd_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LISTEN_FDS", raising=False)
    monkeypatch.delenv("LISTEN_PID", raising=False)

    with pytest.raises(RuntimeError, match="requires one LISTEN_FDS"):
        daemon.serve(_config(), systemd_socket=True)
