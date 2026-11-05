"""Check 3: ``git diff | agentman pipe "review this"`` works.

The whole client arc runs: resolve config and the repo's Python type, open the
socket, stream the turn, validate the output in the repository interpreter.
The daemon is a real threaded uvicorn server over a Unix socket; the model is a
streaming mock over real TCP.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from support.openai_transport import MockOpenAI, build_tcp_model, serve_openai_mock
from typer.testing import CliRunner

from agentman.cli import app
from agentman.config import CONFIG_FILENAME, Config, Value
from agentman.daemon import build_app
from agentman.persistence.engine import create_engine
from agentman.persistence.store import Run, Store, Thread

pytestmark = pytest.mark.integration

MODELS = """
from pydantic import BaseModel


class Review(BaseModel):
    summary: str
"""


def _request_prompt(request: dict[str, Any]) -> str:
    messages = request.get("messages", [])
    assert isinstance(messages, list)
    for message in reversed(messages):
        if isinstance(message, dict) and message.get("role") == "user":
            content = message.get("content", "")
            return content if isinstance(content, str) else json.dumps(content, sort_keys=True)
    raise AssertionError("mock request has no user message")


def test_git_diff_pipe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database_url: str) -> None:
    import agentman.runtime as runtime

    workspace = tmp_path / "workspace"
    (workspace / ".agentman").mkdir(parents=True)
    (workspace / ".agentman" / CONFIG_FILENAME).write_text("agent: reviewer\nmodel: mock:model\n")
    (workspace / ".agentman" / "reviewer.yaml").write_text("instructions: review the diff\noutput: models:Review\n")
    (workspace / ".agentman" / "models.py").write_text(MODELS)
    venv_python = workspace / ".venv/bin/python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text(f'#!/bin/sh\nexec {shutil.which("python3")} "$@"\n')
    venv_python.chmod(0o755)

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    socket_path = tmp_path / "agentman.sock"
    (config_home / "agentman" / CONFIG_FILENAME).write_text(f"database_url: {database_url}\nsocket: {socket_path}\n")
    state_home = tmp_path / "state"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(workspace)

    mock = MockOpenAI(content=json.dumps({"summary": "looks good"}))
    with serve_openai_mock(mock) as mock_port:
        model = build_tcp_model(port=mock_port)
        monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
        config = Config(
            agent=Value("reviewer", "cli"),
            model=Value("mock:model", "cli"),
            database_url=Value(database_url, "cli"),
            socket=Value(str(socket_path), "cli"),
            router_url=Value(None, "cli"),
        )
        server = uvicorn.Server(uvicorn.Config(build_app(config), uds=str(socket_path), log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(0.025)
        assert server.started, "the daemon did not start"
        try:
            result = CliRunner().invoke(app, ["pipe", "review this"], input="+added line\n-removed line\n")
            assert result.exit_code == 0, result.output
        finally:
            server.should_exit = True
            thread.join(timeout=10)

    assert mock.request is not None
    sent = json.dumps(mock.request)
    assert "review this" in sent
    assert "+added line" in sent


def test_pipe_each_creates_independent_runs_with_a_client_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database_url: str
) -> None:
    import agentman.runtime as runtime

    workspace = tmp_path / "fanout-workspace"
    (workspace / ".agentman").mkdir(parents=True)
    (workspace / ".agentman" / CONFIG_FILENAME).write_text("agent: reviewer\nmodel: mock:model\n")
    (workspace / ".agentman" / "reviewer.yaml").write_text("instructions: answer briefly\n")

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    socket_path = tmp_path / "agentman.sock"
    (config_home / "agentman" / CONFIG_FILENAME).write_text(f"database_url: {database_url}\nsocket: {socket_path}\n")
    state_home = tmp_path / "state"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(workspace)

    mock = MockOpenAI(
        delay=0.15,
        chunk_delay=0.05,
        content_for=lambda request: "reply:" + _request_prompt(request).replace("\n", "|"),
    )
    with serve_openai_mock(mock) as mock_port:
        model = build_tcp_model(port=mock_port)
        monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
        config = Config(
            agent=Value("reviewer", "cli"),
            model=Value("mock:model", "cli"),
            database_url=Value(database_url, "cli"),
            socket=Value(str(socket_path), "cli"),
            router_url=Value(None, "cli"),
        )
        server = uvicorn.Server(uvicorn.Config(build_app(config), uds=str(socket_path), log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(0.025)
        assert server.started, "the daemon did not start"
        try:
            text_result = CliRunner().invoke(
                app,
                ["pipe", "--each", "-j", "2", "review"],
                input="alpha\n\nbeta\ngamma\n",
            )
            json_result = CliRunner().invoke(
                app,
                ["pipe", "--each", "-j", "2", "--json"],
                input="epsilon\nzeta\n",
            )
            assert text_result.exit_code == 0, text_result.output
            assert json_result.exit_code == 0, json_result.output
            assert text_result.output.splitlines() == [
                "[1]",
                "reply:review||alpha",
                "[2]",
                "reply:review||",
                "[3]",
                "reply:review||beta",
                "[4]",
                "reply:review||gamma",
            ]
            assert json_result.output.splitlines() == ['"reply:epsilon"', '"reply:zeta"']
        finally:
            server.should_exit = True
            thread.join(timeout=10)

    async def read_runs() -> tuple[list[Thread], list[Run]]:
        engine = create_engine(database_url)
        store = Store(engine)
        threads = await store.list_threads(workspace=str(workspace.resolve()))
        runs = await store.list_runs(thread_id=threads[0].id)
        await engine.dispose()
        return threads, runs

    threads, runs = asyncio.run(read_runs())
    assert len(threads) == 1
    assert len(runs) == 6
    assert len({run.id for run in runs}) == 6
    assert {run.thread_id for run in runs} == {threads[0].id}
    assert {run.status for run in runs} == {"completed"}
    assert {run.prompt for run in runs} == {
        "review\n\nalpha",
        "review\n\n",
        "review\n\nbeta",
        "review\n\ngamma",
        "epsilon",
        "zeta",
    }
    assert mock.max_active_calls == 2


def test_pipe_each_validates_each_typed_result_and_settles_other_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database_url: str
) -> None:
    import agentman.runtime as runtime

    workspace = tmp_path / "typed-fanout-workspace"
    (workspace / ".agentman").mkdir(parents=True)
    (workspace / ".agentman" / CONFIG_FILENAME).write_text("agent: reviewer\nmodel: mock:model\n")
    (workspace / ".agentman" / "reviewer.yaml").write_text("instructions: answer briefly\noutput: models:Review\n")
    (workspace / ".agentman" / "models.py").write_text(MODELS)
    venv_python = workspace / ".venv/bin/python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text(f'#!/bin/sh\nexec {shutil.which("python3")} "$@"\n')
    venv_python.chmod(0o755)

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    socket_path = tmp_path / "agentman.sock"
    (config_home / "agentman" / CONFIG_FILENAME).write_text(f"database_url: {database_url}\nsocket: {socket_path}\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(workspace)

    def response_for(request: dict[str, object]) -> str:
        prompt = _request_prompt(request)
        return json.dumps({"wrong": "invalid"}) if prompt == "bad" else json.dumps({"summary": f"ok:{prompt}"})

    mock = MockOpenAI(content_for=response_for, delay=0.05, chunk_delay=0.02)
    with serve_openai_mock(mock) as mock_port:
        model = build_tcp_model(port=mock_port)
        monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
        config = Config(
            agent=Value("reviewer", "cli"),
            model=Value("mock:model", "cli"),
            database_url=Value(database_url, "cli"),
            socket=Value(str(socket_path), "cli"),
            router_url=Value(None, "cli"),
        )
        server = uvicorn.Server(uvicorn.Config(build_app(config), uds=str(socket_path), log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(0.025)
        assert server.started, "the daemon did not start"
        try:
            result = CliRunner().invoke(app, ["pipe", "--each", "-j", "2", "--json"], input="good\nbad\ngood-2\n")
            assert result.exit_code == 1, result.output
            assert [json.loads(line) for line in result.output.splitlines() if line.startswith("{")] == [
                {"summary": "ok:good"},
                {"summary": "ok:good-2"},
            ]
            assert "input 2: output failed validation" in result.output
        finally:
            server.should_exit = True
            thread.join(timeout=10)

    async def read_runs() -> tuple[list[Thread], list[Run]]:
        engine = create_engine(database_url)
        store = Store(engine)
        threads = await store.list_threads(workspace=str(workspace.resolve()))
        runs = await store.list_runs(thread_id=threads[0].id)
        await engine.dispose()
        return threads, runs

    threads, runs = asyncio.run(read_runs())
    assert len(threads) == 1
    assert len(runs) == 3
    assert {run.thread_id for run in runs} == {threads[0].id}
    assert {run.status for run in runs} == {"completed"}
    assert mock.max_active_calls == 2


def test_pipe_each_concurrent_turns_share_settled_thread_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database_url: str
) -> None:
    """A settled prior turn is visible to every concurrent fan-out request."""
    import agentman.runtime as runtime

    workspace = tmp_path / "history-fanout-workspace"
    (workspace / ".agentman").mkdir(parents=True)
    (workspace / ".agentman" / CONFIG_FILENAME).write_text("agent: reviewer\nmodel: mock:model\n")
    (workspace / ".agentman" / "reviewer.yaml").write_text("instructions: answer briefly\n")

    config_home = tmp_path / "config"
    (config_home / "agentman").mkdir(parents=True)
    socket_path = tmp_path / "agentman.sock"
    (config_home / "agentman" / CONFIG_FILENAME).write_text(f"database_url: {database_url}\nsocket: {socket_path}\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))
    monkeypatch.chdir(workspace)

    mock = MockOpenAI(content="reply", delay=0.08, chunk_delay=0.02)
    with serve_openai_mock(mock) as mock_port:
        model = build_tcp_model(port=mock_port)
        monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
        config = Config(
            agent=Value("reviewer", "cli"),
            model=Value("mock:model", "cli"),
            database_url=Value(database_url, "cli"),
            socket=Value(str(socket_path), "cli"),
            router_url=Value(None, "cli"),
        )
        server = uvicorn.Server(uvicorn.Config(build_app(config), uds=str(socket_path), log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(0.025)
        assert server.started, "the daemon did not start"
        try:
            seed = CliRunner().invoke(app, ["pipe", "--json"], input="seed\n")
            fanout = CliRunner().invoke(app, ["pipe", "--each", "-j", "2", "--json"], input="alpha\nbeta\n")
            assert seed.exit_code == 0, seed.output
            assert fanout.exit_code == 0, fanout.output
        finally:
            server.should_exit = True
            thread.join(timeout=10)

    fanout_requests = mock.requests[-2:]
    assert {_request_prompt(request) for request in fanout_requests} == {"alpha", "beta"}
    for request in fanout_requests:
        user_prompts = [message["content"] for message in request["messages"] if message.get("role") == "user"]
        assert any(prompt.startswith("seed") for prompt in user_prompts)

    async def read_runs() -> tuple[list[Thread], list[Run]]:
        engine = create_engine(database_url)
        store = Store(engine)
        threads = await store.list_threads(workspace=str(workspace.resolve()))
        runs = await store.list_runs(thread_id=threads[0].id)
        await engine.dispose()
        return threads, runs

    threads, runs = asyncio.run(read_runs())
    assert len(threads) == 1
    assert len(runs) == 3
    assert {run.thread_id for run in runs} == {threads[0].id}
