"""Focused daemon tests for resume, model, and stream boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dbos._error import DBOSAwaitedWorkflowCancelledError
from inferference import ConstraintKind, ModelInfo
from pydantic import ValidationError
from pydantic_ai_harness.step_persistence import ToolEffectRecord
from pydantic_ai_harness.step_persistence.recovery import RecoveryInspection

import agentman.daemon as daemon
from agentman.config import Config, Value
from agentman.daemon import AgentmanApp, _HttpError
from agentman.persistence.store import RUN_TERMINAL, Run
from agentman.protocol import TurnRequest


def _app() -> AgentmanApp:
    return AgentmanApp(
        Config(
            agent=Value(None, "default"),
            model=Value(None, "default"),
            database_url=Value("postgresql://unused", "test"),
            socket=Value("/tmp/agentman-test.sock", "test"),
            router_url=Value("http://router.test/v1", "test"),
        )
    )


async def _wait_forever_receive() -> dict[str, Any]:
    await asyncio.Future()
    raise AssertionError("the receive task should be cancelled")


def _run(
    workspace: str,
    status: str = "running",
    error: dict[str, Any] | None = None,
) -> Run:
    now = datetime.now(UTC)
    return Run(
        id="run-1",
        thread_id="thread-1",
        workspace=workspace,
        agent="reviewer",
        model="mock:model",
        status=status,
        prompt="review",
        output_json=None,
        output_type=None,
        messages_json=None,
        usage=None,
        created_at=now,
        updated_at=now,
        error=error,
    )


class _Store:
    def __init__(self, run: Run | None = None) -> None:
        self.run = run

    async def get_run(self, run_id: str) -> Run | None:
        return self.run if self.run is not None and run_id == self.run.id else None

    async def get_thread(self, thread_id: str) -> object:
        return SimpleNamespace(id=thread_id)


class _CancelStore(_Store):
    def __init__(self, run: Run) -> None:
        super().__init__(run)
        self.updates: list[dict[str, Any]] = []

    async def get_run(self, run_id: str) -> Run | None:
        return self.run if self.run is not None and self.run.id == run_id else None

    async def update_run(self, run_id: str, **fields: Any) -> None:
        assert self.run is not None and self.run.id == run_id
        self.updates.append(dict(fields))
        self.run = replace(self.run, **fields)

    async def finish_run(self, run_id: str, **fields: Any) -> bool:
        assert self.run is not None and self.run.id == run_id
        assert fields["status"] in RUN_TERMINAL
        if self.run.status != "running":
            return False
        self.updates.append(dict(fields))
        self.run = replace(self.run, **fields)
        return True


def _dbos(
    *,
    workflow_status: str | None = "CANCELLED",
    cancelled: list[str] | None = None,
    resumed: list[str] | None = None,
    handle: Any = None,
    cancel_error: Exception | None = None,
) -> type:
    """Build a stand-in for the ``DBOS`` class that records its calls."""

    class _DBOS:
        @staticmethod
        async def cancel_workflow_async(run_id: str) -> None:
            if cancelled is not None:
                cancelled.append(run_id)
            if cancel_error is not None:
                raise cancel_error

        @staticmethod
        async def get_workflow_status_async(run_id: str) -> SimpleNamespace | None:
            return None if workflow_status is None else SimpleNamespace(status=workflow_status)

        @staticmethod
        async def resume_workflow_async(run_id: str) -> Any:
            if resumed is not None:
                resumed.append(run_id)
            return handle

    return _DBOS


class _EmptyStepStore:
    async def latest_snapshot(self, *, run_id: str, include_interrupted: bool) -> None:
        return None


class _Journal:
    def __init__(self, effects: tuple[ToolEffectRecord, ...] = ()) -> None:
        self.effects = effects

    async def recovery(self, *, run_id: str) -> RecoveryInspection:
        return RecoveryInspection(
            latest=None,
            settled=None,
            unresolved=self.effects,
            completed_tools=(),
            failed_tools=(),
        )


def _effect(summary: str | None) -> ToolEffectRecord:
    return ToolEffectRecord(
        run_id="run-1",
        tool_call_id="call-1",
        tool_name="write_file",
        status="started",
        effect_summary=summary,
    )


@pytest.mark.parametrize(
    ("run", "workspace", "effects", "expected"),
    [
        (None, "/repo", (), "not an agentman run"),
        (_run("/repo", "completed"), "/repo", (), "already terminal"),
        (_run("/missing"), "/missing", (), "workspace does not exist"),
        (_run("/repo"), "/repo", (_effect(None),), "unknown_after_crash"),
        (_run("/stored"), "/requested", (), "different workspace"),
    ],
)
def test_resume_api_refusals_use_http_409(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    run: Run | None,
    workspace: str,
    effects: tuple[ToolEffectRecord, ...],
    expected: str,
) -> None:
    if expected == "workspace does not exist":
        run = _run(str(tmp_path / "gone"))
        workspace = run.workspace
    if expected == "unknown_after_crash":
        run = _run(str(tmp_path))
        workspace = run.workspace
    if run is None:
        store = _Store()
    elif expected == "different workspace":
        store = _Store(run)
    else:
        store = _Store(run)

    app = _app()
    app.store = cast(Any, store)
    app.journal = cast(Any, _Journal(effects))
    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status="PENDING"))

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("a refused resume must not send a success response")

        payload = json.dumps({"workspace": workspace, "run_id": "run-1"}).encode()
        with pytest.raises(_HttpError, match=expected) as error:
            await app._handle_resume(payload, send)
        assert error.value.status == 409
        if expected == "different workspace":
            assert "/stored" not in error.value.message
            assert "/requested" not in error.value.message

    asyncio.run(exercise())


def test_resume_api_reconciles_file_evidence_before_dbos(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = tmp_path / "file.txt"
    target.write_text("after")
    summary = json.dumps(
        {
            "path": str(target),
            "pre_image": hashlib.sha256(b"before").hexdigest(),
            "post_image": hashlib.sha256(b"after").hexdigest(),
        }
    )
    app = _app()
    app.store = cast(Any, _Store(_run(str(tmp_path))))
    app.journal = cast(Any, _Journal((_effect(summary),)))
    resumed: list[str] = []

    class _Handle:
        async def get_result(self) -> None:
            return None

    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status="PENDING", resumed=resumed, handle=_Handle()))

    async def exercise() -> list[dict[str, Any]]:
        sent: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._handle_resume(
            json.dumps({"workspace": str(tmp_path), "run_id": "run-1"}).encode(),
            send,
        )
        return sent

    sent = asyncio.run(exercise())
    assert resumed == ["run-1"]
    assert sent[0]["status"] == 200
    assert json.loads(sent[1]["body"]) == {"run_id": "run-1"}


class _Capabilities:
    def __init__(self, *, models: tuple[str, ...] = ("resident",), schema: bool = True) -> None:
        self.models = {name: ModelInfo(name=name, n_ctx=8192) for name in models}
        self.schema = schema

    def supports(self, kind: ConstraintKind) -> bool:
        return self.schema and kind == ConstraintKind.SCHEMA

    def resolve_model_field(self, reference: str) -> str:
        if reference == "alias":
            return "resident"
        if reference in self.models:
            return reference
        raise ValueError(f"unknown model {reference!r}")

    def model(self, name: str) -> ModelInfo | None:
        return self.models.get(name)


def _turn_request(
    *,
    model: str = "inferference:alias",
    schema: dict[str, Any] | None = None,
    max_transport_retries: int | None = None,
) -> TurnRequest:
    return TurnRequest(
        workspace="/repo",
        thread_id="thread-1",
        model=model,
        prompt="hello",
        output_schema=schema,
        max_transport_retries=max_transport_retries,
    )


def test_turn_request_rejects_negative_transport_retries() -> None:
    with pytest.raises(ValidationError):
        _turn_request(max_transport_retries=-1)


def test_daemon_resolves_model_alias_and_context_window(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    capabilities = _Capabilities()

    async def get_capabilities() -> _Capabilities:
        return capabilities

    monkeypatch.setattr(app, "_capabilities", get_capabilities)
    model, router_url, n_ctx = asyncio.run(app._resolve_turn_model(_turn_request(schema={"type": "object"})))

    assert model == "inferference:resident"
    assert router_url == "http://router.test/v1"
    assert n_ctx == 8192


def test_unsupported_transport_retry_limit_fails_before_workflow_or_capability_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app()
    app.store = cast(Any, _Store())
    started: list[object] = []

    async def fail_capabilities() -> object:
        raise AssertionError("an unsupported provider must fail before model resolution")

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*args: object) -> object:
            started.append(args)
            raise AssertionError("an unsupported provider must fail before a workflow starts")

    monkeypatch.setattr(app, "_capabilities", fail_capabilities)
    monkeypatch.setattr(daemon, "DBOS", _DBOS)

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("the rejected request must not start an SSE response")

        request = _turn_request(model="anthropic:claude", max_transport_retries=0)
        with pytest.raises(_HttpError, match="cannot enforce `max_transport_retries`") as error:
            await app._handle_turn(request.model_dump_json().encode(), _wait_forever_receive, send)
        assert error.value.status == 422

    asyncio.run(exercise())
    assert started == []


@pytest.mark.parametrize(
    ("turn_request", "capabilities", "expected"),
    [
        (_turn_request(model="inferference:missing"), _Capabilities(), "unknown model"),
        (
            _turn_request(schema={"type": "object"}),
            _Capabilities(schema=False),
            "SCHEMA constraint",
        ),
    ],
)
def test_daemon_rejects_unknown_models_and_unenforced_typed_output(
    turn_request: TurnRequest,
    capabilities: _Capabilities,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app()

    async def get_capabilities() -> _Capabilities:
        return capabilities

    monkeypatch.setattr(app, "_capabilities", get_capabilities)
    with pytest.raises(_HttpError, match=expected) as error:
        asyncio.run(app._resolve_turn_model(turn_request))
    assert error.value.status == 422


def test_default_string_inferference_turn_does_not_require_schema_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _app()

    async def get_capabilities() -> _Capabilities:
        return _Capabilities(schema=False)

    monkeypatch.setattr(app, "_capabilities", get_capabilities)
    resolved = asyncio.run(app._resolve_turn_model(_turn_request(schema=None)))
    assert resolved == ("inferference:resident", "http://router.test/v1", 8192)


def test_capabilities_resolve_router_for_a_repository_model(monkeypatch: pytest.MonkeyPatch) -> None:
    import inferference

    app = _app()
    app.router_url = None
    clients: list[str] = []

    class _Client:
        def __init__(self, url: str) -> None:
            clients.append(url)

        async def capabilities(self) -> _Capabilities:
            return _Capabilities()

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(inferference, "resolve_router_url", lambda: "http://resolved.test/v1")
    monkeypatch.setattr(daemon, "InferferenceClient", _Client)
    asyncio.run(app._capabilities())
    assert app.router_url == "http://resolved.test/v1"
    assert clients == ["http://resolved.test/v1"]


def test_failed_workflow_emits_terminal_error_without_exception_details(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.store = cast(Any, _Store())

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            raise RuntimeError("private model response")

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._handle_turn(
            _turn_request(model="mock:model").model_dump_json().encode(), _wait_forever_receive, send
        )

    asyncio.run(exercise())
    frames = [message["body"].decode() for message in sent if message.get("more_body")]
    assert len(frames) == 1
    assert "event: turn_error" in frames[0]
    assert '"final":true' in frames[0]
    assert "inspect the durable run record" in frames[0]
    assert "private model response" not in frames[0]


def test_turn_handler_cancels_the_run_on_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    cancelled: list[str] = []
    cancellation_requested = asyncio.Event()

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            await cancellation_requested.wait()
            raise DBOSAwaitedWorkflowCancelledError("run-1")

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            return _Handle()

        @staticmethod
        async def cancel_workflow_async(run_id: str) -> None:
            cancelled.append(run_id)
            cancellation_requested.set()

        @staticmethod
        async def get_workflow_status_async(run_id: str) -> SimpleNamespace:
            return SimpleNamespace(status="CANCELLED")

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []
    disconnected = False

    async def exercise() -> None:
        nonlocal disconnected
        request = _turn_request(model="mock:model")
        messages = iter(
            (
                {"type": "http.request", "body": request.model_dump_json().encode(), "more_body": False},
                {"type": "http.disconnect"},
            )
        )

        async def receive() -> dict[str, Any]:
            nonlocal disconnected
            message = next(messages)
            if message["type"] == "http.disconnect":
                disconnected = True
            return message

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._http({"method": "POST", "path": "/v1/turns", "query_string": b""}, receive, send)

    asyncio.run(exercise())
    assert cancelled == ["run-1"]
    assert disconnected
    assert store.run.status == "cancelled"
    assert store.run.error == {"reason": "client_disconnected", "type": None, "message": None}
    assert store.updates[0]["usage"]["requests"] == 0
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert app.registry._queues == {}


def test_turn_handler_sends_turn_error_with_the_cancel_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.store = cast(
        Any,
        _CancelStore(
            _run(
                workspace="/repo",
                status="cancelled",
                error={"reason": "cancel_requested", "type": None, "message": None},
            )
        ),
    )

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            raise DBOSAwaitedWorkflowCancelledError("run-1")

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._handle_turn(
            _turn_request(model="mock:model").model_dump_json().encode(), _wait_forever_receive, send
        )

    asyncio.run(exercise())
    frames = [message["body"].decode() for message in sent if message.get("more_body")]
    assert len(frames) == 1
    assert "event: turn_error" in frames[0]
    assert '"error":"turn cancelled"' in frames[0]
    assert '"reason":"cancel_requested"' in frames[0]
    assert '"run_id":"run-1"' in frames[0]
    assert '"final":true' in frames[0]


def test_health_lists_cancel_capability() -> None:
    app = _app()
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._route("GET", "/v1/health", {}, b"", _wait_forever_receive, send)

    asyncio.run(exercise())
    payload = json.loads(sent[-1]["body"])
    assert payload["capabilities"] == ["cancel"]


@pytest.mark.parametrize(
    ("run_id", "workspace", "expected_status", "expected_message"),
    [
        ("missing", "/repo", 404, "no run"),
        ("run-1", "/other", 409, "different workspace"),
    ],
)
def test_cancel_route_refuses_unknown_runs_and_other_workspaces(
    run_id: str,
    workspace: str,
    expected_status: int,
    expected_message: str,
) -> None:
    app = _app()
    app.store = cast(Any, _CancelStore(_run("/repo")))

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("a refused cancel must not send a response")

        with pytest.raises(_HttpError, match=expected_message) as error:
            await app._route(
                "POST",
                f"/v1/runs/{run_id}/cancel",
                {},
                json.dumps({"workspace": workspace}).encode(),
                _wait_forever_receive,
                send,
            )
        assert error.value.status == expected_status

    asyncio.run(exercise())


def test_cancel_route_returns_false_for_a_terminal_run() -> None:
    app = _app()
    app.store = cast(Any, _CancelStore(_run("/repo", status="completed")))
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._route(
            "POST",
            "/v1/runs/run-1/cancel",
            {},
            b'{"workspace":"/repo"}',
            _wait_forever_receive,
            send,
        )

    asyncio.run(exercise())
    assert json.loads(sent[-1]["body"]) == {"run_id": "run-1", "status": "completed", "cancelled": False}


def test_cancel_route_marks_a_running_run_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    cancelled: list[str] = []

    class _DBOS:
        @staticmethod
        async def cancel_workflow_async(run_id: str) -> None:
            cancelled.append(run_id)

        @staticmethod
        async def get_workflow_status_async(run_id: str) -> SimpleNamespace:
            return SimpleNamespace(status="CANCELLED")

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._route(
            "POST",
            "/v1/runs/run-1/cancel",
            {},
            b'{"workspace":"/repo"}',
            _wait_forever_receive,
            send,
        )

    asyncio.run(exercise())
    assert cancelled == ["run-1"]
    assert store.run.status == "cancelled"
    assert store.run.error is not None and store.run.error["reason"] == "cancel_requested"
    assert json.loads(sent[-1]["body"]) == {"run_id": "run-1", "status": "cancelled", "cancelled": True}


# -- disconnect, final-status races, and resume after a cancel -----------------


class _PendingHandle:
    """A workflow handle whose result ends when the cancel arrives."""

    def __init__(self, outcome: BaseException | None = None) -> None:
        self.cancelled = asyncio.Event()
        self.outcome = outcome if outcome is not None else DBOSAwaitedWorkflowCancelledError("run-1")

    def get_workflow_id(self) -> str:
        return "run-1"

    async def get_result(self) -> object:
        await self.cancelled.wait()
        raise self.outcome


def _disconnect_turn(
    monkeypatch: pytest.MonkeyPatch,
    app: AgentmanApp,
    *,
    handle: _PendingHandle,
    cancel_error: Exception | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Run a turn whose client disconnects at once. Return the DBOS cancels and the sent messages."""
    cancelled: list[str] = []
    base = _dbos(cancelled=cancelled, cancel_error=cancel_error)

    class _DBOS(base):  # type: ignore[valid-type, misc]
        @staticmethod
        async def start_workflow_async(*_: object) -> _PendingHandle:
            return handle

        @staticmethod
        async def cancel_workflow_async(run_id: str) -> None:
            await base.cancel_workflow_async(run_id)
            handle.cancelled.set()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def receive() -> dict[str, Any]:
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        # A raise here means the handler leaked an error after the response started.
        await app._handle_turn(_turn_request(model="mock:model").model_dump_json().encode(), receive, send)

    asyncio.run(exercise())
    return cancelled, sent


def test_disconnect_before_the_run_row_exists_still_cancels_the_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.store = cast(Any, _Store())
    cancelled, sent = _disconnect_turn(monkeypatch, app, handle=_PendingHandle())
    assert cancelled == ["run-1"]
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert app.registry._queues == {}


def test_disconnect_survives_a_store_error_and_still_cancels_the_workflow(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class _BrokenStore(_Store):
        async def get_run(self, run_id: str) -> Run | None:
            raise RuntimeError("the database is down")

    app = _app()
    app.store = cast(Any, _BrokenStore())
    cancelled, sent = _disconnect_turn(monkeypatch, app, handle=_PendingHandle())
    assert cancelled == ["run-1"]
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert "the database is down" in caplog.text


def test_disconnect_survives_a_dbos_cancel_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app()
    app.store = cast(Any, _CancelStore(_run("/repo")))
    cancelled, sent = _disconnect_turn(
        monkeypatch, app, handle=_PendingHandle(), cancel_error=RuntimeError("DBOS is down")
    )
    assert cancelled == ["run-1"]
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert "DBOS is down" in caplog.text


def test_disconnect_logs_an_unexpected_workflow_error_and_does_not_raise(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    app = _app()
    app.store = cast(Any, _Store())
    _, sent = _disconnect_turn(monkeypatch, app, handle=_PendingHandle(RuntimeError("workflow exploded")))
    assert [message["type"] for message in sent] == ["http.response.start"]
    assert "workflow exploded" in caplog.text


class _RacingStore(_CancelStore):
    """A store where the workflow unwind writes the final status first."""

    def __init__(self, run: Run, *, rival_status: str, rival_reason: str | None = None) -> None:
        super().__init__(run)
        self.rival_status = rival_status
        self.rival_reason = rival_reason

    async def finish_run(self, run_id: str, **fields: Any) -> bool:
        assert self.run is not None
        error = {"reason": self.rival_reason, "type": None, "message": None} if self.rival_reason else None
        self.run = replace(self.run, status=self.rival_status, error=error)
        return False


async def _cancel(app: AgentmanApp, *, reason: str = "cancel_requested") -> dict[str, Any]:
    return await app.cancel_run("run-1", workspace="/repo", reason=reason)


def test_cancel_reports_cancelled_when_the_unwind_wrote_the_status_first(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _RacingStore(_run("/repo"), rival_status="cancelled", rival_reason="workflow_cancelled")
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))

    result = asyncio.run(_cancel(app))
    assert result == {"run_id": "run-1", "status": "cancelled", "cancelled": True}
    assert store.run is not None and store.run.error is not None
    assert store.run.error["reason"] == "workflow_cancelled", "the first writer keeps its reason"


@pytest.mark.parametrize("rival_status", ["completed", "failed"])
def test_cancel_reports_false_when_the_run_ended_another_way_first(
    monkeypatch: pytest.MonkeyPatch, rival_status: str
) -> None:
    app = _app()
    store = _RacingStore(_run("/repo"), rival_status=rival_status)
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))

    result = asyncio.run(_cancel(app))
    assert result == {"run_id": "run-1", "status": rival_status, "cancelled": False}


def test_cancel_never_rewrites_a_completed_run(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _CancelStore(_run("/repo", status="completed"))
    app.store = cast(Any, store)
    cancelled: list[str] = []
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=cancelled))

    result = asyncio.run(_cancel(app))
    assert result == {"run_id": "run-1", "status": "completed", "cancelled": False}
    assert cancelled == [] and store.updates == []


def test_concurrent_cancels_keep_the_reason_of_the_first_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    store = _CancelStore(_run("/repo"))
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    monkeypatch.setattr(daemon, "DBOS", _dbos(cancelled=[]))

    async def exercise() -> list[dict[str, Any]]:
        return list(
            await asyncio.gather(
                _cancel(app, reason="cancel_requested"),
                _cancel(app, reason="client_disconnected"),
            )
        )

    results = asyncio.run(exercise())
    assert [result["status"] for result in results] == ["cancelled", "cancelled"]
    assert len(store.updates) == 1, "only the first writer changes the row"
    assert store.run is not None and store.run.error is not None
    assert store.run.error["reason"] == "cancel_requested"


def test_stream_sends_turn_result_when_a_cancel_met_a_completed_run(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    completed = replace(
        _run("/repo", status="completed"),
        output_json={"title": "done"},
        output_type="models:Summary",
        messages_json=[{"kind": "request"}],
        usage={"requests": 1},
        run_metadata={"logical_turn_count": 1},
    )
    app.store = cast(Any, _CancelStore(completed))

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            raise DBOSAwaitedWorkflowCancelledError("run-1")

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)
    sent: list[dict[str, Any]] = []

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app._handle_turn(
            _turn_request(model="mock:model").model_dump_json().encode(), _wait_forever_receive, send
        )

    asyncio.run(exercise())
    frames = [message["body"].decode() for message in sent if message.get("more_body")]
    assert len(frames) == 1
    assert "event: turn_result" in frames[0]
    assert "turn_error" not in frames[0]
    payload = json.loads(frames[0].split("data: ", 1)[1])
    assert payload["final"] is True
    assert payload["event"]["output"] == {"title": "done"}
    assert payload["event"]["usage"] == {"requests": 1}
    assert payload["event"]["thread_id"] == "thread-1"


def test_resume_of_a_run_cancelled_during_the_resume_returns_409(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    app = _app()
    app.store = cast(Any, _Store(_run(str(tmp_path))))
    app.journal = cast(Any, _Journal())

    class _Handle:
        async def get_result(self) -> None:
            raise DBOSAwaitedWorkflowCancelledError("run-1")

    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status="PENDING", resumed=[], handle=_Handle()))

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("a cancelled resume must not send a success response")

        payload = json.dumps({"workspace": str(tmp_path), "run_id": "run-1"}).encode()
        with pytest.raises(_HttpError, match="was cancelled") as error:
            await app._handle_resume(payload, send)
        assert error.value.status == 409

    asyncio.run(exercise())


def test_resume_refuses_a_run_that_dbos_cancelled_and_repairs_the_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A crash between the DBOS cancel and the row write leaves ``running`` on the row."""
    app = _app()
    store = _CancelStore(_run(str(tmp_path)))
    app.store = cast(Any, store)
    app.step_store = cast(Any, _EmptyStepStore())
    app.journal = cast(Any, _Journal())
    resumed: list[str] = []
    monkeypatch.setattr(daemon, "DBOS", _dbos(workflow_status="CANCELLED", resumed=resumed))

    async def exercise() -> None:
        async def send(_: Any) -> None:
            raise AssertionError("a refused resume must not send a success response")

        payload = json.dumps({"workspace": str(tmp_path), "run_id": "run-1"}).encode()
        with pytest.raises(_HttpError, match="workflow was cancelled") as error:
            await app._handle_resume(payload, send)
        assert error.value.status == 409

    asyncio.run(exercise())
    assert resumed == []
    assert store.run is not None and store.run.status == "cancelled"
    assert store.run.error is not None and store.run.error["reason"] == "workflow_cancelled"
