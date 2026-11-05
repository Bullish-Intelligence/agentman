"""Focused daemon tests for resume, model, and stream boundaries."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from inferference import ConstraintKind, ModelInfo
from pydantic import ValidationError
from pydantic_ai_harness.step_persistence import ToolEffectRecord
from pydantic_ai_harness.step_persistence.recovery import RecoveryInspection

import agentman.daemon as daemon
from agentman.config import Config, Value
from agentman.daemon import AgentmanApp, _HttpError
from agentman.persistence.store import Run
from agentman.protocol import StreamEnvelope, TurnRequest


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


def _run(workspace: str, status: str = "running") -> Run:
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
    )


class _Store:
    def __init__(self, run: Run | None = None) -> None:
        self.run = run

    async def get_run(self, run_id: str) -> Run | None:
        return self.run if self.run is not None and run_id == self.run.id else None

    async def get_thread(self, thread_id: str) -> object:
        return SimpleNamespace(id=thread_id)


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

    class _DBOS:
        @staticmethod
        async def resume_workflow_async(run_id: str) -> _Handle:
            resumed.append(run_id)
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)

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
            await app._handle_turn(request.model_dump_json().encode(), send)
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

        await app._handle_turn(_turn_request(model="mock:model").model_dump_json().encode(), send)

    asyncio.run(exercise())
    frames = [message["body"].decode() for message in sent if message.get("more_body")]
    assert len(frames) == 1
    assert "event: turn_error" in frames[0]
    assert '"final":true' in frames[0]
    assert "inspect the durable run record" in frames[0]
    assert "private model response" not in frames[0]


def test_disconnect_removes_subscriber_without_canceling_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _app()
    app.store = cast(Any, _Store())
    completed = False
    workflow_tasks: list[asyncio.Task[None]] = []

    class _Handle:
        def get_workflow_id(self) -> str:
            return "run-1"

        async def get_result(self) -> object:
            await workflow_tasks[0]
            return SimpleNamespace(
                run_id="run-1",
                thread_id="thread-1",
                output="done",
                output_type="str",
                messages=[],
                usage=None,
                run_metadata={},
            )

    class _DBOS:
        @staticmethod
        async def start_workflow_async(*_: object) -> _Handle:
            async def durable_workflow() -> None:
                nonlocal completed
                await asyncio.sleep(0.02)
                app.registry.publish(
                    "run-1",
                    StreamEnvelope(
                        run_id="run-1",
                        request_ordinal=1,
                        event_ordinal=0,
                        kind="part_delta",
                        event={"event_kind": "part_delta"},
                    ),
                )
                await asyncio.sleep(0.03)
                completed = True

            workflow_tasks.append(asyncio.create_task(durable_workflow()))
            return _Handle()

    monkeypatch.setattr(daemon, "DBOS", _DBOS)

    async def exercise() -> None:
        async def send(message: dict[str, Any]) -> None:
            if message.get("more_body"):
                raise OSError("client disconnected")

        with pytest.raises(OSError, match="client disconnected"):
            await app._handle_turn(_turn_request(model="mock:model").model_dump_json().encode(), send)
        await workflow_tasks[0]

    asyncio.run(exercise())
    assert completed
    assert app.registry._queues == {}
