"""Phase C: run_turn wiring — typed output, the journal, and history reuse."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from pydantic_ai_harness.filesystem import FileSystem
from pydantic_ai_harness.step_persistence import InMemoryStepStore
from support.fake_store import FakeProductStore
from support.openai_transport import MockOpenAI, build_mock_model, serve_openai_mock

from agentman.runtime import (
    Turn,
    _agent_cache_key,
    _durability_name,
    agent_for_turn,
    load_history,
    reset_agent_cache,
    run_turn,
    usage_so_far,
)

SCHEMA = {
    "type": "object",
    "properties": {"title": {"type": "string"}, "rating": {"type": "integer"}},
    "required": ["title", "rating"],
}


def _turn(thread_id: str, *, prompt: str = "summarise this") -> Turn:
    return Turn(
        workspace="/repo",
        thread_id=thread_id,
        agent_spec={"instructions": "be brief"},
        agent_name="summary",
        model="inferference:gemma",
        prompt=prompt,
        output_schema=SCHEMA,
        output_type="models:Summary",
    )


def _chat_requests(mock: MockOpenAI) -> list[dict[str, Any]]:
    return [
        request
        for path, request in zip(mock.request_paths, mock.requests, strict=True)
        if path.endswith("/chat/completions")
    ]


def test_agent_cache_key_names_workspace_and_tools_inputs() -> None:
    base = _turn("thread")
    key_kwargs = {"step_store": InMemoryStepStore(), "event_stream_handler": None}

    workspace_key = _agent_cache_key(base, **key_kwargs)
    assert workspace_key != _agent_cache_key(replace(base, workspace="/other"), **key_kwargs)
    assert workspace_key != _agent_cache_key(replace(base, tools="coder"), **key_kwargs)
    assert _durability_name(base) != _durability_name(replace(base, workspace="/other"))
    assert workspace_key != _agent_cache_key(replace(base, max_transport_retries=0), **key_kwargs)


def test_cache_key_rejects_non_json_agent_spec() -> None:
    import pytest

    turn = replace(_turn("thread"), agent_spec={"instructions": object()})

    with pytest.raises(TypeError):
        _agent_cache_key(turn, step_store=InMemoryStepStore(), event_stream_handler=None)


def test_workspace_key_builds_coder_at_each_workspace(monkeypatch, tmp_path) -> None:
    import agentman.runtime as runtime

    monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: "test-model")
    reset_agent_cache()
    store = InMemoryStepStore()
    first = agent_for_turn(
        replace(_turn("thread"), workspace=str(tmp_path / "first"), tools="coder"),
        step_store=store,
        event_stream_handler=None,
    )
    second = agent_for_turn(
        replace(_turn("thread"), workspace=str(tmp_path / "second"), tools="coder"),
        step_store=store,
        event_stream_handler=None,
    )

    assert first is not second
    first_filesystem = next(
        capability for capability in first._root_capability.capabilities if isinstance(capability, FileSystem)
    )
    second_filesystem = next(
        capability for capability in second._root_capability.capabilities if isinstance(capability, FileSystem)
    )
    assert first_filesystem.root_dir == (tmp_path / "first").resolve()
    assert second_filesystem.root_dir == (tmp_path / "second").resolve()


def test_missing_tools_keeps_slice_one_capabilities(monkeypatch) -> None:
    import agentman.runtime as runtime

    monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: "test-model")
    reset_agent_cache()
    agent = agent_for_turn(
        _turn("thread"),
        step_store=InMemoryStepStore(),
        event_stream_handler=None,
    )

    capabilities: list[object] = []
    agent._root_capability.apply(capabilities.append)
    assert all(type(capability).__name__ != "Coder" for capability in capabilities)


async def test_typed_output_is_validated_and_recorded() -> None:
    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    result = await run_turn(
        _turn(thread.id),
        step_store=step_store,
        product_store=product,
        run_id="run-1",
        model=build_mock_model(mock),
    )

    assert result.output == {"title": "hello", "rating": 4}
    assert result.output_type == "models:Summary"


async def test_outgoing_request_carries_native_json_schema() -> None:
    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    await run_turn(
        _turn(thread.id),
        step_store=step_store,
        product_store=product,
        run_id="run-1",
        model=build_mock_model(mock),
    )

    body = mock.request
    assert body is not None
    assert body["response_format"]["type"] == "json_schema"


async def test_transport_zero_makes_one_request_and_does_not_fall_back() -> None:
    import pytest

    mock = MockOpenAI(status_for=lambda _: 503)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="zero")
    turn = replace(
        _turn(thread.id, prompt="one request"),
        agent_spec={"instructions": "be brief", "retries": 0},
        output_schema=None,
        output_type="str",
        max_transport_retries=0,
    )

    reset_agent_cache()
    with serve_openai_mock(mock) as port, pytest.raises(Exception):  # noqa: B017 - provider error type is upstream
        await run_turn(
            replace(turn, router_url=f"http://127.0.0.1:{port}"),
            step_store=step_store,
            product_store=product,
            run_id="run-zero",
        )
    reset_agent_cache()

    record = await product.get_run("run-zero")
    assert record is not None
    assert record.status == "failed"
    assert record.run_metadata == {
        "requested_max_transport_retries": 0,
        "effective_max_transport_retries": 0,
        "provider_fallback": False,
        "logical_turn_count": 1,
        "physical_model_request_count": 1,
    }
    chat_requests = _chat_requests(mock)
    assert len(chat_requests) == 1
    assert {request["model"] for request in chat_requests} == {"gemma"}


async def test_positive_transport_retry_is_separate_from_agent_retries_and_keeps_typed_output() -> None:
    attempts: dict[str, int] = {}

    def status_for(request: dict[str, Any]) -> int:
        messages = request.get("messages", [])
        prompt = messages[-1]["content"] if messages else ""
        attempts[prompt] = attempts.get(prompt, 0) + 1
        return 503 if attempts[prompt] == 1 else 200

    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}), status_for=status_for)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="retry")
    turn = replace(
        _turn(thread.id, prompt="typed proposal"),
        agent_spec={"instructions": "be brief", "retries": 0},
        tools=None,
        max_transport_retries=1,
    )

    reset_agent_cache()
    with serve_openai_mock(mock) as port:
        result = await run_turn(
            replace(turn, router_url=f"http://127.0.0.1:{port}"),
            step_store=step_store,
            product_store=product,
            run_id="run-positive",
        )
    reset_agent_cache()

    assert result.output == {"title": "hello", "rating": 4}
    chat_requests = _chat_requests(mock)
    assert len(chat_requests) == 2
    assert all(request["response_format"]["type"] == "json_schema" for request in chat_requests)
    assert result.run_metadata == {
        "requested_max_transport_retries": 1,
        "effective_max_transport_retries": 1,
        "provider_fallback": False,
        "logical_turn_count": 1,
        "physical_model_request_count": 2,
    }
    assert "api_key" not in result.run_metadata


async def test_agent_output_retry_budget_does_not_follow_transport_retry_limit() -> None:
    import pytest

    mock = MockOpenAI(content="not valid structured output")
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="validation")
    turn = replace(
        _turn(thread.id, prompt="invalid typed output"),
        agent_spec={"instructions": "be brief", "retries": 0},
        max_transport_retries=2,
    )

    reset_agent_cache()
    with serve_openai_mock(mock) as port, pytest.raises(Exception):  # noqa: B017 - validation type is upstream
        await run_turn(
            replace(turn, router_url=f"http://127.0.0.1:{port}"),
            step_store=step_store,
            product_store=product,
            run_id="run-invalid-output",
        )
    reset_agent_cache()

    record = await product.get_run("run-invalid-output")
    assert record is not None
    assert len(_chat_requests(mock)) == 1
    assert record.run_metadata["effective_max_transport_retries"] == 2
    assert record.run_metadata["physical_model_request_count"] == 1


async def test_concurrent_runs_keep_transport_limits_and_counts_separate() -> None:
    import asyncio

    attempts: dict[str, int] = {}

    def status_for(request: dict[str, Any]) -> int:
        messages = request.get("messages", [])
        prompt = messages[-1]["content"] if messages else ""
        attempts[prompt] = attempts.get(prompt, 0) + 1
        if prompt == "zero" or attempts[prompt] == 1:
            return 503
        return 200

    mock = MockOpenAI(content="done", status_for=status_for)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    zero_thread = await product.create_thread(workspace="/repo", name="zero")
    retry_thread = await product.create_thread(workspace="/repo", name="retry")
    zero_turn = replace(
        _turn(zero_thread.id, prompt="zero"),
        agent_spec={"instructions": "be brief", "retries": 0},
        output_schema=None,
        output_type="str",
        max_transport_retries=0,
    )
    retry_turn = replace(
        _turn(retry_thread.id, prompt="retry"),
        agent_spec={"instructions": "be brief", "retries": 0},
        output_schema=None,
        output_type="str",
        max_transport_retries=1,
    )

    reset_agent_cache()
    with serve_openai_mock(mock) as port:
        outcomes = await asyncio.gather(
            run_turn(
                replace(zero_turn, router_url=f"http://127.0.0.1:{port}"),
                step_store=step_store,
                product_store=product,
                run_id="run-concurrent-zero",
            ),
            run_turn(
                replace(retry_turn, router_url=f"http://127.0.0.1:{port}"),
                step_store=step_store,
                product_store=product,
                run_id="run-concurrent-retry",
            ),
            return_exceptions=True,
        )
    reset_agent_cache()

    assert isinstance(outcomes[0], Exception)
    assert not isinstance(outcomes[1], Exception)
    assert outcomes[1].output == "done"
    zero_record = await product.get_run("run-concurrent-zero")
    retry_record = await product.get_run("run-concurrent-retry")
    assert zero_record is not None and retry_record is not None
    assert zero_record.run_metadata["effective_max_transport_retries"] == 0
    assert zero_record.run_metadata["physical_model_request_count"] == 1
    assert retry_record.run_metadata["effective_max_transport_retries"] == 1
    assert retry_record.run_metadata["physical_model_request_count"] == 2
    assert attempts == {"zero": 1, "retry": 2}


async def test_step_persistence_records_from_the_first_run() -> None:
    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    await run_turn(
        _turn(thread.id),
        step_store=step_store,
        product_store=product,
        run_id="run-1",
        model=build_mock_model(mock),
    )

    records = await step_store.list_runs(conversation_id=thread.id)
    assert [record.run_id for record in records] == ["run-1"]
    events = await step_store.list_events(run_id="run-1")
    assert {event.kind for event in events} >= {"run_started", "model_request_started", "run_completed"}
    snapshot = await step_store.latest_snapshot(run_id="run-1")
    assert snapshot is not None
    assert snapshot.conversation_id == thread.id
    assert snapshot.messages, "the snapshot must carry the turn's messages"


async def test_run_id_is_the_pydantic_ai_run_id() -> None:
    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    await run_turn(
        _turn(thread.id),
        step_store=step_store,
        product_store=product,
        run_id="run-42",
        model=build_mock_model(mock),
    )

    assert (await product.get_run("run-42")) is not None
    assert (await step_store.get_run(run_id="run-42")) is not None


async def test_second_turn_loads_the_prior_snapshot() -> None:
    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    model = build_mock_model(mock)

    await run_turn(
        _turn(thread.id, prompt="first"),
        step_store=step_store,
        product_store=product,
        run_id="run-1",
        model=model,
    )
    history = await load_history(step_store, product, thread.id)
    assert history, "the prior snapshot must be found for the same thread"
    assert all(message.conversation_id == thread.id for message in history)


async def test_failed_run_is_recorded_as_failed() -> None:
    mock = MockOpenAI(content="not json at all")
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    import pytest

    with pytest.raises(Exception):  # noqa: B017 - the turn failed; the type is pydantic-ai's concern
        await run_turn(
            _turn(thread.id),
            step_store=step_store,
            product_store=product,
            run_id="run-1",
            model=build_mock_model(mock),
        )
    run = await product.get_run("run-1")
    assert run is not None and run.status == "failed"
    assert run.error == {
        "reason": "exception",
        "type": "UnexpectedModelBehavior",
        "message": "Exceeded maximum output retries (1)",
    }
    assert run.usage is not None and run.usage["requests"] == 2


async def test_usage_so_far_counts_only_the_responses_of_its_own_run() -> None:
    """A snapshot carries the thread history; earlier runs must not count."""
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    good = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    await run_turn(
        _turn(thread.id, prompt="first"),
        step_store=step_store,
        product_store=product,
        run_id="run-1",
        model=build_mock_model(good),
    )

    import pytest

    with pytest.raises(Exception):  # noqa: B017 - the type is pydantic-ai's concern
        await run_turn(
            _turn(thread.id, prompt="second"),
            step_store=step_store,
            product_store=product,
            run_id="run-2",
            model=build_mock_model(MockOpenAI(content="not json at all")),
        )

    first = await usage_so_far(step_store, "run-1")
    second = await product.get_run("run-2")
    assert first is not None and first["requests"] == 1
    assert second is not None and second.usage is not None
    assert second.usage["requests"] == 2
    absent = await usage_so_far(step_store, "run-absent")
    assert absent is not None and absent["requests"] == 0 and absent["output_tokens"] == 0


async def test_agent_is_cached_per_spec_model_schema(monkeypatch) -> None:
    """F8: one agent serves every run with the same (spec, model, schema)."""
    import agentman.runtime as runtime
    from agentman.agent import build_agent as real_build_agent

    mock = MockOpenAI(content=json.dumps({"title": "hello", "rating": 4}))
    model = build_mock_model(mock)
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")
    built: list[object] = []

    def counting_build_agent(**kwargs):
        agent = real_build_agent(**kwargs)
        built.append(agent)
        return agent

    monkeypatch.setattr(runtime, "build_agent", counting_build_agent)
    monkeypatch.setattr(runtime, "build_model", lambda *args, **kwargs: model)
    runtime.reset_agent_cache()

    for index in range(3):
        await run_turn(
            _turn(thread.id, prompt=f"turn {index}"),
            step_store=step_store,
            product_store=product,
            run_id=f"run-{index}",
        )

    assert len(built) == 1, "the agent must be built once for one (spec, model, schema)"
    for index in range(3):
        record = await step_store.get_run(run_id=f"run-{index}")
        assert record is not None
        assert record.run_id == f"run-{index}"
        assert record.agent_name == "summary"

    # A different schema is a different agent.
    await run_turn(
        Turn(
            workspace="/repo",
            thread_id=thread.id,
            agent_spec={"instructions": "be brief"},
            agent_name="summary",
            model="inferference:gemma",
            prompt="other",
            output_schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "rating": {"type": "integer"},
                    "note": {"type": "string"},
                },
            },
            output_type="models:Other",
        ),
        step_store=step_store,
        product_store=product,
        run_id="run-other",
    )
    assert len(built) == 2


async def test_a_failed_failure_record_does_not_replace_the_original_error() -> None:
    """Postgres is often why a turn failed, so recording ``failed`` may fail too.

    The original exception must still reach the caller. Finding G1.
    """
    import pytest

    class BrokenUpdateStore(FakeProductStore):
        async def update_run(self, run_id: str, **fields: object) -> None:
            raise RuntimeError("the product store is down")

    mock = MockOpenAI(content="not json at all")
    product = BrokenUpdateStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    with pytest.raises(Exception) as caught:  # noqa: B017 - the type is pydantic-ai's concern
        await run_turn(
            _turn(thread.id),
            step_store=InMemoryStepStore(),
            product_store=product,
            run_id="run-1",
            model=build_mock_model(mock),
        )
    assert "the product store is down" not in str(caught.value)
    run = await product.get_run("run-1")
    assert run is not None and run.status == "running", "the failure record could not be written"


async def test_the_journal_run_id_is_the_id_agentman_minted() -> None:
    """Pin the one-canonical-ID invariant against a harness change.

    ``WorkflowRunIdStepPersistence.for_run`` overrides how the harness resolves
    the run id. With ``agent_name`` set and ``run_id`` unset the base class
    derives its own ``sp-`` prefixed id instead. The override exists to stop
    that. Nothing in the harness contract promises the current resolution
    order, so this test is the guard. Finding G17.
    """
    step_store = InMemoryStepStore()
    product = FakeProductStore()
    thread = await product.create_thread(workspace="/repo", name="t")

    await run_turn(
        _turn(thread.id),
        step_store=step_store,
        product_store=product,
        run_id="run-canonical",
        model=build_mock_model(MockOpenAI(content=json.dumps({"title": "t", "rating": 3}))),
    )

    record = await step_store.get_run(run_id="run-canonical")
    assert record is not None, "the journal must hold the id agentman minted"
    assert record.run_id == "run-canonical"
    assert not record.run_id.startswith("sp-"), "the harness derived its own id; the override broke"
    assert record.agent_name == "summary", "the agent name must survive on the journal record"
