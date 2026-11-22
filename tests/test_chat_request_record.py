"""The non-secret record of one outgoing chat-completion request."""

from __future__ import annotations

import hashlib
import json

import httpx2
import pytest

from agentman.model import (
    TransportRequestCounter,
    _count_chat_completion,
    bind_transport_request_counter,
    chat_request_record,
)
from agentman.protocol import RunMetadata


def _request(body: dict[str, object], url: str = "http://router.test:8080/v1/chat/completions") -> httpx2.Request:
    return httpx2.Request("POST", url, content=json.dumps(body).encode(), headers={"authorization": "Bearer sk-secret"})


def test_record_holds_model_caps_size_hash_host_path_and_time() -> None:
    request = _request({"model": "gemma", "max_tokens": 64, "messages": [{"role": "user", "content": "private text"}]})
    record = chat_request_record(request)

    assert record.model == "gemma"
    assert record.output_caps == {"max_tokens": 64}
    assert record.body_bytes == len(request.content)
    assert record.body_sha256 == hashlib.sha256(request.content).hexdigest()
    assert record.host == "router.test:8080"
    assert record.path == "/v1/chat/completions"
    assert record.at.endswith("+00:00")
    dumped = record.model_dump_json()
    assert "private text" not in dumped
    assert "sk-secret" not in dumped


def test_record_keeps_each_output_cap_field_as_sent() -> None:
    record = chat_request_record(_request({"model": "m", "max_completion_tokens": 9, "max_tokens": None}))
    assert record.output_caps == {"max_tokens": None, "max_completion_tokens": 9}


def test_record_of_a_body_without_a_cap_has_no_cap_field() -> None:
    assert chat_request_record(_request({"model": "m"})).output_caps == {}


def test_record_drops_url_credentials() -> None:
    record = chat_request_record(_request({"model": "m"}, url="http://user:pass@router.test/v1/chat/completions"))
    assert record.host == "router.test"
    assert "pass" not in record.model_dump_json()


def test_record_survives_a_body_that_is_not_json() -> None:
    request = httpx2.Request("POST", "http://router.test/v1/chat/completions", content=b"not json")
    record = chat_request_record(request)
    assert record.model is None
    assert record.output_caps == {}
    assert record.body_bytes == len(b"not json")


@pytest.mark.asyncio
async def test_hook_appends_one_record_for_each_chat_completion_only() -> None:
    counter = TransportRequestCounter()
    with bind_transport_request_counter(counter):
        await _count_chat_completion(_request({"model": "m"}))
        await _count_chat_completion(_request({"model": "m"}))
        await _count_chat_completion(_request({"model": "m"}, url="http://router.test/v1/models"))
    assert counter.count == 2
    assert len(counter.requests) == 2


def test_run_metadata_accepts_an_older_payload_and_ignores_unknown_fields() -> None:
    old = RunMetadata.model_validate({"physical_model_request_count": 2})
    assert old.chat_requests is None
    newer = RunMetadata.model_validate(
        {"chat_requests": [{"model": "m", "body_bytes": 3, "future_field": 1}], "future_field": True}
    )
    assert newer.chat_requests is not None
    assert newer.chat_requests[0].body_bytes == 3
    assert RunMetadata().model_dump(mode="json")["chat_requests"] is None
