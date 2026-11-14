"""Phase C: the model resolver and the native-output profile gate."""

from __future__ import annotations

import asyncio

import pytest
from inferference import Capabilities, ConstraintKind, ModelInfo
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.profiles import ModelProfile

from agentman.model import (
    PREFIX,
    ModelResolutionError,
    build_model,
    profile_for,
    require_schema,
    resolve_model,
    resolve_parallelism,
    router_api_key,
)


def test_non_inferference_string_passes_through_untouched() -> None:
    import asyncio

    assert asyncio.run(resolve_model("openai:gpt-4o")) == "openai:gpt-4o"
    assert asyncio.run(resolve_model("anthropic:claude")) == "anthropic:claude"


def test_schema_absent_raises_before_any_request() -> None:
    capabilities = Capabilities(server="test", models=(), constraints=frozenset({ConstraintKind.CHOICE}))
    with pytest.raises(ModelResolutionError) as error:
        require_schema(capabilities)
    assert "SCHEMA" in str(error.value)


def test_schema_present_is_accepted() -> None:
    require_schema(Capabilities(server="test", models=(), constraints=frozenset({ConstraintKind.SCHEMA})))


def test_profile_merges_over_the_provider_default() -> None:
    base: ModelProfile = {"context_window": 1, "supports_tools": True, "supports_json_schema_output": False}
    profile = profile_for(32768)
    merged = profile(base)
    assert merged["context_window"] == 32768
    assert merged["supports_json_schema_output"] is True
    assert merged["default_structured_output_mode"] == "native"
    assert merged["supports_tools"] is True


def test_build_model_aims_at_the_openai_compatible_base() -> None:
    model = build_model(f"{PREFIX}gemma", router_url="http://127.0.0.1:8100", n_ctx=32768)
    assert isinstance(model, OpenAIChatModel)
    assert model.model_name == "gemma"
    assert model.base_url.rstrip("/") == "http://127.0.0.1:8100/v1"
    assert model.profile["context_window"] == 32768
    assert model.provider.client.max_retries == 2


def test_build_model_accepts_a_v1_url_without_doubling_it() -> None:
    model = build_model(f"{PREFIX}gemma", router_url="http://127.0.0.1:8100/v1", n_ctx=None)
    assert isinstance(model, OpenAIChatModel)
    assert model.base_url.rstrip("/") == "http://127.0.0.1:8100/v1"


def test_build_model_passes_other_strings_through() -> None:
    assert build_model("openai:gpt-4o", router_url=None, n_ctx=None) == "openai:gpt-4o"


@pytest.mark.parametrize("retry_limit", [0, 1, 4])
def test_build_model_sets_the_requested_transport_retry_limit(retry_limit: int) -> None:
    model = build_model(
        f"{PREFIX}gemma",
        router_url="http://127.0.0.1:8100",
        n_ctx=None,
        max_transport_retries=retry_limit,
    )

    assert isinstance(model, OpenAIChatModel)
    assert model.provider.client.max_retries == retry_limit


@pytest.mark.parametrize("model_string", ["openai:gpt-4o", "anthropic:claude"])
def test_build_model_rejects_an_explicit_limit_for_unsupported_providers(model_string: str) -> None:
    with pytest.raises(ModelResolutionError, match="cannot enforce `max_transport_retries`"):
        build_model(model_string, router_url=None, n_ctx=None, max_transport_retries=0)


def test_resolve_parallelism_reads_the_resolved_model_field_and_closes_client(monkeypatch) -> None:
    class FakeCapabilities:
        def resolve_model_field(self, reference: str) -> str:
            assert reference == "alias"
            return "resident"

        def model(self, name: str) -> ModelInfo | None:
            assert name == "resident"
            return ModelInfo(name=name, n_parallel=8)

    class FakeClient:
        instances: list[FakeClient] = []

        def __init__(self, url: str, *, api_key: str | None = None) -> None:
            self.url = url
            self.api_key = api_key
            self.closed = False
            self.instances.append(self)

        async def capabilities(self) -> FakeCapabilities:
            return FakeCapabilities()

        async def aclose(self) -> None:
            self.closed = True

    import agentman.model as model

    monkeypatch.setattr(model, "InferferenceClient", FakeClient)
    assert asyncio.run(resolve_parallelism("inferference:alias", jobs=None, router_url="http://router")) == 8
    assert FakeClient.instances[0].url == "http://router"
    assert FakeClient.instances[0].api_key is None
    assert FakeClient.instances[0].closed


def test_resolve_parallelism_passes_the_api_key_to_the_client(monkeypatch) -> None:
    class FakeCapabilities:
        def resolve_model_field(self, reference: str) -> str:
            return reference

        def model(self, name: str) -> ModelInfo:
            return ModelInfo(name=name, n_parallel=2)

    class FakeClient:
        keys: list[str | None] = []

        def __init__(self, url: str, *, api_key: str | None = None) -> None:
            self.keys.append(api_key)

        async def capabilities(self) -> FakeCapabilities:
            return FakeCapabilities()

        async def aclose(self) -> None:
            pass

    import agentman.model as model

    monkeypatch.setattr(model, "InferferenceClient", FakeClient)
    asyncio.run(resolve_parallelism("inferference:m", jobs=None, router_url="http://router", api_key="k-1"))
    assert FakeClient.keys == ["k-1"]


def test_router_api_key_reads_the_named_variable() -> None:
    assert router_api_key("MY_KEY", {"MY_KEY": "k-1", "OPENAI_API_KEY": "other"}) == "k-1"


def test_router_api_key_defaults_to_openai_api_key() -> None:
    assert router_api_key(None, {"OPENAI_API_KEY": "k-2"}) == "k-2"


@pytest.mark.parametrize("environ", [{}, {"OPENAI_API_KEY": ""}])
def test_router_api_key_is_none_when_unset_or_empty(environ: dict[str, str]) -> None:
    assert router_api_key(None, environ) is None


def test_build_model_sends_the_api_key_as_a_bearer_header() -> None:
    model = build_model(f"{PREFIX}gemma", router_url="http://127.0.0.1:8100", n_ctx=None, api_key="k-3")
    assert isinstance(model, OpenAIChatModel)
    assert model.provider.client.api_key == "k-3"


def test_resolve_model_uses_the_resolved_model_field(monkeypatch) -> None:
    class FakeCapabilities:
        def supports(self, kind: ConstraintKind) -> bool:
            return kind == ConstraintKind.SCHEMA

        def resolve_model_field(self, reference: str) -> str:
            assert reference == "alias"
            return "resident"

        def model(self, name: str) -> ModelInfo | None:
            assert name == "resident"
            return ModelInfo(name=name, n_ctx=16384, n_parallel=4)

    class FakeClient:
        closed = False

        async def capabilities(self) -> FakeCapabilities:
            return FakeCapabilities()

        async def aclose(self) -> None:
            self.closed = True

    import agentman.model as model

    client = FakeClient()
    monkeypatch.setattr(model, "InferferenceClient", lambda url, *, api_key=None: client)
    resolved = asyncio.run(model.resolve_model("inferference:alias", router_url="http://router"))

    assert isinstance(resolved, OpenAIChatModel)
    assert resolved.model_name == "resident"
    assert client.closed


def test_explicit_parallelism_does_not_lookup_capabilities(monkeypatch) -> None:
    import agentman.model as model

    def explode(*args: object, **kwargs: object) -> object:
        raise AssertionError("explicit jobs must not look up capabilities")

    monkeypatch.setattr(model, "InferferenceClient", explode)
    assert asyncio.run(resolve_parallelism("mock:model", jobs=3, router_url=None)) == 3


def test_non_inferference_default_requires_explicit_parallelism() -> None:
    with pytest.raises(ModelResolutionError, match="pass an explicit `-j N`"):
        asyncio.run(resolve_parallelism("mock:model", jobs=None, router_url=None))


def test_missing_advertised_parallelism_is_a_configuration_error(monkeypatch) -> None:
    class FakeCapabilities:
        def resolve_model_field(self, reference: str) -> str:
            return reference

        def model(self, name: str) -> ModelInfo:
            return ModelInfo(name=name)

    class FakeClient:
        def __init__(self, url: str, *, api_key: str | None = None) -> None:
            pass

        async def capabilities(self) -> FakeCapabilities:
            return FakeCapabilities()

        async def aclose(self) -> None:
            pass

    import agentman.model as model

    monkeypatch.setattr(model, "InferferenceClient", FakeClient)
    with pytest.raises(ModelResolutionError, match="positive n_parallel"):
        asyncio.run(resolve_parallelism("inferference:gemma", jobs=None, router_url="http://router"))
