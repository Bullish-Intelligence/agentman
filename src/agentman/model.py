"""Resolve a model string. Daemon-side.

One resolver. ``inferference:<name>`` builds an :class:`OpenAIChatModel` aimed at
the local router; anything else goes to pydantic-ai unchanged, so remote works
from day one because pydantic-ai already makes it work. agentman adds no
provider code.

Typed output is profile-gated. pydantic-ai emits
``response_format: {"type": "json_schema", ...}`` only in ``native`` output mode
and refuses native mode unless the model profile advertises support. A bare dict
replaces the provider default rather than merging over it, so the override is a
callable.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

import httpx2
from inferference import (
    Capabilities,
    ConstraintKind,
    InferferenceClient,
    resolve_router_url,
)
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.profiles import ModelProfile
from pydantic_ai.providers.openai import OpenAIProvider

from agentman.config import DEFAULT_ROUTER_KEY_ENV

PREFIX = "inferference:"
"""The model-string prefix that selects the local router."""

DEFAULT_MAX_TRANSPORT_RETRIES = 2
"""The locked OpenAI SDK default, retained when a caller omits the setting."""


@dataclass
class TransportRequestCounter:
    """Count physical chat-completion HTTP attempts in one run context."""

    count: int = 0


_active_transport_counter: ContextVar[TransportRequestCounter | None] = ContextVar(
    "agentman_active_transport_counter", default=None
)


@contextmanager
def bind_transport_request_counter(counter: TransportRequestCounter | None) -> Iterator[None]:
    """Bind a run-local counter while the agent makes model requests."""
    token = _active_transport_counter.set(counter)
    try:
        yield
    finally:
        _active_transport_counter.reset(token)


async def _count_chat_completion(request: httpx2.Request) -> None:
    if request.method == "POST" and request.url.path.endswith("/chat/completions"):
        counter = _active_transport_counter.get()
        if counter is not None:
            counter.count += 1


def router_api_key(env_name: str | None = None, environ: Mapping[str, str] | None = None) -> str | None:
    """Return the router API key from the environment variable ``env_name``.

    ``env_name`` defaults to ``OPENAI_API_KEY``. Return ``None`` when the
    variable is unset or empty, so a router that needs no key sees no header.
    The key is a secret: never log it and never put it in an error message.
    """
    source = os.environ if environ is None else environ
    return source.get(env_name or DEFAULT_ROUTER_KEY_ENV) or None


class ModelResolutionError(Exception):
    """The model string cannot be resolved to a usable model."""


async def resolve_parallelism(
    model_string: str,
    *,
    jobs: int | None,
    router_url: str | None = None,
    api_key: str | None = None,
) -> int:
    """Resolve the client-side request bound for a fan-out.

    An explicit job count is already a complete client contract and must not
    contact inferference. Without one, only an inferference model has a public
    capabilities contract from which the bound can be discovered.
    """
    if jobs is not None:
        if jobs <= 0:
            raise ModelResolutionError("the job count must be a positive integer")
        return jobs
    if not model_string.startswith(PREFIX):
        raise ModelResolutionError(f"cannot discover decode slots for model {model_string!r}; pass an explicit `-j N`")

    try:
        url = router_url or resolve_router_url()
    except Exception as error:
        raise ModelResolutionError(f"cannot resolve the inferference router: {error}") from error
    client = InferferenceClient(url, api_key=api_key)
    try:
        try:
            capabilities = await client.capabilities()
        except Exception as error:
            raise ModelResolutionError(f"cannot read inferference capabilities: {error}") from error
    finally:
        await client.aclose()

    reference = model_string[len(PREFIX) :]
    resolved, model_info = resolve_capability_model(capabilities, reference)
    n_parallel = model_info.n_parallel
    if n_parallel is None or n_parallel <= 0:
        raise ModelResolutionError(
            f"inferference did not advertise a positive n_parallel for model {resolved!r}; pass an explicit `-j N`"
        )
    return n_parallel


async def resolve_model(model_string: str, *, router_url: str | None = None, api_key: str | None = None) -> Model | str:
    """Resolve ``model_string`` to a model.

    An ``inferference:<name>`` string is validated against the live router and
    becomes an :class:`OpenAIChatModel`. Any other string is handed to
    pydantic-ai unchanged.
    """
    if not model_string.startswith(PREFIX):
        return model_string
    url = router_url or resolve_router_url()
    client = InferferenceClient(url, api_key=api_key)
    try:
        capabilities = await client.capabilities()
    finally:
        await client.aclose()
    reference = model_string[len(PREFIX) :]
    require_schema(capabilities)
    resolved, model_info = resolve_capability_model(capabilities, reference)
    return build_model(f"{PREFIX}{resolved}", router_url=url, n_ctx=model_info.n_ctx, api_key=api_key)


def resolve_capability_model(capabilities: Capabilities, reference: str):
    """Resolve the model field once, then inspect that published model."""
    try:
        resolved = capabilities.resolve_model_field(reference)
    except Exception as error:  # CapabilityError carries the operator-facing text
        raise ModelResolutionError(str(error)) from error
    model_info = capabilities.model(resolved)
    if model_info is None:
        raise ModelResolutionError(f"inferference resolved {reference!r} to unknown model {resolved!r}")
    return resolved, model_info


def build_model(
    model_string: str,
    *,
    router_url: str | None,
    n_ctx: int | None,
    max_transport_retries: int | None = None,
    api_key: str | None = None,
) -> Model | str:
    """Build a model from strings alone, with no I/O.

    The DBOS workflow body calls this with values the daemon already resolved,
    so a replayed workflow builds the same model object deterministically.
    """
    if max_transport_retries is not None and max_transport_retries < 0:
        raise ModelResolutionError("`max_transport_retries` must be non-negative")
    if not model_string.startswith(PREFIX):
        if max_transport_retries is not None:
            raise ModelResolutionError(
                f"provider for {model_string!r} cannot enforce `max_transport_retries`; "
                "the setting is supported for inferference models"
            )
        return model_string
    reference = model_string[len(PREFIX) :]
    base_url = _openai_base(router_url or resolve_router_url())
    default_provider = OpenAIProvider(
        base_url=base_url,
        api_key=api_key,
        http_client=httpx2.AsyncClient(event_hooks={"request": [_count_chat_completion]}),
    )
    retry_limit = max_transport_retries if max_transport_retries is not None else DEFAULT_MAX_TRANSPORT_RETRIES
    provider = OpenAIProvider(openai_client=default_provider.client.with_options(max_retries=retry_limit))
    return OpenAIChatModel(reference, provider=provider, profile=profile_for(n_ctx))


def require_schema(capabilities: Capabilities) -> None:
    """Raise when the router does not declare the SCHEMA constraint.

    Structure is enforced at generation time only when the request carries a
    JSON schema. A server that cannot enforce it must fail here rather than
    answer unconstrained.
    """
    if not capabilities.supports(ConstraintKind.SCHEMA):
        raise ModelResolutionError(
            "the router does not declare the SCHEMA constraint, so typed output cannot be enforced; "
            "refusing to send a request the server would answer unconstrained"
        )


def profile_for(n_ctx: int | None) -> Callable[[ModelProfile], ModelProfile]:
    def profile(base: ModelProfile) -> ModelProfile:
        return {
            **base,
            "context_window": n_ctx,
            "supports_json_schema_output": True,
            "default_structured_output_mode": "native",
        }

    return profile


def _openai_base(url: str) -> str:
    """Return the OpenAI-compatible base URL, with exactly one ``/v1`` suffix."""
    trimmed = url.rstrip("/")
    return trimmed if trimmed.endswith("/v1") else f"{trimmed}/v1"


__all__ = [
    "PREFIX",
    "DEFAULT_MAX_TRANSPORT_RETRIES",
    "ModelResolutionError",
    "TransportRequestCounter",
    "bind_transport_request_counter",
    "build_model",
    "router_api_key",
    "profile_for",
    "resolve_capability_model",
    "require_schema",
    "resolve_parallelism",
    "resolve_model",
]
