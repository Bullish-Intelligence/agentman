"""Build a pydantic-ai ``Agent`` from a spec.

The agent carries harness ``StepPersistence`` and ``DBOSDurability``. When the
spec opts into tools it also carries agentman's file-effect evidence capability
and harness ``Coder``; agentman does not assemble a replacement toolset.
``capture_frontier`` is not passed. Retries come from the spec; agentman writes
no retry code.

The capabilities are run-agnostic within one cache shape: persistence and
durability read the run id from the invocation context, while optional tools and
file evidence remain rooted in the cached workspace. The run id comes from
``Agent.run(run_id=...)``; the event handler reads ``ctx.run_id`` from the
stream context.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, cast

from pydantic_ai import Agent, StructuredDict
from pydantic_ai.agent.abstract import EventStreamHandler
from pydantic_ai.agent.spec import AgentSpec
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.durable_exec.dbos import DBOSDurability
from pydantic_ai.models import Model
from pydantic_ai.tools import RunContext
from pydantic_ai_harness.coder import Coder
from pydantic_ai_harness.step_persistence import StepPersistence, StepStore

from agentman.effects import FileEffectEvidence
from structured_agents import ToolSpec


@dataclass
class WorkflowRunIdStepPersistence(StepPersistence[Any]):
    """A ``StepPersistence`` that always uses the pydantic-ai run id.

    A cached agent serves many runs, so its ``StepPersistence`` cannot carry a
    per-run id. The harness resolves the id in ``for_run``: with ``agent_name``
    set and ``run_id`` unset it derives a prefixed ``sp-`` id. agentman mints
    the one canonical id at the runtime boundary and passes it to
    ``Agent.run(run_id=...)``, so this subclass restores it after ``for_run``.
    The agent name stays on the journal record.
    """

    async def for_run(self, ctx: RunContext[Any]) -> AbstractCapability[Any]:
        capability = cast("WorkflowRunIdStepPersistence", await super().for_run(ctx))
        if self.run_id is None and ctx.run_id is not None:
            return replace(capability, run_id=ctx.run_id)
        return capability


def _coder(workspace: str) -> Coder[Any]:
    """Return harness Coder with a stable id for its DBOS-owned hook.

    Current harness Coder includes an internal output-limiter capability that
    contributes a durable hook but has no id. DBOS requires an id for every
    durable capability operation. Name that one harness child without changing
    Coder's tools or behavior; the name is stable across workers and versions
    that retain the child.
    """
    coder = Coder(workspace=workspace)
    for capability in coder.capabilities:
        if type(capability).__name__ == "_BoundToolOutputs" and capability.id is None:
            capability.id = "coder_tool_outputs"
    return coder


def output_type_for(schema: Mapping[str, Any] | None) -> Any:
    """Return the output type the daemon builds from a JSON Schema.

    The daemon never receives a Python type, so the schema becomes a
    ``StructuredDict``. No schema means the built-in ``str`` default.
    """
    return StructuredDict(dict(schema)) if schema is not None else str


def build_agent(
    *,
    spec: AgentSpec | dict[str, Any],
    model: Model | str,
    output_schema: Mapping[str, Any] | None,
    capabilities: Sequence[Any] = (),
    name: str | None = None,
) -> Agent[Any, Any]:
    """Build an agent from a spec, a model, an output schema, and capabilities."""
    return Agent.from_spec(
        spec,
        model=model,
        output_type=output_type_for(output_schema),
        name=name,
        capabilities=list(capabilities),
    )


def turn_capabilities(
    *,
    store: StepStore,
    agent_name: str | None,
    event_stream_handler: EventStreamHandler[Any] | None,
    workspace: str = ".",
    tools: str | None = None,
    durability_name: str | None = None,
) -> list[Any]:
    """Build the capabilities every turn carries, in order.

    ``Coder`` is the harness-owned workspace-scoped toolset. The run id is read
    from the pydantic-ai run context, and the event handler reads ``ctx.run_id``
    so the capabilities can serve every run represented by one cache entry.
    """
    capabilities: list[Any] = [
        WorkflowRunIdStepPersistence(store=store, agent_name=agent_name),
    ]
    if tools is not None and tools != ToolSpec.CODER.value:
        raise ValueError(f"unsupported toolset: {tools!r}")
    if tools == ToolSpec.CODER.value:
        # Coder's shell is intentionally unrestricted. The daemon socket's
        # permissions are the access boundary for this system-level capability.
        capabilities.append(FileEffectEvidence(store=store, workspace=workspace))
        capabilities.append(_coder(workspace))
    capabilities.append(DBOSDurability(event_stream_handler=event_stream_handler, name=durability_name))
    return capabilities


__all__ = [
    "WorkflowRunIdStepPersistence",
    "build_agent",
    "output_type_for",
    "turn_capabilities",
]
