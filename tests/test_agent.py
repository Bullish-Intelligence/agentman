"""Turn capability configuration tests."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic_ai.durable_exec._operation import (
    CapabilityOperationId,
    EventStreamHandlerId,
    ModelRequestId,
    OperationConfigRole,
    ToolsetCallToolId,
)
from pydantic_ai.models.test import TestModel
from pydantic_ai_harness.step_persistence import InMemoryStepStore

from agentman.agent import _SerializableDBOSDurability, build_agent, turn_capabilities

OPERATION_IDS: dict[OperationConfigRole, Any] = {
    "model": ModelRequestId(None, streaming=True, model_name="default"),
    "event": EventStreamHandlerId(),
    "tool": ToolsetCallToolId("mcp", toolset_id="tools"),
    "capability": CapabilityOperationId("step_persistence", operation="record_tool_effect"),
}


async def _discard_events(ctx: Any, events: Any) -> None:
    async for _ in events:
        pass


def _durability(*, tools: str | None) -> _SerializableDBOSDurability:
    capabilities = turn_capabilities(
        store=InMemoryStepStore(),
        agent_name="cancel-config",
        event_stream_handler=_discard_events,
        workspace=".",
        tools=tools,
        durability_name=f"agentman_cancel_config_{tools or 'plain'}",
    )
    agent = build_agent(
        spec={"instructions": "be brief"},
        model=TestModel(),
        output_schema=None,
        capabilities=capabilities,
        name="cancel-config",
    )
    bound: list[Any] = []
    agent._root_capability.apply(bound.append)
    return next(item for item in bound if isinstance(item, _SerializableDBOSDurability))


@pytest.mark.parametrize("tools", [None, "coder"])
def test_only_the_model_role_is_preemptible(tools: str | None) -> None:
    durability = _durability(tools=tools)

    # The backend keeps the role configuration in a private attribute. The
    # pydantic-ai version in use has no public accessor for the bound agent.
    config = durability.get_durable_operation_backend()._config
    assert config.base("model", operation_id=OPERATION_IDS["model"]).get("preemptible") is True
    for role in ("event", "tool", "capability"):
        assert "preemptible" not in config.base(role, operation_id=OPERATION_IDS[role])
