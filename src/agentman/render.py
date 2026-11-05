"""Markdown from ``ModelMessage``.

Rendered on demand, never parsed, never stored, never authoritative.
``messages_json`` is pydantic-ai ``ModelMessage`` JSON and never a second
message format.
"""

from __future__ import annotations

from typing import Any

from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    SystemPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)


def render_thread(messages_json: list[dict[str, Any]] | None) -> str:
    """Render stored ``ModelMessage`` JSON as Markdown."""
    if not messages_json:
        return ""
    messages: list[ModelMessage] = ModelMessagesTypeAdapter.validate_python(messages_json)
    return render_messages(messages)


def render_messages(messages: list[ModelMessage]) -> str:
    """Render a message history as Markdown."""
    blocks: list[str] = []
    for message in messages:
        blocks.extend(_render_message(message))
    return "\n\n".join(block for block in blocks if block).rstrip() + ("\n" if blocks else "")


def _render_message(message: ModelMessage) -> list[str]:
    if isinstance(message, ModelRequest):
        return _render_request(message)
    if isinstance(message, ModelResponse):
        return _render_response(message)
    return []


def _render_request(message: ModelRequest) -> list[str]:
    blocks: list[str] = []
    for part in message.parts:
        if isinstance(part, UserPromptPart):
            blocks.append(f"## User\n\n{_text(part.content)}")
        elif isinstance(part, SystemPromptPart):
            blocks.append(f"> system: {_text(part.content)}")
        elif isinstance(part, ToolReturnPart):
            blocks.append(f"**tool `{part.tool_name}` returned**\n\n```\n{_text(part.content)}\n```")
        elif isinstance(part, RetryPromptPart):
            blocks.append(f"> retry: {_text(part.content)}")
    return blocks


def _render_response(message: ModelResponse) -> list[str]:
    blocks: list[str] = []
    for part in message.parts:
        if isinstance(part, TextPart):
            blocks.append(f"## Assistant\n\n{part.content}")
        elif isinstance(part, ThinkingPart):
            blocks.append(f"<details><summary>thinking</summary>\n\n{part.content}\n\n</details>")
        elif isinstance(part, ToolCallPart):
            blocks.append(f"**tool call `{part.tool_name}`**\n\n```json\n{part.args_as_json_str()}\n```")
    return blocks


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return str(content)


__all__ = ["render_messages", "render_thread"]
