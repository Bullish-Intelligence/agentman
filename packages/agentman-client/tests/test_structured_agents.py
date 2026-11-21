"""Phase A: spec discovery and the agentman `output:` extension."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic_ai.agent.spec import AgentSpec

from structured_agents import LoadedSpec, SpecError, ToolSpec, as_mapping, discover_specs, load_spec


def _write(directory: Path, name: str, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(text)
    return path


def test_repo_spec_shadows_global_spec(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    global_ = tmp_path / "global"
    _write(repo, "reviewer.yaml", "instructions: repo instructions\n")
    _write(global_, "reviewer.yaml", "instructions: global instructions\n")

    found = discover_specs([repo, global_])
    assert found == {"reviewer": repo / "reviewer.yaml"}


def test_config_file_is_never_an_agent(tmp_path: Path) -> None:
    directory = tmp_path / "repo"
    _write(directory, "agentman.yaml", "agent: default\n")
    _write(directory, "reviewer.yaml", "instructions: hi\n")
    assert list(discover_specs([directory])) == ["reviewer"]


def test_output_key_is_popped_before_pydantic_ai_sees_it(tmp_path: Path) -> None:
    directory = tmp_path / "repo"
    path = _write(directory, "summary.yaml", "instructions: hi\noutput: models:Summary\n")

    loaded = load_spec(path)
    assert isinstance(loaded.agent, AgentSpec)
    assert loaded.agent.output_schema is None
    mapping = as_mapping(loaded)
    assert "output" not in mapping
    assert loaded.output is not None
    assert loaded.output.directory == directory
    assert loaded.output.target == "models:Summary"


def test_spec_without_output_yields_the_str_default(tmp_path: Path) -> None:
    path = _write(tmp_path, "chat.yaml", "instructions: hi\n")
    loaded = load_spec(path)
    assert loaded.output is None


def test_explicit_str_output_is_the_default(tmp_path: Path) -> None:
    path = _write(tmp_path, "chat.yaml", "output: str\n")
    assert load_spec(path).output is None


def test_tools_key_is_popped_and_validated(tmp_path: Path) -> None:
    path = _write(tmp_path, "coder.yaml", "instructions: edit files\ntools: coder\n")

    loaded = load_spec(path)

    assert loaded.tools is ToolSpec.CODER
    assert "tools" not in as_mapping(loaded)


def test_unknown_toolset_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "broken.yaml", "tools: shell\n")

    with pytest.raises(SpecError, match="unsupported `tools: shell`"):
        load_spec(path)


def test_name_falls_back_to_the_filename_stem(tmp_path: Path) -> None:
    path = _write(tmp_path, "parser.yaml", "instructions: hi\n")
    assert load_spec(path).name == "parser"


def test_explicit_name_wins(tmp_path: Path) -> None:
    path = _write(tmp_path, "parser.yaml", "name: the-parser\n")
    assert load_spec(path).name == "the-parser"


def test_malformed_output_names_the_file(tmp_path: Path) -> None:
    path = _write(tmp_path, "broken.yaml", "output: no-colon-here\n")
    with pytest.raises(SpecError) as error:
        load_spec(path)
    assert str(path) in str(error.value)


def test_invalid_spec_names_the_file(tmp_path: Path) -> None:
    path = _write(tmp_path, "broken.yaml", "retries: not-a-number\n")
    with pytest.raises(SpecError) as error:
        load_spec(path)
    assert str(path) in str(error.value)


def test_loaded_spec_is_a_value(tmp_path: Path) -> None:
    path = _write(tmp_path, "chat.yaml", "instructions: hi\n")
    loaded: LoadedSpec = load_spec(path)
    assert loaded.path == path
    assert loaded.directory == tmp_path
