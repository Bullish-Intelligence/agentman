"""Phase A: the repository-interpreter subprocess, schema out and validation in."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from agentman.schema import (
    OutputValidationError,
    SchemaError,
    find_repo_python,
    schema_out,
    validate_output,
)
from structured_agents import OutputRef

MODELS = """
from pydantic import BaseModel, Field


class Address(BaseModel):
    street: str
    city: str


class Summary(BaseModel):
    title: str = Field(min_length=3, max_length=40)
    rating: int = Field(ge=0, le=5)
    address: Address
"""


@pytest.fixture()
def scratch_repo(tmp_path: Path) -> Path:
    (tmp_path / "models.py").write_text(MODELS)
    return tmp_path


def _reference(workspace: Path, target: str = "models:Summary") -> OutputRef:
    return OutputRef(directory=workspace, target=target)


def test_schema_out_produces_defs_for_nested_models(scratch_repo: Path) -> None:
    schema = schema_out(_reference(scratch_repo), workspace=scratch_repo, python=sys.executable)
    assert schema["type"] == "object"
    assert "$defs" in schema
    assert "Address" in schema["$defs"]
    assert schema["properties"]["address"]["$ref"].endswith("/Address")


def test_restrictive_model_round_trips_validation(scratch_repo: Path) -> None:
    payload = '{"title": "hello", "rating": 4, "address": {"street": "a", "city": "b"}}'
    canonical = validate_output(_reference(scratch_repo), payload, workspace=scratch_repo, python=sys.executable)
    assert '"title":"hello"' in canonical.replace(" ", "")


def test_validation_finding_is_not_an_infra_error(scratch_repo: Path) -> None:
    bad = '{"title": "x", "rating": 9, "address": {"street": "a", "city": "b"}}'
    with pytest.raises(OutputValidationError):
        validate_output(_reference(scratch_repo), bad, workspace=scratch_repo, python=sys.executable)


def test_missing_module_reports_the_path(scratch_repo: Path) -> None:
    with pytest.raises(SchemaError) as error:
        schema_out(_reference(scratch_repo, "missing:Thing"), workspace=scratch_repo, python=sys.executable)
    assert "missing.py" in str(error.value)


def test_missing_attribute_is_an_infra_error(scratch_repo: Path) -> None:
    with pytest.raises(SchemaError) as error:
        schema_out(_reference(scratch_repo, "models:Nope"), workspace=scratch_repo, python=sys.executable)
    assert "Nope" in str(error.value)


def test_find_repo_python_prefers_the_devenv_venv(tmp_path: Path) -> None:
    python = tmp_path / ".devenv/state/venv/bin/python"
    python.parent.mkdir(parents=True)
    python.write_text("")
    resolution = find_repo_python(tmp_path)
    assert resolution.path == str(python)


def test_find_repo_python_fails_loudly_naming_the_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("agentman.schema.shutil.which", lambda _name: None)
    with pytest.raises(SchemaError) as error:
        find_repo_python(tmp_path)
    message = str(error.value)
    assert ".devenv/state/venv/bin/python" in message
    assert ".venv/bin/python" in message
    assert "python3" in message
