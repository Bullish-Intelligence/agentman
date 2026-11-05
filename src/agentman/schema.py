"""Run the repository interpreter: JSON Schema out, validation in.

The client installs once, into the machine toolchain venv. It reaches a
repository's Python types by running that repository's own interpreter in a
subprocess -- never by importing. Schema goes out before the turn; validation
comes back in after it.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from structured_agents import OutputRef

EXIT_OK = 0
EXIT_FINDING = 1
EXIT_INFRA = 2
EXIT_USAGE = 3

_RUNNER_PATH = Path(__file__).with_name("_schema_runner.py")


class SchemaError(Exception):
    """The repository interpreter or the output type could not be resolved."""


class OutputValidationError(Exception):
    """The model returned JSON that the output type rejects."""


@dataclass(frozen=True)
class PythonResolution:
    """The interpreter chosen for a workspace and the paths that were tried."""

    path: str
    tried: tuple[str, ...]


def find_repo_python(workspace: Path) -> PythonResolution:
    """Find the repository interpreter.

    Order: ``<workspace>/.devenv/state/venv/bin/python``, then
    ``<workspace>/.venv/bin/python``, then ``python3`` on ``PATH``. Fail loudly
    when none exists, naming every path that was tried.
    """
    workspace = Path(workspace)
    tried = (
        str(workspace / ".devenv/state/venv/bin/python"),
        str(workspace / ".venv/bin/python"),
    )
    for candidate in tried:
        if Path(candidate).is_file():
            return PythonResolution(path=candidate, tried=tried)
    found = shutil.which("python3")
    if found is not None:
        return PythonResolution(path=found, tried=tried)
    raise SchemaError("no repository interpreter found; tried: " + ", ".join((*tried, "python3 on PATH")))


def schema_out(reference: OutputRef, *, workspace: Path, python: str | None = None) -> dict:
    """Return the JSON Schema of the output type, resolved in the workspace."""
    interpreter = python or find_repo_python(workspace).path
    result = _run(
        interpreter,
        ["schema", str(reference.directory), reference.target],
        workspace=workspace,
    )
    if result.returncode != EXIT_OK:
        raise SchemaError(
            f"cannot resolve output type {reference.target!r} from {reference.directory} "
            f"with {interpreter}: {result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:  # pragma: no cover - runner always emits JSON
        raise SchemaError(f"output type {reference.target!r} produced invalid JSON Schema: {error}") from error


def validate_output(
    reference: OutputRef,
    payload: str,
    *,
    workspace: Path,
    python: str | None = None,
) -> str:
    """Validate ``payload`` against the output type in the workspace interpreter.

    Returns the canonical JSON of the validated instance. Raises
    :class:`OutputValidationError` on a validation finding, and
    :class:`SchemaError` when the type cannot be resolved.
    """
    interpreter = python or find_repo_python(workspace).path
    result = _run(
        interpreter,
        ["validate", str(reference.directory), reference.target],
        workspace=workspace,
        stdin=payload,
    )
    if result.returncode == EXIT_FINDING:
        raise OutputValidationError(result.stderr.strip() or "output validation failed")
    if result.returncode != EXIT_OK:
        raise SchemaError(f"cannot validate against {reference.target!r} with {interpreter}: {result.stderr.strip()}")
    return result.stdout


def _run(
    interpreter: str, arguments: list[str], *, workspace: Path, stdin: str | None = None
) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            [interpreter, str(_RUNNER_PATH), *arguments],
            cwd=workspace,
            input=stdin,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise SchemaError(f"cannot run {interpreter}: {error}") from error


__all__ = [
    "OutputValidationError",
    "PythonResolution",
    "SchemaError",
    "find_repo_python",
    "schema_out",
    "validate_output",
]
