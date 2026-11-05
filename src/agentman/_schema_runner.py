"""Resolve one pydantic output type in a repository interpreter.

Run this file by absolute path with any Python 3 interpreter that has pydantic
installed. It must not import ``agentman``: the repository venv does not have
agentman, and that is the point of the subprocess. The client resolves the
repository's types in the repository's own interpreter.

Usage::

    python _schema_runner.py schema <directory> <module:Attribute>
    python _schema_runner.py validate <directory> <module:Attribute>   # JSON on stdin

The named module resolves against ``directory`` by file. The directory is
added to ``sys.path`` only for the duration of the import, so a module may
import its siblings, and nothing outlives this process. This is a throwaway
interpreter, so that is not pollution of the client's environment.

Exit codes: ``0`` ok · ``1`` validation finding · ``2`` import/infra · ``3`` usage.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, NoReturn

EXIT_OK = 0
EXIT_FINDING = 1
EXIT_INFRA = 2
EXIT_USAGE = 3


def _fail(code: int, message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def _load(directory: Path, target: str) -> Any:
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        _fail(EXIT_USAGE, f"target {target!r} is not `module:Attribute`")
    module_path = directory / (module_name.replace(".", "/") + ".py")
    if not module_path.is_file():
        _fail(EXIT_INFRA, f"cannot import {target!r}: {module_path} does not exist")
    sys.path.insert(0, str(directory))
    try:
        spec = importlib.util.spec_from_file_location(f"_agentman_output_{module_name}", module_path)
        if spec is None or spec.loader is None:  # pragma: no cover
            _fail(EXIT_INFRA, f"cannot load {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
        except Exception as error:
            _fail(EXIT_INFRA, f"error importing {module_path}: {error!r}")
        try:
            return getattr(module, attribute)
        except AttributeError:
            _fail(EXIT_INFRA, f"{module_path} has no attribute {attribute!r}")
    finally:
        sys.path.pop(0)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        _fail(EXIT_USAGE, __doc__ or "usage error")
    operation, directory_text, target = argv[1], argv[2], argv[3]
    directory = Path(directory_text)

    if operation == "schema":
        output_type = _load(directory, target)
        schema_method = getattr(output_type, "model_json_schema", None)
        if schema_method is None:
            _fail(EXIT_INFRA, f"{target!r} is not a pydantic model: no model_json_schema()")
        print(json.dumps(schema_method()))
        return EXIT_OK

    if operation == "validate":
        payload = sys.stdin.read()
        output_type = _load(directory, target)
        validate_method = getattr(output_type, "model_validate_json", None)
        if validate_method is None:
            _fail(EXIT_INFRA, f"{target!r} is not a pydantic model: no model_validate_json()")
        try:
            instance = validate_method(payload)
        except Exception as error:
            print(str(error), file=sys.stderr)
            return EXIT_FINDING
        print(instance.model_dump_json())
        return EXIT_OK

    _fail(EXIT_USAGE, f"unknown operation {operation!r}; expected `schema` or `validate`")
    return EXIT_USAGE  # unreachable


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
