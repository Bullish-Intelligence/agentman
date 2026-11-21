"""Run import-linter with both source trees visible.

The `agentman` package comes from two distributions: `agentman-client` ships
`agentman/__init__.py`, and the root distribution ships the server modules. In
a development checkout these are two source trees. Python joins them when it
imports `agentman`, but grimp reads the package location before any import.
This script imports `agentman` first, so grimp sees the joined path.

Use: ``uv run python tests/support/lint_imports.py [--config FILE]``

With ``--merged``, the script does not import `agentman`. Use it when one source
folder already holds both trees, as in a wheel install.
"""

from __future__ import annotations

import argparse
import sys

from importlinter.cli import lint_imports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None, help="The import-linter config file.")
    parser.add_argument("--merged", action="store_true", help="One folder holds both source trees.")
    args = parser.parse_args(argv)
    if not args.merged:
        import agentman  # noqa: F401  (the import joins the package paths)
    return lint_imports(config_filename=args.config, no_cache=True, no_logo=True)


if __name__ == "__main__":
    sys.exit(main())
