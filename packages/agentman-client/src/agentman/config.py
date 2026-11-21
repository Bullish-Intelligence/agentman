"""Load and merge ``agentman.yaml``, and report where each value came from.

The schema is flat on purpose. Override always means "this key wins", so there
is no deep-merge ambiguity to reason about.

Two scopes, one rule: repository shadows global, per key. The daemon reads only
the global file; the client reads the merged result. That split is what keeps a
Postgres URL out of a tracked repository file.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_FILENAME = "agentman.yaml"

REPO_KEYS = ("agent", "model")
"""Keys a repository config may set."""

GLOBAL_ONLY_KEYS = ("database_url", "socket", "router_url", "router_key_env")
"""Keys only the global config may set. A repository that names one is an error."""

CONFIG_KEYS = REPO_KEYS + GLOBAL_ONLY_KEYS

DEFAULT_ROUTER_KEY_ENV = "OPENAI_API_KEY"
"""The environment variable that holds the router API key, unless config names another.

The chat model client already reads this variable, so one key serves both the
capability probe and the chat requests."""

DEFAULT_AGENT = "default"
"""The built-in agent name, used when no scope names one."""

SOURCE_CLI = "cli"
SOURCE_DEFAULT = "default"
SOURCE_ENVIRONMENT = "environment"


class ConfigError(Exception):
    """A config file is unreadable, malformed, or names a key it may not set."""


@dataclass(frozen=True)
class Value:
    """One resolved config value and where it came from."""

    value: str | None
    source: str


@dataclass(frozen=True)
class Config:
    """The resolved, flat configuration."""

    agent: Value
    model: Value
    database_url: Value
    socket: Value
    router_url: Value
    router_key_env: Value = Value(DEFAULT_ROUTER_KEY_ENV, "default")

    def get(self, key: str) -> Value:
        if key not in CONFIG_KEYS:
            raise KeyError(key)
        return getattr(self, key)

    def items(self):
        for key in CONFIG_KEYS:
            yield key, self.get(key)


def default_socket_path() -> Path:
    """Return the socket path under the runtime directory.

    ``$XDG_RUNTIME_DIR`` is unset in non-interactive shells on some machines.
    Fall back to ``/run/user/<uid>`` and fail loudly when neither resolves.
    """
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    if runtime:
        return Path(runtime) / "agentman.sock"
    fallback = Path(f"/run/user/{os.getuid()}")
    if fallback.is_dir():
        return fallback / "agentman.sock"
    raise ConfigError(f"cannot resolve the runtime directory: $XDG_RUNTIME_DIR is unset and {fallback} does not exist")


def load_config(
    *,
    cli: Mapping[str, str | None] | None = None,
    repo_dir: Path | None = None,
    global_dir: Path | None = None,
) -> Config:
    """Load and merge the flat config.

    Precedence is CLI flag, then repository config, then global config, then the
    built-in default. ``repo_dir`` and ``global_dir`` are the agentman
    directories, or ``None`` when they do not exist.
    """
    cli = {key: value for key, value in (cli or {}).items() if value is not None}
    repo = _read_config(repo_dir, allow_global_only=False) if repo_dir is not None else {}
    global_ = _read_config(global_dir, allow_global_only=True) if global_dir is not None else {}

    resolved: dict[str, Value] = {}
    for key in CONFIG_KEYS:
        if key in cli:
            resolved[key] = Value(cli[key], SOURCE_CLI)
        elif key in repo:
            assert repo_dir is not None
            resolved[key] = Value(repo[key], str(repo_dir / CONFIG_FILENAME))
        elif key in global_:
            assert global_dir is not None
            resolved[key] = Value(global_[key], str(global_dir / CONFIG_FILENAME))
        elif key == "database_url" and os.environ.get("AGENTMAN_DATABASE_URL"):
            resolved[key] = Value(os.environ["AGENTMAN_DATABASE_URL"], SOURCE_ENVIRONMENT)
        else:
            resolved[key] = Value(_default(key), SOURCE_DEFAULT)

    return Config(**resolved)


def _default(key: str) -> str | None:
    if key == "agent":
        return DEFAULT_AGENT
    if key == "socket":
        return str(default_socket_path())
    if key == "router_key_env":
        return DEFAULT_ROUTER_KEY_ENV
    return None


def _read_config(directory: Path, *, allow_global_only: bool) -> dict[str, str]:
    path = directory / CONFIG_FILENAME
    if not path.is_file():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"cannot read config {path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigError(f"cannot parse config {path}: {error}") from error
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"config {path} must be a mapping, got {type(raw).__name__}")

    resolved: dict[str, str] = {}
    for key, value in raw.items():
        if key not in CONFIG_KEYS:
            raise ConfigError(f"config {path} names unknown key `{key}`; known keys: {', '.join(CONFIG_KEYS)}")
        if key in GLOBAL_ONLY_KEYS and not allow_global_only:
            raise ConfigError(
                f"config {path} names `{key}`, which is global-only; "
                f"move it to $XDG_CONFIG_HOME/agentman/{CONFIG_FILENAME}"
            )
        if value is not None and not isinstance(value, str):
            raise ConfigError(f"config {path}: `{key}` must be a string, got {type(value).__name__}")
        if value is not None:
            resolved[key] = value
    return resolved


def provenance(config: Config) -> list[tuple[str, str | None, str]]:
    """Return ``(key, value, source)`` rows, in a stable order for display."""
    return [(key, value.value, value.source) for key, value in config.items()]


__all__ = [
    "CONFIG_FILENAME",
    "CONFIG_KEYS",
    "DEFAULT_AGENT",
    "DEFAULT_ROUTER_KEY_ENV",
    "GLOBAL_ONLY_KEYS",
    "REPO_KEYS",
    "Config",
    "ConfigError",
    "Value",
    "default_socket_path",
    "load_config",
    "provenance",
]
