"""Phase A: config load, merge, and provenance."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentman.config import CONFIG_FILENAME, ConfigError, load_config, provenance


@pytest.fixture(autouse=True)
def _runtime_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path / "run"))


def _scope(tmp_path: Path, name: str, text: str) -> Path:
    directory = tmp_path / name
    directory.mkdir(parents=True)
    (directory / CONFIG_FILENAME).write_text(text)
    return directory


def test_repo_shadows_global_per_key(tmp_path: Path) -> None:
    global_dir = _scope(tmp_path, "global", "agent: global-agent\nmodel: inferference:global\n")
    repo_dir = _scope(tmp_path, "repo", "agent: repo-agent\n")

    config = load_config(repo_dir=repo_dir, global_dir=global_dir)
    assert config.agent.value == "repo-agent"
    assert config.agent.source == str(repo_dir / CONFIG_FILENAME)
    assert config.model.value == "inferference:global"
    assert config.model.source == str(global_dir / CONFIG_FILENAME)


def test_cli_flag_beats_both_scopes(tmp_path: Path) -> None:
    repo_dir = _scope(tmp_path, "repo", "agent: repo-agent\n")
    config = load_config(cli={"agent": "flag-agent"}, repo_dir=repo_dir)
    assert config.agent.value == "flag-agent"
    assert config.agent.source == "cli"


def test_defaults_apply_when_nothing_names_a_key(tmp_path: Path) -> None:
    config = load_config()
    assert config.agent.value == "default"
    assert config.agent.source == "default"
    assert config.model.value is None
    assert config.socket.value is not None
    assert config.socket.value.endswith("agentman.sock")


def test_repo_config_naming_global_only_key_fails(tmp_path: Path) -> None:
    repo_dir = _scope(tmp_path, "repo", "database_url: postgresql://secret\n")
    with pytest.raises(ConfigError) as error:
        load_config(repo_dir=repo_dir)
    message = str(error.value)
    assert "database_url" in message
    assert str(repo_dir / CONFIG_FILENAME) in message


@pytest.mark.parametrize("key", ["socket", "router_url"])
def test_every_global_only_key_is_refused_in_a_repo(tmp_path: Path, key: str) -> None:
    repo_dir = _scope(tmp_path, "repo", f"{key}: value\n")
    with pytest.raises(ConfigError):
        load_config(repo_dir=repo_dir)


def test_unknown_key_fails(tmp_path: Path) -> None:
    repo_dir = _scope(tmp_path, "repo", "banana: yes\n")
    with pytest.raises(ConfigError) as error:
        load_config(repo_dir=repo_dir)
    assert "banana" in str(error.value)


def test_provenance_attributes_every_value(tmp_path: Path) -> None:
    global_dir = _scope(tmp_path, "global", "database_url: postgresql://db\nmodel: m\n")
    rows = dict((key, (value, source)) for key, value, source in provenance(load_config(global_dir=global_dir)))
    assert rows["database_url"] == ("postgresql://db", str(global_dir / CONFIG_FILENAME))
    assert rows["model"] == ("m", str(global_dir / CONFIG_FILENAME))
    assert rows["agent"] == ("default", "default")


def test_database_url_can_come_from_the_service_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTMAN_DATABASE_URL", "postgresql+asyncpg://agentman@/agentman?host=/run/postgresql")

    config = load_config()

    assert config.database_url.value == "postgresql+asyncpg://agentman@/agentman?host=/run/postgresql"
    assert config.database_url.source == "environment"
