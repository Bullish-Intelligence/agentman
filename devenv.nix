# agentman — Python development environment.
#
# Each check task first syncs the venv from uv.lock. The dev group supplies
# pytest, ruff, ty, and import-linter.
{ pkgs, config, ... }:

{
  # `uv sync` in the `setup:sync` task needs `git`.
  packages = [ pkgs.git ];

  # Python toolchain. The venv hosts the app and the dev group (pytest, ruff, ty).
  # Run `uv sync` once to fill it.
  languages.python = {
    enable = true;
    venv.enable = true;
    uv.enable = true;
  };

  # The project-local Postgres cluster. The system cluster on 5432 belongs to
  # another service, so this project starts its own cluster on its own port.
  # devenv supervises it as a process; start it with `devenv up`.
  services.postgres = {
    enable = true;
    port = 5433;
    listen_addresses = "127.0.0.1";
    initialDatabases = [ { name = "agentman"; } ];
  };

  # https://devenv.sh/tasks/
  #
  # The `setup:*` tasks run once, then the standard tasks (check:*, test:*).
  # The `input` declarations let a check runner reuse a result when a commit
  # changes none of the task inputs.
  tasks =
    let
      # Files that no lint, format, type, or test task reads.
      unread = [
        "README.md"
        "AGENTS.md"
        "CLAUDE.md"
        ".gitignore"
        ".tmuxp.yaml"
        "flake.nix"
        "flake.lock"
        "nix/**"
        "scripts/**"
        "docs/**"
      ];
    in
    {
      "setup:sync" = {
        exec = "uv sync --locked";
        # hatchling reads the readme when uv builds the editable install.
        input.testee.reads = [ "uv.lock" "pyproject.toml" "README.md" ];
      };

      "check:lint" = {
        exec = ''
          out="''${TESTEE_RUN_DIR:-.devenv/state}"
          mkdir -p "$out"
          set -o pipefail
          uv run --no-sync ruff check src tests --output-format json | tee "$out/check:lint.ruff.json"
        '';
        input.testee.ignores = unread;
      };

      "check:format" = {
        exec = "uv run --no-sync ruff format --check src tests";
        input.testee.ignores = unread;
      };

      "check:types" = {
        exec = "uv run --no-sync ty check src";
        input.testee.ignores = unread;
      };

      "test:quick" = {
        exec = ''
          echo "n/a: no quick test subset"
          exit 0
        '';
        input.testee.ignores = unread;
      };

      "test:full" = {
        exec = ''
          out="''${TESTEE_RUN_DIR:-.devenv/state}"
          mkdir -p "$out"
          uv run --no-sync python -m pytest --junitxml="$out/test:full.junit.xml"
        '';
        input.testee.ignores = unread;
      };

      "base:check" = {
        after = [ "check:lint" "check:format" "check:types" ];
        input.testee.ignores = unread;
      };
      "base:test" = {
        after = [ "test:full" ];
        input.testee.ignores = unread;
      };
    };
}
