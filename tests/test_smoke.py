"""The skeleton's only claim: the package imports and states a version."""

from importlib import import_module


def test_package_states_a_three_part_version() -> None:
    module = import_module("agentman")
    assert module.__version__.count(".") == 2
