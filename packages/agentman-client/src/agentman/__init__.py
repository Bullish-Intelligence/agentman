"""agentman — a CLI for talking to typed agents, with every call recorded.

This file belongs to the ``agentman-client`` distribution.

The package owns three things: which thread a turn belongs to, which agent and
model run it, and where the record lives. See ``docs/architecture.md``.
"""

# The `agentman` distribution ships modules in this package too (daemon, CLI).
# Extend the path so they resolve when both distributions are installed, in
# one folder or in two editable source trees.
__path__[:] = __import__("pkgutil").extend_path(__path__, __name__)

__version__ = "0.0.4"
