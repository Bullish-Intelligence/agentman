# agentman-client

The client side of [agentman](https://github.com/Bullish-Intelligence/agentman).
This distribution has no server dependencies.
It installs no DBOS, SQLAlchemy, asyncpg, psycopg, Alembic, uvicorn,
`pydantic-ai-harness`, or `inferference`.

It provides these modules:

- `agentman.client`: the HTTP client for the daemon Unix socket.
- `agentman.protocol`: the request, response, and event models.
- `agentman.config` and `agentman.discovery`: configuration and workspace lookup.
- `structured_agents`: the agent specification loader.

```python
from agentman.client import Client
from agentman.protocol import TurnRequest
from structured_agents import load_spec
```

The `agentman` distribution depends on this one, at the same version.
It adds the daemon, the runtime, the CLI, and the migrations.
Both distributions install into the one `agentman` package directory.
