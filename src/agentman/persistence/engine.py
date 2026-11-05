"""The async engine and session factory. Daemon-private.

One line matters more than the rest of this file: there is no global lock, no
single shared connection, and no exclusive session. Postgres provides the
concurrency boundary; agentman adds none. A lock here would silently serialise
every turn and destroy the batching invariant without failing a test.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    """Build the application-owned asynchronous engine."""
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build the session factory over ``engine``."""
    return async_sessionmaker(engine, expire_on_commit=False)


__all__ = ["create_engine", "create_session_factory"]
