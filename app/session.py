"""Database session management for SQLAlchemy Core."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError

_engine: Engine | None = None

def _get_engine() -> Engine:
    """Return a cached SQLAlchemy Engine singleton.

    Prioritizes POSTGRES_TEST_URL for test isolation, falling back
    to DATABASE_URL for standard execution.
    """
    global _engine
    if _engine is None:
        db_url: str | None = (
            os.environ.get("POSTGRES_TEST_URL") or os.environ.get("DATABASE_URL")
        )
        if not db_url:
            raise KeyError(
                "Critical: DATABASE_URL or POSTGRES_TEST_URL must be set in the environment."
            )
        # Enable aggressive, pessimistic pool connection testing
        _engine = create_engine(db_url, pool_pre_ping=True, pool_size=20, max_overflow=20)
    return _engine


@contextmanager
def get_db_connection() -> Generator[Connection, None, None]:
    """Yield a new SQLAlchemy Connection with auto-close semantics."""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            engine: Engine = _get_engine()
            with engine.connect() as conn:
                yield conn
                return
        except OperationalError as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(retry_delay)
