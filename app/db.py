"""Database layer for BRS Engine."""

import asyncpg
from app.config import settings

class _DB:
    pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=2,
                max_size=10,
            )

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

db = _DB()

async def get_db_pool() -> asyncpg.Pool:
    if db.pool is None:
        await db.connect()
    assert db.pool is not None
    return db.pool
