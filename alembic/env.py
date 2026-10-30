"""Alembic environment configuration for BRS Engine.

Database URL is injected from the DATABASE_URL environment variable
via env.py — never hardcoded (CLAUDE.md Secrets & Cryptographic Mandate).
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.db_models import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

db_url: str = os.environ.get("DATABASE_URL", "")
if db_url:
    config.set_main_option("sqlalchemy.url", db_url)

target_metadata = metadata


def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name and name.startswith("celery_"):
        return False
    return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (SQL generation only)."""
    url: str = config.get_main_option("sqlalchemy.url", "")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connected to database)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()