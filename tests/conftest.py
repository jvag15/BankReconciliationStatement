"""Shared test fixtures and configuration for BRS Engine test suite."""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Environment stubs — prevent KeyError on missing env vars at import time.
# These MUST run before any src/ module is imported.


@pytest.fixture(autouse=True, scope="session")
def _set_env_vars() -> None:
    """Inject minimal env vars so src/ modules can be imported safely."""
    env_defaults: dict[str, str] = {
        "DATABASE_URL": "postgresql+psycopg2://brs_user:brs_password@127.0.0.1:5432/brs_db",
        "REDIS_BROKER_URL": "redis://127.0.0.1:6379/0",
        "POSTGRES_RESULT_BACKEND_URL": (
            "db+postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db"
        ),
        "FERNET_KEY": "",  # Overridden per-test when Fernet is needed.
        "LIGHTGBM_MODEL_PATH": "",  # Stub — real model not needed in unit tests.
        "FASTTEXT_MODEL_PATH": "",
        "HDBSCAN_CACHE_DIR": "",
    }
    original: dict[str, str | None] = {}
    for key, default in env_defaults.items():
        original[key] = os.environ.get(key)
        if key not in os.environ:
            os.environ[key] = default
    yield
    for key, orig_value in original.items():
        if orig_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = orig_value


@pytest.fixture()
def fernet_key() -> str:
    """Provide a valid Fernet key for encryption tests."""
    from cryptography.fernet import Fernet

    key: str = Fernet.generate_key().decode()
    os.environ["FERNET_KEY"] = key
    yield key
    os.environ.pop("FERNET_KEY", None)
