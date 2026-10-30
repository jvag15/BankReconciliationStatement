"""Centralised, cross-platform resolution of the encrypted upload spool.

Every component that reads or writes the encrypted chunk spool must resolve
its directory through :func:`get_storage_dir` so there is exactly one source
of truth.

Historical scar
---------------
``app/main.py`` previously hardcoded ``Path("E:/bank-statement/var/uploads")``.
On a Linux container a Windows drive-letter path is not absolute, so it
resolved *relative* to the working directory and silently created a nested
``./E:/bank-statement/var/uploads`` tree. The API wrote ciphertext into the
real spool while the worker looked in the phantom one (or vice versa),
producing ``FileNotFoundError`` on every ingest.

The directory is driven by ``STORAGE_DIR``. In Docker Compose it is set to
``/app/var/uploads`` — the path backed by the ``./var:/app/var`` bind mount
that is shared between ``brs_web`` and ``brs_worker``, so a file written by
one service is readable by the other. Outside a container it defaults to a
local temp directory.
"""

from __future__ import annotations

import os
from pathlib import Path

_DEFAULT_STORAGE_DIR = "/tmp/brs_storage"


def get_storage_dir() -> Path:
    """Return the encrypted upload spool directory, creating it if absent.

    Resolved from ``STORAGE_DIR`` (default :data:`_DEFAULT_STORAGE_DIR`).
    The path is always returned resolved and absolute so downstream
    traversal checks compare like against like.
    """
    raw = os.environ.get("STORAGE_DIR", "").strip() or _DEFAULT_STORAGE_DIR
    path = Path(raw).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_model_dir() -> Path:
    """Return the directory holding trained ML artifacts, creating it if absent.

    Resolved from ``MODEL_DIR``; when unset it defaults to a ``models``
    directory beside the upload spool so both live on the volume shared
    between ``brs_web`` and ``brs_worker`` — a retrain performed on the
    worker is then immediately visible to inference. This is the single
    source of truth for ``lgbm_matcher.bin``, ``label_encoder.pkl`` and
    ``tfidf_vectorizer.pkl``.
    """
    raw = os.environ.get("MODEL_DIR", "").strip()
    path = Path(raw).resolve() if raw else (get_storage_dir().parent / "models")
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()
