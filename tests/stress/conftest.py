"""Gating for the stress suite.

Every test module here declares ``pytestmark = pytest.mark.stress`` (a
``pytestmark`` in a conftest does NOT apply to test modules, so it must live
in each module), which the `addopts` in pyproject.toml deselects by default.
This conftest adds a second guard: even under an explicit ``pytest -m
stress``, the suite is skipped unless RUN_STRESS=1, so a run in an
environment without a full live stack skips cleanly rather than erroring on a
missing DB/broker.
"""

from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config, items):  # noqa: ANN001
    if os.environ.get("RUN_STRESS") == "1":
        return
    skip = pytest.mark.skip(reason="stress suite: set RUN_STRESS=1 to run")
    for item in items:
        if "stress" in item.keywords:
            item.add_marker(skip)
