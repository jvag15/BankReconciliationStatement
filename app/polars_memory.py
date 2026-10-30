"""Container-aware memory-pressure sensing for ingestion batch sizing.

The ingestion worker runs under a hard cgroup memory limit (4G in
docker-compose).  A fixed insert batch that is comfortable on an idle
worker can push an already-loaded worker over that limit and trigger an
OOM kill mid-chunk.  These helpers read the *container's* live memory
usage and derive a batch size that shrinks as pressure rises.

Detection order for both limit and usage:
  1. cgroup v2  (/sys/fs/cgroup/memory.max, memory.current)
  2. cgroup v1  (/sys/fs/cgroup/memory/memory.limit_in_bytes, .usage_in_bytes)
  3. psutil host figures (last resort, e.g. running outside a container)

``psutil`` is already a dependency; the cgroup reads are plain file I/O so
this module stays import-light and never fails the caller — every probe
degrades to a safe default rather than raising.
"""

from __future__ import annotations

import os

# Batch-size envelope. Base matches scripts/bulk_ingest.py's healthy default;
# the floor keeps forward progress even under severe pressure.
DEFAULT_BASE_CHUNK = 5_000
MIN_CHUNK = 500

_CGROUP_V2_MAX = "/sys/fs/cgroup/memory.max"
_CGROUP_V2_CURRENT = "/sys/fs/cgroup/memory.current"
_CGROUP_V1_LIMIT = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
_CGROUP_V1_USAGE = "/sys/fs/cgroup/memory/memory.usage_in_bytes"

# cgroup v1 reports "unlimited" as a huge sentinel; treat anything above this
# as "no real limit set" and fall back to host memory.
_UNLIMITED_SENTINEL = 1 << 62


def _read_int(path: str) -> int | None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
    except (OSError, ValueError):
        return None
    if raw == "max":  # cgroup v2 "no limit"
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if value <= 0 or value >= _UNLIMITED_SENTINEL:
        return None
    return value


def container_memory_limit_bytes() -> int:
    """Return the effective memory ceiling in bytes (cgroup, else host)."""
    for path in (_CGROUP_V2_MAX, _CGROUP_V1_LIMIT):
        value = _read_int(path)
        if value is not None:
            return value
    import psutil

    return int(psutil.virtual_memory().total)


def container_memory_usage_bytes() -> int:
    """Return current memory usage in bytes (cgroup, else host)."""
    for path in (_CGROUP_V2_CURRENT, _CGROUP_V1_USAGE):
        value = _read_int(path)
        if value is not None:
            return value
    import psutil

    return int(psutil.virtual_memory().used)


def memory_pressure_ratio() -> float:
    """Return used/limit clamped to [0.0, 1.0]. Higher means less headroom."""
    limit = container_memory_limit_bytes()
    if limit <= 0:
        return 0.0
    ratio = container_memory_usage_bytes() / limit
    if ratio < 0.0:
        return 0.0
    if ratio > 1.0:
        return 1.0
    return ratio


def get_adaptive_chunk_size(
    base_chunk: int = DEFAULT_BASE_CHUNK,
    min_chunk: int = MIN_CHUNK,
) -> int:
    """Return an insert batch size scaled down by current memory pressure.

    The tiers are deliberately coarse so the size is stable within a run
    and only steps down as the worker fills up:

        pressure < 0.60  -> full base chunk
        0.60 - 0.75      -> half
        0.75 - 0.85      -> quarter
        >= 0.85          -> min_chunk (bail to the floor)
    """
    # An explicit override wins, so operators can pin the size in tight envs.
    override = os.environ.get("INGEST_CHUNK_SIZE", "").strip()
    if override.isdigit() and int(override) > 0:
        return max(min_chunk, int(override))

    pressure = memory_pressure_ratio()
    if pressure < 0.60:
        size = base_chunk
    elif pressure < 0.75:
        size = base_chunk // 2
    elif pressure < 0.85:
        size = base_chunk // 4
    else:
        size = min_chunk

    return max(min_chunk, size)
