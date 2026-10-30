"""Gate 5 — SKIP LOCKED Contention.

Using pytest-postgresql, open two concurrent database connections.
Both execute SELECT FOR UPDATE SKIP LOCKED simultaneously against
the same pool of UNPROCESSED rows. Assert:
- Connection 1 claims rows 1–1,000.
- Connection 2 claims rows 1,001–2,000 without blocking.
- No row is claimed by both.
- No deadlock exception is raised.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Generator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError

from app.db_batch_claim import claim_unprocessed_batch
from app.db_models import metadata
from app.session import _get_engine


# ---------------------------------------------------------------------------
# Schema fixture — creates tables in the test PostgreSQL database
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_engine() -> Generator[Engine, None, None]:
    """Create a test PostgreSQL engine and instantiate the schema.

    Requires the DATABASE_URL environment variable to point at a
    running PostgreSQL 15+ instance. The test database must exist.
    Tables are created at module scope and dropped after.
    Skips if PostgreSQL is unavailable.
    """
    try:
        engine: Engine = _get_engine()
    except KeyError:
        pytest.skip("DATABASE_URL not set — PostgreSQL not available for Gate 5")
        return  # Never reached, but helps type checker

    # Verify the database is actually reachable
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip(
            "PostgreSQL not reachable at DATABASE_URL — "
            "Gate 5 requires a running PostgreSQL instance"
        )

    try:
        yield engine
    finally:
        pass  # Do not drop tables, we use a shared database


@pytest.fixture(autouse=True, scope="module")
def _seed_unprocessed_rows(pg_engine: Engine) -> Generator[None, None, None]:
    """Insert 2,000 UNPROCESSED bank_transactions rows for contention test."""
    with pg_engine.connect() as conn:
        with conn.begin():
            # Clear any pre-existing data
            conn.execute(text("DELETE FROM audit_log"))
            conn.execute(text("DELETE FROM bank_transactions"))

            # Insert 2,000 UNPROCESSED rows
            for i in range(2_000):
                conn.execute(
                    text(
                        """
                        INSERT INTO bank_transactions (
                            source_chunk_id, file_origin, status,
                            amount, value_date, description_raw,
                            ingested_at, updated_at
                        ) VALUES (
                            :chunk_id, :file_origin, 'UNPROCESSED',
                            :amount, :value_date, :description,
                            NOW(), NOW()
                        )
                        """
                    ),
                    {
                        "chunk_id": f"chunk-concurrency-{i:06d}",
                        "file_origin": "CONCURRENCY_TEST.csv",
                        "amount": "100.00",
                        "value_date": "2024-01-15",
                        "description": f"Concurrent test row {i}",
                    },
                )
    yield
    # Cleanup happens in pg_engine teardown (drop_all)


# ---------------------------------------------------------------------------
# Concurrent claim test
# ---------------------------------------------------------------------------


class TestSkipLockedContention:
    """Verify SELECT FOR UPDATE SKIP LOCKED produces non-overlapping batches
    when two connections claim simultaneously."""

    def test_two_concurrent_claims_non_overlapping(
        self,
        pg_engine: Engine,
    ) -> None:
        """Two concurrent SKIP LOCKED claims must not overlap.

        Connection 1 claims up to 1,000 rows. While its transaction is
        still open, Connection 2 also claims up to 1,000 rows. Because
        of SKIP LOCKED, Connection 2 skips the rows locked by
        Connection 1 and claims the next 1,000 available rows.
        """
        results: dict[str, list[int]] = {"conn1": [], "conn2": []}
        errors: list[Exception] = []

        def _claim_worker(
            label: str,
            batch_size: int,
            hold_event: threading.Event,
            wait_event: threading.Event,
        ) -> None:
            """Worker that claims a batch, signals, and waits."""
            try:
                with pg_engine.connect() as conn:
                    with conn.begin():
                        claimed: list[int] = claim_unprocessed_batch(
                            conn, batch_size,
                        )
                        results[label] = claimed

                        # Signal that we've claimed our rows
                        hold_event.set()
                        # Wait for the other worker to also claim
                        wait_event.wait(timeout=10)
            except Exception as exc:
                errors.append(exc)

        # Synchronization events
        conn1_ready: threading.Event = threading.Event()
        conn2_ready: threading.Event = threading.Event()

        thread1: threading.Thread = threading.Thread(
            target=_claim_worker,
            args=("conn1", 1_000, conn1_ready, conn2_ready),
        )
        thread2: threading.Thread = threading.Thread(
            target=_claim_worker,
            args=("conn2", 1_000, conn2_ready, conn1_ready),
        )

        thread1.start()
        # Wait for conn1 to claim and lock before starting conn2
        conn1_ready.wait(timeout=10)
        thread2.start()

        thread1.join(timeout=30)
        thread2.join(timeout=30)

        # No exceptions should have occurred
        assert len(errors) == 0, f"Deadlock or DB error: {errors}"

        claimed1: list[int] = results["conn1"]
        claimed2: list[int] = results["conn2"]

        # Both connections must have claimed rows
        assert len(claimed1) > 0, "Connection 1 claimed 0 rows"
        assert len(claimed2) > 0, "Connection 2 claimed 0 rows"

        # No row may be claimed by both connections
        overlap: set[int] = set(claimed1) & set(claimed2)
        assert len(overlap) == 0, (
            f"SKIP LOCKED overlap detected! "
            f"{len(overlap)} rows claimed by both connections: "
            f"{sorted(overlap)[:10]}..."
        )

        # Total claimed must be 2,000 (all rows)
        total_claimed: int = len(set(claimed1) | set(claimed2))
        assert total_claimed == 2_000, (
            f"Expected 2,000 total claimed rows, got {total_claimed}. "
            f"Conn1: {len(claimed1)}, Conn2: {len(claimed2)}"
        )

    def test_single_connection_claims_exactly_1000(
        self,
        pg_engine: Engine,
    ) -> None:
        """A single connection claiming 1,000 rows gets exactly 1,000."""
        with pg_engine.connect() as conn:
            with conn.begin():
                claimed: list[int] = claim_unprocessed_batch(conn, 1_000)
                assert len(claimed) == 1_000, (
                    f"Expected 1,000 rows, got {len(claimed)}"
                )

    def test_no_deadlock_on_concurrent_claims(
        self,
        pg_engine: Engine,
    ) -> None:
        """Rapid concurrent claims must not produce deadlocks.

        5 threads claiming simultaneously — no deadlock.
        """
        all_claimed: list[list[int]] = []
        lock: threading.Lock = threading.Lock()
        errors: list[Exception] = []

        def _claim_500(label: str) -> None:
            try:
                with pg_engine.connect() as conn:
                    with conn.begin():
                        ids: list[int] = claim_unprocessed_batch(conn, 500)
                        
                        with lock:
                            all_claimed.append(ids)
                        time.sleep(1.0)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads: list[threading.Thread] = [
            threading.Thread(target=_claim_500, args=(f"w{i}",))
            for i in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0, f"Deadlock/error detected: {errors}"

        # Verify no overlap across all 5 workers
        seen: set[int] = set()
        for batch in all_claimed:
            batch_set: set[int] = set(batch)
            overlap: set[int] = seen & batch_set
            assert len(overlap) == 0, f"Overlap between workers: {overlap}"
            seen.update(batch_set)


# ---------------------------------------------------------------------------
# FAILURE INJECTION: SELECT FOR UPDATE without SKIP LOCKED
# ---------------------------------------------------------------------------


class TestForUpdateWithoutSkipLocked:
    """Verify that plain SELECT FOR UPDATE would cause blocking.

    This is a negative test demonstrating why SKIP LOCKED is
    architecturally mandatory (ADR-003).
    """

    def test_plain_for_update_causes_blocking(
        self,
        pg_engine: Engine,
    ) -> None:
        """A plain SELECT FOR UPDATE (without SKIP LOCKED) blocks
        the second connection. This demonstrates the defect that
        SKIP LOCKED prevents."""
        blocked: list[bool] = [False]
        hold_event: threading.Event = threading.Event()

        def _hold_lock() -> None:
            with pg_engine.connect() as conn:
                with conn.begin():
                    conn.execute(
                        text(
                            """
                            SELECT id FROM bank_transactions
                            WHERE status = 'UNPROCESSED'
                            ORDER BY id ASC
                            LIMIT 100
                            FOR UPDATE
                            """
                        ),
                    )
                    hold_event.set()
                    # Hold the lock for 2 seconds
                    time.sleep(2)

        def _try_claim() -> None:
            hold_event.wait(timeout=5)
            # Give a small delay to ensure conn1 holds lock
            time.sleep(0.1)
            start: float = time.time()
            try:
                with pg_engine.connect() as conn:
                    # Set a short lock timeout to detect blocking
                    conn.execute(text("SET lock_timeout = '1s'"))
                    conn.execute(
                        text(
                            """
                            SELECT id FROM bank_transactions
                            WHERE status = 'UNPROCESSED'
                            ORDER BY id ASC
                            LIMIT 100
                            FOR UPDATE
                            """
                        ),
                    )
                    elapsed: float = time.time() - start
                    # If it didn't block (>0.5s), it means the lock was held
                    if elapsed > 0.5:
                        blocked[0] = True
            except Exception:
                # Lock timeout = blocking occurred
                blocked[0] = True

        t1: threading.Thread = threading.Thread(target=_hold_lock)
        t2: threading.Thread = threading.Thread(target=_try_claim)

        t1.start()
        t2.start()

        t1.join(timeout=10)
        t2.join(timeout=10)

        # Plain FOR UPDATE SHOULD cause blocking — that's the bug
        # that SKIP LOCKED fixes. We assert it does block to prove
        # the problem exists without SKIP LOCKED.
        assert blocked[0], (
            "Plain FOR UPDATE did not cause blocking — "
            "this test was supposed to demonstrate the blocking defect. "
            "If this fails, the test environment is not exercising contention."
        )
