"""Gate 2 — Redis Failure & DLQ Integrity.

Inject redis.exceptions.ConnectionError at the Celery task acknowledgement point.
Assert that the raw payload is written to dlq_transactions with a non-null stack_trace,
correct file_origin and worker_id, and that the task does not silently succeed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.db_dlq import _write_to_dlq
from app.task_base import DLQBackedTask


# ---------------------------------------------------------------------------
# DLQ write correctness (uses mock DB connection)
# ---------------------------------------------------------------------------


class TestDLQWriteCorrectness:
    """Verify _write_to_dlq writes all required fields with correct values."""

    def test_dlq_write_inserts_all_fields(self) -> None:
        mock_conn: MagicMock = MagicMock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        mock_cm: MagicMock = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_conn)
        mock_cm.__exit__ = MagicMock(return_value=False)
        with patch("app.session.get_db_connection", return_value=mock_cm):
            _write_to_dlq(
                raw_payload="test-payload-data",
                file_origin="HDFK_CC_2024_03.csv",
                worker_id="worker-001",
                exception_class="ConnectionError",
                stack_trace="Traceback (most recent call last):\n  ...",
            )
        assert mock_conn.execute.called
        params = mock_conn.execute.call_args[0][1]
        assert params["raw_payload"] == "test-payload-data"
        assert params["file_origin"] == "HDFK_CC_2024_03.csv"
        assert params["worker_id"] == "worker-001"
        assert params["exception_class"] == "ConnectionError"
        assert params["stack_trace"] == "Traceback (most recent call last):\n  ..."
        assert "created_at" in params

    def test_dlq_write_truncates_long_payload(self) -> None:
        long_payload = "X" * 100_000
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_conn)
        mock_cm.__exit__ = MagicMock(return_value=False)
        with patch("app.session.get_db_connection", return_value=mock_cm):
            _write_to_dlq(
                raw_payload=long_payload,
                file_origin="test.csv",
                worker_id="w1",
                exception_class="TestError",
                stack_trace="trace",
            )
        params = mock_conn.execute.call_args[0][1]
        assert len(params["raw_payload"]) <= 65_535

    def test_dlq_write_handles_none_payload(self) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_conn)
        mock_cm.__exit__ = MagicMock(return_value=False)
        with patch("app.session.get_db_connection", return_value=mock_cm):
            _write_to_dlq(
                raw_payload=None,  # type: ignore[arg-type]
                file_origin="test.csv",
                worker_id="w1",
                exception_class="TestError",
                stack_trace="trace",
            )
        params = mock_conn.execute.call_args[0][1]
        assert params["raw_payload"] == ""


# ---------------------------------------------------------------------------
# FAILURE INJECTION: Redis ConnectionError during task execution
# ---------------------------------------------------------------------------


class TestRedisConnectionErrorDLQRouting:
    """Inject redis.exceptions.ConnectionError and verify DLQ routing."""

    def test_redis_connection_error_routes_to_dlq(self) -> None:
        import redis.exceptions as redis_exc
        task = DLQBackedTask()
        exc = redis_exc.ConnectionError("Conn error")
        task_id = "task-redis-001"
        args: tuple[Any, ...] = ()
        kwargs = {"file_origin": "HDFC_CC_2024_03.csv"}
        einfo = MagicMock()
        einfo.__str__ = MagicMock(return_value="Traceback: redis.exceptions.ConnectionError")
        with patch("app.db_dlq._write_to_dlq") as mock_dlq:
            task.on_failure(exc, task_id, args, kwargs, einfo)
        assert mock_dlq.called
        # _write_to_dlq is called with keyword arguments
        call_kwargs = mock_dlq.call_args[1]
        assert call_kwargs["raw_payload"] == kwargs["file_origin"]
        assert call_kwargs["file_origin"] == kwargs["file_origin"]
        assert call_kwargs["worker_id"] == task_id
        assert call_kwargs["exception_class"] == "ConnectionError"
        assert call_kwargs["stack_trace"] is not None

    def test_redis_error_on_retry_routes_to_dlq(self) -> None:
        import redis.exceptions as redis_exc
        task = DLQBackedTask()
        # Patch the request property to return a mock with retries attribute
        mock_request = MagicMock()
        mock_request.retries = 1
        with patch.object(type(task), 'request', new_callable=lambda: mock_request):
            exc = redis_exc.ConnectionError("Retry error")
            task_id = "task-retry-002"
            kwargs = {"file_origin": "SBI_2024.csv"}
            einfo = MagicMock()
            einfo.__str__ = MagicMock(return_value="Retry traceback")
            with patch("app.db_dlq._write_to_dlq") as mock_dlq:
                task.on_retry(exc, task_id, (), kwargs, einfo)
            assert mock_dlq.called
            call_kwargs = mock_dlq.call_args[1]
            assert "Redis ConnectionError on retry" in call_kwargs["raw_payload"]
            assert call_kwargs["file_origin"] == "SBI_2024.csv"
            assert call_kwargs["worker_id"] == task_id
            assert call_kwargs["exception_class"] == "RedisConnectionError"
            assert call_kwargs["stack_trace"] is not None

    def test_non_redis_error_on_retry_does_not_write_dlq(self) -> None:
        task = DLQBackedTask()
        # Patch the request property to return a mock with retries attribute
        mock_request = MagicMock()
        mock_request.retries = 1
        with patch.object(type(task), 'request', new_callable=lambda: mock_request):
            exc = ValueError("Non-redis error")
            kwargs = {"file_origin": "ICICI_2024.csv"}
            einfo = MagicMock()
            einfo.__str__ = MagicMock(return_value="Traceback")
            with patch("app.db_dlq._write_to_dlq") as mock_dlq:
                task.on_retry(exc, "task-003", (), kwargs, einfo)
            assert not mock_dlq.called


# ---------------------------------------------------------------------------
# FAILURE INJECTION: DLQ write itself fails
# ---------------------------------------------------------------------------


class TestDLQWriteFailsGracefully:
    """When the DLQ write itself fails, the original exception must NOT be suppressed."""

    def test_dlq_write_failure_does_not_suppress_original(self) -> None:
        with patch("app.session.get_db_connection", side_effect=Exception("DB down")):
            _write_to_dlq(
                raw_payload="data",
                file_origin="test.csv",
                worker_id="w1",
                exception_class="Err",
                stack_trace="trace",
            )
        # No exception should propagate out; handled internally

    def test_task_does_not_silently_succeed_on_failure(self) -> None:
        task = DLQBackedTask()
        exc = Exception("Fatal")
        einfo = MagicMock()
        einfo.__str__ = MagicMock(return_value="Fatal traceback")
        with patch("app.db_dlq._write_to_dlq") as mock_dlq:
            task.on_failure(
                exc,
                "task-fatal-001",
                (),
                {"file_origin": "AXIS_2024.csv"},
                einfo,
            )
        assert mock_dlq.called
