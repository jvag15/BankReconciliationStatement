"""Base Celery task class with automatic DLQ routing."""

from __future__ import annotations

from typing import Any

from celery import Task


class DLQBackedTask(Task):  # type: ignore[misc]
    """Base class for all BRS Engine Celery tasks.

    Provides automatic DLQ routing on fatal task failure and Redis
    connection errors during retry.
    """

    abstract = True

    def on_failure(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        """Called by Celery when a task exhausts all retries."""
        from app.db_dlq import _write_to_dlq

        file_origin: str = str(kwargs.get("file_origin", str(args)))
        _write_to_dlq(
            raw_payload=str(kwargs.get("file_origin", str(args))),
            file_origin=file_origin,
            worker_id=task_id,
            exception_class=type(exc).__name__,
            stack_trace=str(einfo),
        )

    def on_retry(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        """Called on each retry.  Logs Redis failures specifically."""
        import redis.exceptions as redis_exc

        if isinstance(exc, redis_exc.ConnectionError):
            from app.db_dlq import _write_to_dlq

            _write_to_dlq(
                raw_payload=f"Redis ConnectionError on retry {self.request.retries}",
                file_origin=str(kwargs.get("file_origin", "unknown")),
                worker_id=task_id,
                exception_class="RedisConnectionError",
                stack_trace=str(einfo),
            )
