"""Celery application configuration for BRS Engine."""

from __future__ import annotations

import os
import sys

from celery import Celery
from celery.schedules import crontab


def create_celery_app() -> Celery:
    """Create and configure the Celery application for BRS Engine."""
    # Dynamically detect if execution context is inside the Docker runtime space
    IS_DOCKER = os.path.exists('/.dockerenv')

    # Route to the container alias if inside Docker, fallback to host loopback if native
    DEFAULT_BROKER = "redis://brs_redis:6379/0" if IS_DOCKER else "redis://127.0.0.1:6379/0"
    DEFAULT_BACKEND = "redis://brs_redis:6379/0" if IS_DOCKER else "redis://127.0.0.1:6379/0"

    REDIS_BROKER = os.getenv('CELERY_BROKER_URL', DEFAULT_BROKER)
    REDIS_BACKEND = os.getenv('CELERY_RESULT_BACKEND', DEFAULT_BACKEND)

    # Dynamic worker concurrency: reserve 1 core for OS, 1 for Web/DB
    total_cores = os.cpu_count() or 4
    safe_concurrency = max(1, total_cores - 2)

    app: Celery = Celery(
        'bank_statement_pipeline',
        broker=REDIS_BROKER,
        backend=REDIS_BACKEND,
        include=[
            "app.task_ingestion",
            "app.task_ledger_ingestion",
            "app.task_dlq_recovery",
            "app.task_webhook",
            "app.task_retrain",
            "app.task_inference",
            "app.execute_unsupervised_analysis",
            "app.task_risk_scoring",
            "app.task_hitl_approval",
            "app.task_matching_engine",
            "app.task_nlp_extraction"
        ],
    )

    # Enforce explicit configurations directly onto the application instance object
    app.conf.update(
        broker_url=REDIS_BROKER,
        result_backend=REDIS_BACKEND,
        broker_transport_options={'visibility_timeout': 3600},
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        task_track_started=True,
        result_expires=86400,
        worker_pool="solo",
        worker_concurrency=safe_concurrency,
        task_default_queue="ingestion",
        task_routes={
            "brs_engine.tasks.process_statement_chunk": {"queue": "ingestion"},
            "brs_engine.tasks.process_ledger_chunk": {"queue": "ingestion"},
            "brs_engine.tasks.run_matching_engine": {"queue": "ingestion"},
            "brs_engine.tasks.recover_dlq_entries": {"queue": "ingestion"},
            "brs_engine.tasks.dispatch_reconciliation_webhooks": {"queue": "ingestion"},
            "retrain_lgbm_model": {"queue": "retrain"},
            "run_ml_audit": {"queue": "ml_predict"},
        }
    )

    app.conf.beat_schedule = {
        "retrain-lgbm-nightly": {
            "task": "retrain_lgbm_model",
            "schedule": crontab(hour=2, minute=0),
        },
        # Sweep the dead-letter queue every 5 minutes: re-enqueue transient
        # failures whose spool file survives, terminally fail the rest.
        "dlq-recovery-sweep": {
            "task": "brs_engine.tasks.recover_dlq_entries",
            "schedule": crontab(minute="*/5"),
        },
    }

    return app


celery_app: Celery = create_celery_app()
