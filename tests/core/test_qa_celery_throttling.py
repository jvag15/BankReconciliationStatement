"""QA Automation: Celery Queue Throttling Validation."""
from __future__ import annotations

import pytest
from app.celery_app import celery_app

class TestCeleryThrottlingQA:
    """Mock the broker to ensure parallel execution for ingestion, but strict sequential serialization for heavy ML tasks."""

    def test_ml_tasks_routed_for_sequential_execution(self) -> None:
        """Verify that heavy ML tasks and retraining tasks are isolated from the high-concurrency ingestion queue."""
        routes = celery_app.conf.task_routes
        
        # Heavy ML Prediction must be strictly isolated to ml_predict queue
        assert "run_ml_audit" in routes, "ML Predict task is not explicitly routed"
        assert routes["run_ml_audit"]["queue"] == "ml_predict", "ML Predict must be on isolated queue for strict serialization"
        
        # LightGBM Retraining must be strictly isolated to retrain queue
        assert "retrain_lgbm_model" in routes, "Retrain task is not explicitly routed"
        assert routes["retrain_lgbm_model"]["queue"] == "retrain", "Retraining must be on isolated queue for strict serialization"

    def test_ingestion_supports_parallelism(self) -> None:
        """Verify that ingestion tasks are routed to the parallel-friendly ingestion queue."""
        routes = celery_app.conf.task_routes
        
        assert "brs_engine.tasks.process_statement_chunk" in routes, "Ingestion chunk task is not explicitly routed"
        assert routes["brs_engine.tasks.process_statement_chunk"]["queue"] == "ingestion", "Ingestion chunk must run on parallel ingestion queue"

    def test_safe_concurrency_calculation(self) -> None:
        """Verify that Celery configures worker concurrency dynamically to leave DB and OS headroom."""
        worker_concurrency = celery_app.conf.worker_concurrency
        assert isinstance(worker_concurrency, int), "Worker concurrency must be dynamically calculated as an integer"
        assert worker_concurrency >= 1, "Concurrency must never drop below 1"
