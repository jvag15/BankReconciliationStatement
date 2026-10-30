"""LightGBM risk scoring with generator-based batching."""

from __future__ import annotations

from typing import Any, Generator

import numpy as np

_BATCH_SIZE = 1_000


def run_risk_scoring_stream(
    transaction_ids: list[int],
    feature_matrix: np.ndarray,
) -> Generator[list[dict[str, Any]], None, None]:
    """
    Score transactions for anomaly risk using the LightGBM Booster.

    Never passes the full feature matrix to ``model.predict()`` at
    once — batch size is capped at 1,000 rows (ADR-010).

    ``feature_matrix`` shape: ``(N, num_features)`` — float32 preferred
    for memory efficiency.
    """
    from app.ml_worker_init import get_risk_model

    model: Any = get_risk_model()
    n_rows: int = len(transaction_ids)

    for offset in range(0, n_rows, _BATCH_SIZE):
        batch_ids: list[int] = transaction_ids[offset:offset + _BATCH_SIZE]
        batch_features: np.ndarray = feature_matrix[offset:offset + _BATCH_SIZE]

        scores: np.ndarray = model.predict(batch_features)

        results: list[dict[str, Any]] = [
            {
                "transaction_id": int(tx_id),
                "risk_score": float(score),
                "status": "SCORED",
            }
            for tx_id, score in zip(batch_ids, scores)
        ]

        yield results
        del batch_features, scores, results  # Aggressive GC between batches
