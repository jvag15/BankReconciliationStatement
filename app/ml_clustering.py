"""HDBSCAN anomaly clustering."""

from __future__ import annotations

from typing import Any

import numpy as np


def run_anomaly_clustering(
    feature_matrix: np.ndarray,
    transaction_ids: list[int],
) -> list[dict[str, Any]]:
    """
    Cluster transaction feature vectors using HDBSCAN.

    Cluster label ``-1`` = noise point = anomaly candidate for manual
    review.

    Note: HDBSCAN is not a streaming algorithm.  The full feature
    matrix for the current batch must be passed at once.  Limit batch
    size upstream to respect the memory budget (see ``PATTERN 3`` in
    ``polars_processing.md``).
    """
    from app.ml_worker_init import get_clusterer

    clusterer: Any = get_clusterer()
    labels: np.ndarray = clusterer.fit_predict(feature_matrix)

    return [
        {
            "transaction_id": int(tx_id),
            "cluster_id": int(label),
            "is_anomaly": bool(label == -1),
        }
        for tx_id, label in zip(transaction_ids, labels)
    ]
