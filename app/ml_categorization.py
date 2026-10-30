"""fastText categorization with generator-based batching."""

from __future__ import annotations

from typing import Any, Generator

_BATCH_SIZE = 1_000


def run_categorization_stream(
    transaction_ids: list[int],
    narration_texts: list[str],
) -> Generator[list[dict[str, Any]], None, None]:
    """
    Categorize transaction narrations using the fastText model.

    fastText's ``predict()`` operates on individual strings; batch
    orchestration is handled by the loop.  Maximum batch size: 1,000.
    """
    from app.ml_worker_init import get_categorization_model

    model: Any = get_categorization_model()

    for offset in range(0, len(narration_texts), _BATCH_SIZE):
        batch_ids: list[int] = transaction_ids[offset:offset + _BATCH_SIZE]
        batch_texts: list[str] = narration_texts[offset:offset + _BATCH_SIZE]

        results: list[dict[str, Any]] = []
        for tx_id, text in zip(batch_ids, batch_texts):
            labels: list[str]
            probs: list[float]
            labels, probs = model.predict(text.replace("\n", " "), k=1)
            results.append(
                {
                    "transaction_id": int(tx_id),
                    "category": (
                        labels[0].replace("__label__", "")
                        if labels
                        else "UNKNOWN"
                    ),
                    "category_score": float(probs[0]) if probs else 0.0,
                }
            )

        yield results
        del results
