"""Generator-based NLP inference for BRS Engine."""

from __future__ import annotations

from typing import Any, Generator

_BATCH_SIZE = 1_000


def run_nlp_extraction_stream(
    transaction_ids: list[int],
    raw_descriptions: list[str],
) -> Generator[list[dict[str, Any]], None, None]:
    """
    Extract banking reference entities from transaction descriptions.

    Yields batches of extraction results.  Each batch is flushed to
    PostgreSQL by the caller before the next batch is processed.
    Maximum batch size: 1,000 rows (ADR-010).
    """
    from app.ml_worker_init import get_nlp_pipeline

    nlp: Any = get_nlp_pipeline()

    for offset in range(0, len(raw_descriptions), _BATCH_SIZE):
        batch_ids: list[int] = transaction_ids[offset:offset + _BATCH_SIZE]
        batch_texts: list[str] = raw_descriptions[offset:offset + _BATCH_SIZE]

        results: list[dict[str, Any]] = []
        for tx_id, doc in zip(
            batch_ids, nlp.pipe(batch_texts, batch_size=_BATCH_SIZE)
        ):
            entity_map: dict[str, str] = {
                ent.label_: ent.text for ent in doc.ents
            }
            results.append(
                {
                    "transaction_id": tx_id,
                    "utr": entity_map.get("UTR"),
                    "neft_ref": entity_map.get("NEFT_REF"),
                    "rtgs_ref": entity_map.get("RTGS_REF"),
                    "imps_ref": entity_map.get("IMPS_REF"),
                    "upi_ref": entity_map.get("UPI_REF"),
                    "ifsc": entity_map.get("IFSC"),
                    "cin": entity_map.get("CIN"),
                    "pan": entity_map.get("PAN"),
                }
            )

        yield results
        del results  # Explicit GC — caller has already flushed to PostgreSQL.
