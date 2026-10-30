"""Celery task for continuous ML retraining from human-in-the-loop feedback.

Nightly (via the ``retrain-lgbm-nightly`` beat entry) this harvests the
transactions an operator has manually re-categorised — ``match_rule_id =
'MANUAL_HITL'`` — and retrains the TF-IDF + LightGBM classifier that
``app/task_inference.py`` uses for automated categorisation.

The loop is closed end to end:

    HITL overrides  ->  TF-IDF vectoriser + LightGBM classifier
                    ->  artifacts written to the shared MODEL_DIR
                    ->  InferencePipelineSingleton.reload() hot-swaps them

Artifacts land in :func:`app.storage_paths.get_model_dir` — the same
directory the inference singleton reads — so a retrain performed on the
worker is immediately visible to inference without a restart.  Data
access uses Polars, not pandas.
"""

from __future__ import annotations

import os
import sys
from collections import Counter

from celery import shared_task

# The trainer does a stratified 80/20 split (test must hold >= n_classes rows)
# and then RandomizedSearchCV(cv=3) over the train side (needs >= 3 per class in
# train). >= 6 samples per class satisfies both comfortably for any class count.
_MIN_SAMPLES_PER_CLASS = 6


@shared_task(name="retrain_lgbm_model", queue="retrain")
def retrain_model() -> str:
    """Retrain the categorisation classifier from HITL-corrected rows."""
    import joblib
    import polars as pl
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sqlalchemy import create_engine, text

    from app.model_classifier import train_matching_model
    from app.storage_paths import get_model_dir

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return "DATABASE_URL not configured."

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("db+postgresql://"):
        db_url = db_url.replace("db+postgresql://", "postgresql+psycopg2://", 1)

    engine = create_engine(db_url)
    try:
        query = (
            "SELECT description_raw, category FROM bank_transactions "
            "WHERE match_rule_id = 'MANUAL_HITL' AND category IS NOT NULL"
        )
        with engine.connect() as conn:
            df = pl.read_database(query, connection=conn)
    finally:
        engine.dispose()

    if df.is_empty():
        return "No human-corrected data available for retraining."

    descriptions = [str(d or "") for d in df["description_raw"].to_list()]
    labels = [str(c) for c in df["category"].to_list()]

    class_counts = Counter(labels)
    if len(class_counts) < 2:
        return "Not enough diverse categories to retrain classification model."
    smallest = min(class_counts.values())
    if smallest < _MIN_SAMPLES_PER_CLASS:
        return (
            f"Insufficient per-class HITL samples (min {smallest}); "
            f"need >= {_MIN_SAMPLES_PER_CLASS} per category to retrain."
        )

    # Vectorise narratives and train. The vectoriser is persisted alongside the
    # model because inference (predict_ledger_accounts) loads it to transform
    # incoming descriptions into the same feature space.
    vectorizer = TfidfVectorizer(
        max_features=20_000,
        ngram_range=(1, 2),
        stop_words="english",
        min_df=1,
    )
    x_tfidf = vectorizer.fit_transform(descriptions)

    model_dir = get_model_dir()

    # Train first: writes lgbm_matcher.bin + label_encoder.pkl into model_dir,
    # or routes to the DLQ and re-raises on failure. Only persist the vectoriser
    # once training has succeeded, so a failure never leaves an orphan vectoriser
    # mismatched against the previous model.
    train_matching_model(x_tfidf, labels, artifact_dir=str(model_dir))
    joblib.dump(vectorizer, model_dir / "tfidf_vectorizer.pkl")

    # Hot-reload the in-process inference model so the new weights take effect
    # immediately for subsequent tasks on this (solo) worker.
    reloaded = False
    try:
        from app.task_inference import InferencePipelineSingleton

        InferencePipelineSingleton.reload()
        reloaded = True
    except Exception as exc:  # pragma: no cover - reload is best-effort
        sys.stderr.write(f"Model hot-reload failed post-retrain: {exc}\n")

    return (
        f"Retrained on {len(labels)} HITL records across "
        f"{len(class_counts)} categories into {model_dir}; "
        f"hot_reload={reloaded}."
    )
