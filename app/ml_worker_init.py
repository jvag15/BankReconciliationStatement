"""Worker-level model initialization for BRS Engine."""

from __future__ import annotations

import os
import sys
from typing import Any

from celery.signals import worker_init


_NLP_PIPELINE: Any = None
_RISK_MODEL: Any = None
_CATEGORIZATION_MODEL: Any = None
_CLUSTERER: Any = None


def _build_spacy_pipeline() -> Any:
    """Internal helper to build the spaCy pipeline."""
    import re

    import spacy
    from spacy.language import Language
    from spacy.util import compile_infix_regex

    nlp: Language = spacy.blank("en")

    # Patch tokenizer infixes — preserve hyphens and slashes
    default_infixes: list[Any] = list(nlp.Defaults.infixes or [])
    filtered_infixes: list[Any] = [
        pattern
        for pattern in default_infixes
        if not re.search(r"\\-|/", pattern)
    ]
    tokenizer: Any = nlp.tokenizer
    tokenizer.infix_finditer = compile_infix_regex(
        filtered_infixes,
    ).finditer

    ruler: Any = nlp.add_pipe("entity_ruler", first=True)
    ruler.add_patterns(_get_banking_entity_patterns())
    return nlp


def _get_banking_entity_patterns() -> list[dict[str, Any]]:
    """Return deterministic regex patterns for Indian banking references."""
    return [
        {
            "label": "UTR",
            "pattern": [{"TEXT": {"REGEX": r"^[A-Z0-9]{16,22}$"}}],
        },
        {
            "label": "NEFT_REF",
            "pattern": [{"TEXT": {"REGEX": r"^NEFT[A-Z0-9\-]{6,25}$"}}],
        },
        {
            "label": "RTGS_REF",
            "pattern": [{"TEXT": {"REGEX": r"^RTGS[A-Z0-9\-]{6,25}$"}}],
        },
        {
            "label": "IMPS_REF",
            "pattern": [{"TEXT": {"REGEX": r"^\d{12}$"}}],
        },
        {
            "label": "UPI_REF",
            "pattern": [{"TEXT": {"REGEX": r"^\d{12,18}$"}}],
        },
        {
            "label": "IFSC",
            "pattern": [{"TEXT": {"REGEX": r"^[A-Z]{4}0[A-Z0-9]{6}$"}}],
        },
        {
            "label": "CIN",
            "pattern": [
                {"TEXT": {"REGEX": r"^[LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$"}}
            ],
        },
        {
            "label": "PAN",
            "pattern": [{"TEXT": {"REGEX": r"^[A-Z]{5}\d{4}[A-Z]$"}}],
        },
    ]


@worker_init.connect  # type: ignore[untyped-decorator]
def load_models_on_worker_start(sender: Any, **kwargs: Any) -> None:
    """Run exactly once per Celery worker process on startup.

    Every model is loaded here, never inside a ``@shared_task`` body.

    Artifact-backed models (LightGBM, fastText) are loaded only when
    their path is configured *and* the file exists.  A missing artifact
    must not abort worker startup — that would take down ingestion and
    matching too, which need no ML at all.  The corresponding
    ``get_*()`` accessor still raises loudly if a task reaches for a
    model that never loaded.
    """
    global _NLP_PIPELINE, _RISK_MODEL, _CATEGORIZATION_MODEL, _CLUSTERER

    import hdbscan

    _NLP_PIPELINE = _build_spacy_pipeline()

    _RISK_MODEL = _load_optional_artifact("LIGHTGBM_MODEL_PATH", _load_lightgbm_model)

    _CATEGORIZATION_MODEL = _load_optional_artifact(
        "FASTTEXT_MODEL_PATH", _load_fasttext_model,
    )

    _CLUSTERER = hdbscan.HDBSCAN(
        min_cluster_size=5,
        min_samples=3,
        metric="euclidean",
        core_dist_n_jobs=-1,
        memory=os.environ.get("HDBSCAN_CACHE_DIR") or None,
        prediction_data=True,
    )


def _load_optional_artifact(env_var: str, loader: Any) -> Any:
    """Load an artifact-backed model, or return ``None`` with a warning.

    Returns ``None`` when *env_var* is unset/empty or points at a path
    that does not exist, so the worker can still boot and serve the
    non-ML tasks.
    """
    path: str = os.environ.get(env_var, "").strip()
    if not path:
        sys.stderr.write(
            f"WARNING: {env_var} is not set — dependent ML tasks will fail "
            f"on access until it is configured.\n"
        )
        return None
    if not os.path.exists(path):
        sys.stderr.write(
            f"WARNING: {env_var} points at a missing artifact ({path}) — "
            f"dependent ML tasks will fail on access.\n"
        )
        return None
    return loader(path)


def _load_lightgbm_model(path: str) -> Any:
    """Load the LightGBM Booster from *path*."""
    import lightgbm as lgb

    return lgb.Booster(model_file=path)


def _load_fasttext_model(path: str) -> Any:
    """Load fastText model from *path*.  Prefer the quantized ``.ftz``
    variant to reduce RSS by 60–80%."""
    import fasttext  # type: ignore[import-untyped,unused-ignore]
    return fasttext.load_model(path)


def get_nlp_pipeline() -> Any:
    """Return the worker-local spaCy NLP pipeline singleton."""
    if _NLP_PIPELINE is None:
        msg = "NLP pipeline accessed before worker initialization."
        raise RuntimeError(msg)
    return _NLP_PIPELINE


def get_risk_model() -> Any:
    """Return the worker-local LightGBM Booster singleton."""
    if _RISK_MODEL is None:
        msg = "LightGBM risk model accessed before worker initialization."
        raise RuntimeError(msg)
    return _RISK_MODEL


def get_categorization_model() -> Any:
    """Return the worker-local fastText model singleton."""
    if _CATEGORIZATION_MODEL is None:
        msg = "fastText categorization model accessed before worker initialization."
        raise RuntimeError(msg)
    return _CATEGORIZATION_MODEL


def get_clusterer() -> Any:
    """Return the worker-local HDBSCAN clusterer singleton."""
    if _CLUSTERER is None:
        msg = "HDBSCAN clusterer accessed before worker initialization."
        raise RuntimeError(msg)
    return _CLUSTERER
