"""LightGBM classifier training for ledger matching."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any
import sys
import traceback
import warnings

import joblib
from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

import app.db_dlq as dlq

_MODEL_FILENAME = "lgbm_matcher.bin"
_ENCODER_FILENAME = "label_encoder.pkl"
_WORKER_ID = "train_matching_model"


def _matrix_row_count(X_tfidf: Any) -> int:
    shape = getattr(X_tfidf, "shape", None)
    if not isinstance(shape, tuple) or not shape:
        raise ValueError("X_tfidf must expose a non-empty shape tuple.")
    row_count = shape[0]
    if not isinstance(row_count, int):
        raise ValueError("X_tfidf row count must be an integer.")
    return row_count


def _route_training_failure(
    exc: Exception,
    X_tfidf: Any,
    y_labels: list[str],
    artifact_dir: str,
) -> None:
    stack_trace = traceback.format_exc()
    shape = getattr(X_tfidf, "shape", "unknown")
    print(
        f"{_WORKER_ID} failed: {type(exc).__name__}: {exc}",
        file=sys.stderr,
    )
    dlq._write_to_dlq(
        raw_payload=(
            f"artifact_dir={artifact_dir}; matrix_shape={shape}; "
            f"label_count={len(y_labels)}"
        ),
        file_origin=artifact_dir,
        worker_id=_WORKER_ID,
        exception_class=type(exc).__name__,
        stack_trace=stack_trace,
    )


def train_matching_model(
    X_tfidf: Any,
    y_labels: list[str],
    artifact_dir: str = "E:\\bank-statement\\var\\artifacts",
) -> None:
    """Train, evaluate, and serialize the LightGBM ledger-matching classifier."""
    try:
        row_count = _matrix_row_count(X_tfidf)
        if row_count != len(y_labels):
            raise ValueError(
                "Feature matrix row count must match label count: "
                f"X_tfidf has {row_count} rows, y_labels has {len(y_labels)} labels."
            )
        if not y_labels:
            raise ValueError("y_labels must contain at least one label.")

        encoder = LabelEncoder()
        y_encoded = encoder.fit_transform(y_labels)

        class_counts = Counter(y_encoded)
        min_class_count = min(class_counts.values())
        class_count = len(encoder.classes_)

        if min_class_count >= 2:
            sys.stdout.write("Executing stratified training split...\n")
            X_train, X_valid, y_train, y_valid = train_test_split(
                X_tfidf, y_encoded, test_size=0.20, stratify=y_encoded, random_state=42
            )
        else:
            sys.stdout.write(
                "Sparse data classes detected (count < 2). Falling back to random split...\n"
            )
            X_train, X_valid, y_train, y_valid = train_test_split(
                X_tfidf, y_encoded, test_size=0.20, random_state=42
            )

        import os
        from sklearn.model_selection import RandomizedSearchCV

        tuning_iterations = int(os.environ.get("HYPERPARAMETER_TUNING_ITERATIONS", 5))

        param_grid = {
            "num_leaves": [15, 31, 63, 127],
            "learning_rate": [0.01, 0.05, 0.1, 0.2],
            "feature_fraction": [0.6, 0.8, 1.0],
        }

        base_clf = LGBMClassifier(
            objective="multiclass",
            metric="multi_logloss",
            boosting_type="gbdt",
            max_depth=-1,
            verbosity=-1,
            n_jobs=-1,
            num_class=class_count,
            class_weight="balanced",
        )
        
        sys.stdout.write(f"Running hyperparameter tuning (RandomizedSearchCV) with n_iter={tuning_iterations}...\n")
        search = RandomizedSearchCV(
            estimator=base_clf,
            param_distributions=param_grid,
            n_iter=tuning_iterations,
            cv=3,
            scoring="neg_log_loss",
            random_state=42,
            n_jobs=-1,
        )
        
        search.fit(X_train, y_train)
        clf = search.best_estimator_
        sys.stdout.write(f"Best parameters found: {search.best_params_}\n")
        
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="X does not have valid feature names.*",
                category=UserWarning,
            )
            y_pred = clf.predict(X_valid, validate_features=False)
            
        report = classification_report(
            y_valid,
            y_pred,
            labels=range(class_count),
            target_names=[str(label) for label in encoder.classes_],
            zero_division=0,
        )
        print(report, file=sys.stdout)

        artifact_path = Path(artifact_dir)
        artifact_path.mkdir(parents=True, exist_ok=True)
        clf.booster_.save_model(str(artifact_path / _MODEL_FILENAME))
        joblib.dump(encoder, artifact_path / _ENCODER_FILENAME)

        del X_train, X_valid, y_train, y_valid, y_pred, report, clf, encoder

    except Exception as exc:
        _route_training_failure(exc, X_tfidf, y_labels, artifact_dir)
        raise