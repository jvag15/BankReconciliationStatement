"""Tests for the LightGBM matching classifier training layer."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
from scipy import sparse

from app.model_classifier import train_matching_model


def _build_sparse_matrix(row_count: int) -> sparse.csr_matrix:
    dense = np.zeros((row_count, 6), dtype=np.float32)
    for row_index in range(row_count):
        dense[row_index, row_index % 6] = 1.0
        dense[row_index, (row_index + 1) % 6] = float(row_index % 3)
    return sparse.csr_matrix(dense)


def test_train_matching_model_writes_artifacts_and_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    X_tfidf = _build_sparse_matrix(30)
    y_labels = ["vendor_payment", "tax_payment", "bank_fee"] * 10

    train_matching_model(X_tfidf, y_labels, artifact_dir=str(tmp_path))

    assert (tmp_path / "lgbm_matcher.bin").is_file()
    assert (tmp_path / "label_encoder.pkl").is_file()
    stdout = capsys.readouterr().out
    assert "precision" in stdout
    assert "recall" in stdout
    assert "f1-score" in stdout


def test_train_matching_model_routes_failures_to_dlq(tmp_path: Path) -> None:
    X_tfidf = _build_sparse_matrix(4)
    y_labels = ["vendor_payment", "tax_payment"]

    with patch("app.db_dlq._write_to_dlq") as mock_dlq:
        with pytest.raises(ValueError):
            train_matching_model(X_tfidf, y_labels, artifact_dir=str(tmp_path))

    assert mock_dlq.called
    call_kwargs = mock_dlq.call_args.kwargs
    assert call_kwargs["file_origin"] == str(tmp_path)
    assert call_kwargs["worker_id"] == "train_matching_model"
    assert call_kwargs["exception_class"] == "ValueError"
    assert "Traceback" in call_kwargs["stack_trace"]


def test_predict_transaction_routing_fallback_when_missing(tmp_path: Path) -> None:
    """Verify that predict_transaction_routing returns fallback category if files are missing."""
    from app.task_inference import predict_transaction_routing

    # Use patch to mock artifacts folder path so we guarantee it cannot find files
    with patch("os.path.join", return_value=str(tmp_path / "nonexistent_file")):
        category, confidence = predict_transaction_routing("TEST PAYMENT")
        assert category == "Office Contingencies"
        assert confidence == 0.50


def test_predict_transaction_routing_success() -> None:
    """Verify that predict_transaction_routing runs on actual artifacts."""
    from app.task_inference import predict_transaction_routing

    # Test on a known narrative from the dataset or any standard text
    category, confidence = predict_transaction_routing("neft salary payout")
    assert isinstance(category, str)
    assert isinstance(confidence, float)
    assert 0.0 <= confidence <= 1.0
