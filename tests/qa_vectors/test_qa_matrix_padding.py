import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import scipy.sparse as sp

from app.task_inference import predict_ledger_accounts, InferencePipelineSingleton

def test_lgbm_vocabulary_dimensionality_boundary() -> None:
    """Micro-Phase 2.1 & 2.2: Mock inference pipeline to enforce sparse matrix padding to 20000 columns."""
    
    # 100 rows, 19500 columns
    dummy_matrix = sp.csr_matrix(np.random.rand(100, 19500))
    
    mock_model = MagicMock()
    mock_model.num_feature.return_value = 20000
    mock_model.predict.return_value = np.zeros((100, 2))
    
    mock_encoder = MagicMock()
    mock_encoder.inverse_transform.return_value = np.array(["TEST_CAT"] * 100)
    
    mock_vectorizer = MagicMock()
    mock_vectorizer.transform.return_value = dummy_matrix
    
    InferencePipelineSingleton._model = mock_model
    InferencePipelineSingleton._encoder = mock_encoder
    
    with patch("app.task_inference.pl.read_database") as mock_read_db, \
         patch("app.task_inference._clean_text_column") as mock_clean, \
         patch("app.task_inference.joblib.load", return_value=mock_vectorizer), \
         patch("app.task_inference.resolve_entity_mappings") as mock_resolve, \
         patch("app.task_inference.create_engine"), \
         patch("requests.post"), \
         patch("app.task_inference.sp.hstack", wraps=sp.hstack) as mock_hstack:
         
        # Ensure we bypass early exits
        mock_df = MagicMock()
        mock_df.is_empty.return_value = False
        mock_df["description_raw"].to_list.return_value = ["dummy text"] * 100
        mock_read_db.return_value = mock_df
        mock_clean.return_value = mock_df
        
        # Ensure resolve_entity_mappings returns something iterable for the update block
        mock_resolved_df = MagicMock()
        mock_resolved_df.select.return_value.iter_rows.return_value = []
        mock_resolved_df.select.return_value.to_dicts.return_value = [{"test_key": "test_val"}]
        mock_resolve.return_value = mock_resolved_df
        
        # Mock paths so it doesn't fail on missing artifacts
        with patch("app.task_inference.Path.exists", return_value=True), \
             patch("app.task_inference.InferencePipelineSingleton.initialize"):
            predict_ledger_accounts()
            
    # Assert model was called and feature checks performed
    mock_model.num_feature.assert_called()
    
    # Assert hstack was utilized with format="csr"
    mock_hstack.assert_called_once()
    assert mock_hstack.call_args.kwargs.get("format") == "csr"
    
    # Verify the matrix shape passed to predict matches exactly 20000
    predict_args = mock_model.predict.call_args[0][0]
    assert predict_args.shape[1] == 20000
    assert sp.issparse(predict_args)
