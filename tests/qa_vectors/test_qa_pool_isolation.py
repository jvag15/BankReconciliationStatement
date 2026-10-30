from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
import numpy as np

def test_connection_pool_isolation_and_silent_rollback_defenses() -> None:
    """Micro-Phase 3.1 & 3.2: Prevent connection pool starvation by strictly inheriting transactional context."""
    
    from app.task_inference import predict_ledger_accounts, InferencePipelineSingleton
    import scipy.sparse as sp

    # Mock DB read returning 1 row
    mock_df = MagicMock()
    mock_df.is_empty.return_value = False
    mock_df["description_raw"].to_list.return_value = ["dummy text"]
    
    mock_model = MagicMock()
    mock_model.num_feature.return_value = 20000
    mock_model.predict.return_value = np.zeros((1, 2))
    InferencePipelineSingleton._model = mock_model
    
    mock_encoder = MagicMock()
    mock_encoder.inverse_transform.return_value = np.array(["TEST_CAT"])
    InferencePipelineSingleton._encoder = mock_encoder
    
    mock_vectorizer = MagicMock()
    mock_vectorizer.transform.return_value = sp.csr_matrix(np.random.rand(1, 19500))

    with patch("app.task_inference.pl.read_database", return_value=mock_df), \
         patch("app.task_inference._clean_text_column", return_value=mock_df), \
         patch("app.task_inference.joblib.load", return_value=mock_vectorizer), \
         patch("app.task_inference.resolve_entity_mappings") as mock_resolve, \
         patch("app.task_inference.create_engine") as mock_create_engine, \
         patch("app.task_inference.Path.exists", return_value=True), \
         patch("requests.post"), \
         patch("app.task_inference.InferencePipelineSingleton.initialize"):

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Simulate engine.begin() context manager yielding a transactional_conn
        mock_transactional_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_transactional_conn
        
        # Make sure resolve_entity_mappings returns something we can select and iterate
        mock_resolved_df = MagicMock()
        mock_resolved_df.select.return_value.iter_rows.return_value = []
        mock_resolved_df.select.return_value.to_dicts.return_value = [{"test_key": "test_val"}]
        mock_resolve.return_value = mock_resolved_df
        
        # Execute the batch inference
        predict_ledger_accounts()
        
        # Phase 3.2: Assert resolve_entity_mappings was called exactly with the transactional_conn
        mock_resolve.assert_called_once()
        args, kwargs = mock_resolve.call_args
        
        # The second argument must be the mock_transactional_conn (not mock_engine or raw connect)
        assert args[1] is mock_transactional_conn
        assert mock_engine.connect.call_count == 1  # The initial select query
        assert mock_engine.begin.call_count == 1    # The transactional block
