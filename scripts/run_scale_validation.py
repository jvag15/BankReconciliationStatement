import gc
import os
import sys
from pathlib import Path

# Add project root to PYTHONPATH for docker execution
sys.path.insert(0, str(Path(__file__).parent.parent.absolute()))

from unittest.mock import patch

import joblib
import numpy as np
import polars as pl
import scipy.sparse as sp
from faker import Faker
from sqlalchemy import create_engine, text

from app.feature_extractor import _clean_text_column
from app.task_inference import InferencePipelineSingleton, predict_ledger_accounts

def setup_mock_db(df: pl.DataFrame):
    db_url = "sqlite:///mock.db"
    os.environ["DATABASE_URL"] = db_url
    engine = create_engine(db_url)
    
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE bank_transactions (
                id TEXT,
                description_raw TEXT,
                amount TEXT,
                category TEXT,
                confidence_score REAL,
                job_id TEXT
            )
            """
        ))
        
        conn.execute(
            text("INSERT INTO bank_transactions (id, description_raw, amount, category, job_id) VALUES (:id, :description_raw, :amount, :category, :job_id)"),
            df.to_dicts()
        )
    return engine

def main():
    Path("mock.db").unlink(missing_ok=True)
    fake = Faker('en_IN')
    Faker.seed(42)
    
    n = 100000
    print(f"Generating {n} synthetic rows...")
    
    data = {
        "id": [fake.uuid4() for _ in range(n)],
        "description_raw": [fake.company() + " " + fake.catch_phrase() for _ in range(n)],
        "amount": [str(round(fake.random.uniform(10, 10000), 2)) for _ in range(n)],
        "category": [None] * n,
        "job_id": ["job-123"] * n
    }
    
    df = pl.DataFrame(data)
    
    setup_mock_db(df)
    
    del df
    del data
    gc.collect()

    print("Executing ML pipeline...")
    
    original_read_database = pl.read_database
    
    def mocked_read_database(query, connection, *args, **kwargs):
        clean_query = query.replace("FOR UPDATE SKIP LOCKED", "")
        return original_read_database(clean_query, connection, *args, **kwargs)

    with patch("src.tasks.inference.pl.read_database", side_effect=mocked_read_database), \
         patch("src.tasks.inference.resolve_entity_mappings", lambda df_updates, conn: df_updates), \
         patch("src.tasks.inference._ARTIFACT_DIR", Path("var/artifacts").absolute()):
         
        predict_ledger_accounts()
    
    gc.collect()
    Path("mock.db").unlink(missing_ok=True)
    print("Memory profile complete.")

if __name__ == "__main__":
    main()
