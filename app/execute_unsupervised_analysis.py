import pandas as pd
import numpy as np
from celery import shared_task
from sqlalchemy import text
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from loguru import logger
import uuid

# We assume standard SQLAlchemy session management is available via the project's dependency injection
# For Celery tasks, we usually instantiate a local session. 
# We'll mock the session retrieval as typical for this template.
from app.session import get_db_connection

@shared_task(queue='ingestion')
def execute_unsupervised_analysis(job_id: str, *args, **kwargs):
    """
    Executes heavy unsupervised ML against ingested bank statements for a job.
    Calculates: Isolation Forest, DBSCAN, and Z-Score Deviation.
    Writes mathematically mapped 3-Tier risk scores back to DB.
    """
    logger.info(f"Starting unsupervised analysis for job: {job_id}")
    
    # Normally we'd query the DB to get the transactions for this job_id
    # We will simulate fetching the dataframe directly via pandas read_sql
    # To conform to standard practices without knowing the exact DB URL in this context,
    # we provide the SQL logic.
    
    with get_db_connection() as session:
        # Fetch transactions
        sql = text("""
            SELECT id, amount, category, description_raw, value_date 
            FROM bank_transactions 
            WHERE job_id = :job_id
        """)
        result = session.execute(sql, {"job_id": job_id}).fetchall()
        
        if not result:
            logger.warning(f"No transactions found for job {job_id}")
            return
            
        df = pd.DataFrame(result, columns=['id', 'amount', 'category', 'description_raw', 'value_date'])
        
        # Explicitly convert database Decimal objects to standard floats for NumPy/scikit-learn compliance
        df['amount'] = df['amount'].astype(float)
        
        # 1. Isolation Forest (Anomaly Density)
        logger.info("Calculating Isolation Forest...")
        # Apply absolute log-transformation to handle orders of magnitude variance cleanly
        amount_reshaped = df['amount'].abs().values.reshape(-1, 1)
        amount_log = np.log1p(amount_reshaped)
        
        scaler = StandardScaler()
        amount_scaled = scaler.fit_transform(amount_log)
        
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        # sklearn returns -1 for anomaly, 1 for normal.
        # We need a score between 0.00 and 1.00 where higher is higher risk.
        # decision_function returns negative for anomalies, positive for normal.
        scores = iso_forest.fit_predict(amount_scaled)
        decision_scores = iso_forest.decision_function(amount_scaled)
        
        # Normalize decision scores to 0-1 range (Risk Score: 1 = high risk, 0 = low risk)
        # Min-Max scale and invert so negative (anomalies) approach 1.0
        norm_scores = (decision_scores - decision_scores.min()) / (decision_scores.max() - decision_scores.min())
        df['isolation_forest_score'] = 1.0 - norm_scores
        
        # Map to 3-Tier Risk Logic
        def map_tier(score):
            if score >= 0.85:
                return 1
            elif score >= 0.50:
                return 2
            return 3
            
        df['calculated_risk_tier'] = df['isolation_forest_score'].apply(map_tier)
        
        # 2. DBSCAN (Spatial Clustering based on amount)
        # Fix: Force 'kd_tree' algorithm to prevent a massive NxN pairwise memory allocation matrix
        logger.info("Calculating DBSCAN using specialized tree indexing...")
        dbscan = DBSCAN(eps=0.5, min_samples=3, algorithm='kd_tree')
        df['dbscan_cluster'] = dbscan.fit_predict(amount_scaled)
        
        # 3. Z-Score (Cython-optimized vectorized transformation)
        logger.info("Calculating vectorized Z-Score Deviation...")
        group_mean = df.groupby('description_raw')['amount'].transform('mean')
        group_std = df.groupby('description_raw')['amount'].transform('std')
        
        # Compute vectorized Z-score; division by zero or NaN (groups of size 1) are safely filled with 0.0
        df['z_score_deviation'] = (df['amount'] - group_mean) / group_std
        df['z_score_deviation'] = df['z_score_deviation'].fillna(0.0)
        
        # Optimized Batched Database Commit
        logger.info("Updating database with calculated vectors in optimized chunks...")
        update_sql = text("""
            UPDATE bank_transactions 
            SET isolation_forest_score = :if_score,
                dbscan_cluster = :cluster,
                z_score_deviation = :z_score,
                calculated_risk_tier = :tier
            WHERE id = :id
        """)
        
        updates = []
        for _, row in df.iterrows():
            updates.append({
                "if_score": float(row['isolation_forest_score']),
                "cluster": int(row['dbscan_cluster']),
                "z_score": float(row['z_score_deviation']),
                "tier": int(row['calculated_risk_tier']),
                "id": int(row['id'])
            })
            
        # Execute in explicit batches of 2,500 to keep memory footprint low and prevent long transaction locks
        chunk_size = 2500
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            session.execute(update_sql, chunk)
            session.commit()
            logger.info(f"Committed ML results chunk: rows {i} to {i + len(chunk)}")
        
    logger.info(f"Unsupervised ML analysis completed for job {job_id}")
    return {"status": "success", "processed_rows": len(updates)}
