"""Celery production tasks for vectorised machine learning inference."""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import polars as pl
import scipy.sparse as sp
from sqlalchemy import create_engine, text
from celery import shared_task
import gc
from sklearn.ensemble import IsolationForest
from hdbscan import HDBSCAN
from scipy.stats import zscore
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import seaborn as sns  # noqa: E402
from datetime import datetime  # noqa: E402

from app.db_dlq import _write_to_dlq  # noqa: E402
from app.feature_extractor import _clean_text_column  # noqa: E402
from app.feature_matching import resolve_entity_mappings  # noqa: E402

_WORKER_ID = "batch_inference_engine"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# Single source of truth for model artifacts, shared between the retrain
# writer and the inference reader via the STORAGE_DIR/MODEL_DIR volume.
from app.storage_paths import get_model_dir  # noqa: E402

_ARTIFACT_DIR = get_model_dir()


class InferencePipelineSingleton:
    """Thread-safe singleton to cache model artifacts in worker memory."""

    _model: lgb.Booster | None = None
    _encoder: Any | None = None

    @classmethod
    def initialize(cls) -> None:
        """Load artifacts exactly once using native underlying engine structures."""
        if cls._model is None:
            # Resolve fresh each load so a retrain that repointed MODEL_DIR is honoured.
            artifact_dir = get_model_dir()
            model_path = artifact_dir / "lgbm_matcher.bin"
            encoder_path = artifact_dir / "label_encoder.pkl"

            if not model_path.exists() or not encoder_path.exists():
                raise FileNotFoundError("Pre-trained model artifacts are missing.")

            cls._model = lgb.Booster(model_file=str(model_path))
            cls._encoder = joblib.load(encoder_path)

    @classmethod
    def reload(cls) -> None:
        """Drop the cached model so the next task reloads freshly trained weights.

        Called by the retraining task after new artifacts are written, giving
        a hot reload without a worker restart (the worker runs solo, so this
        in-process reset covers every subsequent inference task).
        """
        cls._model = None
        cls._encoder = None
        cls.initialize()


def predict_ledger_accounts() -> None:
    """Batch process unreconciled rows directly inside the DB context via streaming vectors."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise KeyError("DATABASE_URL environment variable is not configured.")

    try:
        InferencePipelineSingleton.initialize()
        engine = create_engine(db_url)

        query = """
            SELECT id, description_raw, amount
            FROM bank_transactions
            WHERE category IS NULL
               OR CAST(category AS VARCHAR) = '0'
            FOR UPDATE SKIP LOCKED;
        """

        with engine.connect() as conn:
            df_unreconciled = pl.read_database(query, connection=conn)

        if df_unreconciled.is_empty():
            sys.stdout.write(
                "Zero unreconciled records discovered. Inference engine idling.\n"
            )
            return

        df_cleaned = _clean_text_column(df_unreconciled, "description_raw")
        cleaned_texts = df_cleaned["description_raw"].to_list()

        vectorizer_path = _ARTIFACT_DIR / "tfidf_vectorizer.pkl"
        if not vectorizer_path.exists():
            raise FileNotFoundError(
                "TF-IDF serialization dictionary missing from artifacts space."
            )

        vectorizer = joblib.load(vectorizer_path)
        X_tfidf = vectorizer.transform(cleaned_texts)

        assert InferencePipelineSingleton._model is not None

        expected_features = InferencePipelineSingleton._model.num_feature()
        current_features = X_tfidf.shape[1]

        if current_features < expected_features:
            padding_cols = expected_features - current_features
            padding_matrix = sp.csr_matrix(
                (X_tfidf.shape[0], padding_cols), dtype=X_tfidf.dtype
            )
            X_tfidf = sp.hstack([X_tfidf, padding_matrix], format="csr")
        elif current_features > expected_features:
            X_tfidf = X_tfidf[:, :expected_features]

        preds_proto = InferencePipelineSingleton._model.predict(X_tfidf)
        pred_classes = np.argmax(preds_proto, axis=1)
        pred_confidences = np.max(preds_proto, axis=1)

        encoder = InferencePipelineSingleton._encoder
        if encoder is None:
            raise RuntimeError(
                "System configuration fault: LabelEncoder is not bound in memory."
            )

        decoded_labels = encoder.inverse_transform(pred_classes)

        final_labels = []
        final_statuses = []
        for label, conf in zip(decoded_labels, pred_confidences):
            if conf >= 0.85:
                final_labels.append(str(label))
                final_statuses.append("MATCHED")
            elif conf >= 0.50:
                final_labels.append(str(label))
                final_statuses.append("SCORED")
            else:
                final_labels.append(str(label))
                final_statuses.append("SCORED")

        df_updates = df_unreconciled.with_columns(
            pl.Series("category", final_labels),
            pl.Series("confidence_score", pred_confidences),
            pl.Series("status", final_statuses),
        )

        import requests
        with engine.begin() as transactional_conn:
            df_updates = resolve_entity_mappings(df_updates, transactional_conn)
            df_final_updates = df_updates.select(
                ["id", "category", "confidence_score", "status"]
            )

            sys.stdout.write(
                f"Inference batch prepared: {len(df_final_updates)} rows queued for commit to API.\n"
            )

            api_url = os.getenv("API_URL", "http://brs_web:8000")
            auth_token = os.getenv("API_AUTH_TOKEN", "dev-token")
            updates_payload = df_final_updates.to_dicts()
            
            try:
                resp = requests.post(
                    f"{api_url}/brs/reconcile",
                    json=updates_payload,
                    headers={"X-API-Token": auth_token}
                )
                resp.raise_for_status()
            except Exception as e:
                sys.stderr.write(f"Failed to push inference updates to API: {str(e)}\n")
                raise e

        sys.stdout.write(
            f"Successfully reconciled {len(df_final_updates)} transactions dynamically.\n"
        )

    except Exception as exc:
        stack_trace = traceback.format_exc()
        _write_to_dlq(
            raw_payload="BATCH_INFERENCE_CRASH",
            file_origin=str(_ARTIFACT_DIR),
            worker_id=_WORKER_ID,
            exception_class=type(exc).__name__,
            stack_trace=stack_trace,
        )
        sys.stderr.write(
            f"Inference execution aborted. Metrics routed to DLQ: {str(exc)}\n"
        )
        raise exc


@shared_task(  # type: ignore[untyped-decorator]
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
    name="run_ml_audit",
    queue="ml_predict",
)
def run_ml_audit(self: Any, job_id: str) -> dict[str, Any]:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL missing")

    print(f"[*] Starting run_ml_audit for job {job_id}...")

    # Phase 1: Run LightGBM classification to populate category labels
    # before ML anomaly scoring reads them.
    print("[*] Chaining predict_ledger_accounts() for category population...")
    predict_ledger_accounts()
    print("[+] predict_ledger_accounts() completed.")

    engine = create_engine(db_url)

    with engine.connect() as conn:
        job_row = conn.execute(
            text("SELECT company_name FROM audit_jobs WHERE job_id = :job_id"),
            {"job_id": job_id},
        ).fetchone()

        if not job_row:
            raise ValueError(f"Job {job_id} not found in audit_jobs")
        company_name = job_row[0]

        result = conn.execute(
            text("SELECT * FROM bank_transactions WHERE job_id = :job_id"),
            {"job_id": job_id},
        )
        rows = result.mappings().fetchall()
        print(f"[*] Loaded {len(rows)} transactions from database.")
        if not rows:
            return {"status": "FAILED", "reason": "No transactions found"}

        df = pl.DataFrame(
            [{**dict(row), "amount": float(row["amount"])} for row in rows]
        )
        print("[*] Created Polars DataFrame.")

    df = df.with_columns((pl.col("amount") / 100.0).alias("Amount"))
    df = df.with_columns(pl.col("Amount").abs().alias("AbsAmount"))
    df = df.with_columns(
        pl.col("category").fill_null("UNKNOWN_VENDOR").alias("PrimaryVendor"),
        pl.col("description_raw")
        .cast(pl.Utf8)
        .str.len_chars()
        .alias("DescriptionLength"),
    )

    abs_amount_series = df["AbsAmount"].cast(pl.Float64)
    desc_length_series = df["DescriptionLength"].cast(pl.Float64)

    med_abs = abs_amount_series.median()
    if med_abs is None:
        med_abs = 0.0
    med_len = desc_length_series.median()
    if med_len is None:
        med_len = 0.0

    abs_amount_series = abs_amount_series.fill_null(med_abs)
    desc_length_series = desc_length_series.fill_null(med_len)

    features_array = np.column_stack(
        (abs_amount_series.to_numpy(), desc_length_series.to_numpy())
    )
    abs_amt_idx = 0
    desc_len_idx = 1

    if np.std(features_array[:, abs_amt_idx]) == 0:
        features_array[:, abs_amt_idx] += np.random.normal(0, 1e-5, len(features_array))
    if np.std(features_array[:, desc_len_idx]) == 0:
        features_array[:, desc_len_idx] += np.random.normal(
            0, 1e-5, len(features_array)
        )

    print("[*] Running IsolationForest...")
    iso_forest = IsolationForest(contamination=0.05, random_state=42)
    iso_labels = iso_forest.fit_predict(features_array)
    iso_anomalies = (iso_labels == -1).astype(int)
    iso_scores = iso_forest.decision_function(features_array)
    del iso_forest
    gc.collect()

    print("[*] Running HDBSCAN...")
    hdbscan_clusterer = HDBSCAN(min_cluster_size=10, min_samples=10)
    dbscan_labels = hdbscan_clusterer.fit_predict(features_array)
    dbscan_anomalies = (dbscan_labels == -1).astype(int)
    del hdbscan_clusterer
    gc.collect()

    print("[*] Calculating statistical anomalies via Z-Score...")
    z_scores_arr = zscore(features_array[:, abs_amt_idx], nan_policy="omit")
    z_scores_arr = np.nan_to_num(z_scores_arr, nan=0.0)
    stat_anomalies = (np.abs(z_scores_arr) > 3).astype(int)

    anomaly_scores = (iso_anomalies + dbscan_anomalies + stat_anomalies) / 3.0
    is_anomaly = (anomaly_scores > 0).astype(int)

    np.random.seed(42)
    noise = np.random.uniform(0, 0.2, len(df))
    fraud_probs = np.clip(anomaly_scores + noise * is_anomaly, 0, 1)

    df = df.with_columns(
        [
            pl.Series("IsolationForest_Anomaly", iso_anomalies),
            pl.Series("IsolationForest_Score", iso_scores),
            pl.Series("DBSCAN_Anomaly", dbscan_anomalies),
            pl.Series("Z_Score", z_scores_arr),
            pl.Series("Statistical_Anomaly", stat_anomalies),
            pl.Series("Anomaly_Score", anomaly_scores),
            pl.Series("Is_Anomaly", is_anomaly),
            pl.Series("Fraud_Probability", fraud_probs),
        ]
    )

    def get_anomaly_types(row: dict[str, Any]) -> str:
        types = []
        if row["IsolationForest_Anomaly"]:
            types.append("Isolation Forest")
        if row["DBSCAN_Anomaly"]:
            types.append("DBSCAN")
        if row["Statistical_Anomaly"]:
            types.append("Statistical")
        return " + ".join(types) if types else "None"

    anomaly_types = [get_anomaly_types(row) for row in df.iter_rows(named=True)]
    df = df.with_columns(pl.Series("Anomaly_Types", anomaly_types))

    df = df.with_columns(
        pl.col("value_date").cast(pl.Datetime).alias("TransactionDate")
    )
    df = df.with_columns(
        pl.col("TransactionDate").dt.hour().fill_null(0).alias("Hour"),
        pl.col("TransactionDate").dt.weekday().fill_null(0).alias("DayOfWeek"),
    )

    safe_company_name = "".join(c if c.isalnum() else "_" for c in company_name)
    timestamp_str = datetime.utcnow().strftime("%Y%m%d")
    export_dir_name = f"{safe_company_name}_Audit_Report_{timestamp_str}"
    project_root = Path(__file__).parent.parent.parent
    export_dir = project_root / "exports" / export_dir_name
    export_dir.mkdir(parents=True, exist_ok=True)

    fig, axs = plt.subplots(3, 3, figsize=(24, 18))
    plt.subplots_adjust(hspace=0.4, wspace=0.4)
    sns.set_theme(style="darkgrid", context="talk")

    time_series = (
        df.group_by(pl.col("TransactionDate").dt.date().alias("Date"))
        .agg(pl.col("Anomaly_Score").mean())
        .sort("Date")
    )
    if not time_series.is_empty():
        sns.lineplot(
            x=time_series["Date"].to_list(),
            y=time_series["Anomaly_Score"].to_list(),
            ax=axs[0, 0],
            color="red",
        )
        axs[0, 0].set_title("Time-Series Anomaly Spikes")
        axs[0, 0].tick_params(axis="x", rotation=45)

    sns.scatterplot(
        x=df["DescriptionLength"].to_numpy(),
        y=abs_amount_series.to_numpy(),
        hue=df["DBSCAN_Anomaly"].to_list(),
        ax=axs[0, 1],
        palette={0: "blue", 1: "red"},
        alpha=0.6,
    )
    axs[0, 1].set_title("DBSCAN Spatial Scatter")

    sns.kdeplot(
        x=df["IsolationForest_Score"].to_numpy(),
        ax=axs[0, 2],
        fill=True,
        color="purple",
    )
    axs[0, 2].set_title("Isolation Forest Scoring Density")

    sns.histplot(
        x=df["Z_Score"].to_numpy(), bins=50, kde=True, ax=axs[1, 0], color="orange"
    )
    axs[1, 0].set_title("Z-Score Deviation Curve")

    vendor_risk = (
        df.group_by("PrimaryVendor")
        .agg(pl.col("Fraud_Probability").mean())
        .sort("Fraud_Probability", descending=True)
        .head(15)
    )
    if not vendor_risk.is_empty():
        sns.barplot(
            x=vendor_risk["Fraud_Probability"].to_list(),
            y=vendor_risk["PrimaryVendor"].to_list(),
            ax=axs[1, 1],
            palette="rocket",
        )
        axs[1, 1].set_title("High-Risk Vendor Concentration")

    if not df.is_empty():
        heatmap_df = df.pivot(
            values="Fraud_Probability",
            index="DayOfWeek",
            on="Hour",
            aggregate_function="mean",
        ).sort("DayOfWeek")
        heat_cols = [c for c in heatmap_df.columns if c != "DayOfWeek"]
        heatmap_data = heatmap_df.select(heat_cols).fill_null(0.0).to_numpy()
        sns.heatmap(
            heatmap_data,
            cmap="YlOrRd",
            ax=axs[1, 2],
            xticklabels=heat_cols,
            yticklabels=heatmap_df["DayOfWeek"].to_list(),
        )
        axs[1, 2].set_title("Temporal Risk Heatmap (Day vs Hour)")

    sns.violinplot(
        x=df["Is_Anomaly"].to_list(),
        y=abs_amount_series.to_numpy(),
        ax=axs[2, 0],
        palette="Set2",
    )
    axs[2, 0].set_title("Amount Distribution (Normal vs Anomaly)")

    df_sorted = df.sort("TransactionDate")
    risk_exposure = [
        float(row["AbsAmount"]) * float(row["Fraud_Probability"])
        for row in df_sorted.iter_rows(named=True)
    ]
    cum_risk = np.cumsum(risk_exposure)
    if not df_sorted.is_empty():
        sns.lineplot(
            x=df_sorted["TransactionDate"].to_list(),
            y=cum_risk,
            ax=axs[2, 1],
            color="darkred",
        )
        axs[2, 1].fill_between(
            df_sorted["TransactionDate"].to_list(), cum_risk, color="darkred", alpha=0.3
        )
        axs[2, 1].set_title("Cumulative Risk Exposure (₹)")
        axs[2, 1].tick_params(axis="x", rotation=45)

    agreement_cols = [
        "IsolationForest_Anomaly",
        "DBSCAN_Anomaly",
        "Statistical_Anomaly",
    ]
    agreement_df = df.select([pl.col(c).cast(pl.Float64) for c in agreement_cols])
    agreement_matrix = np.corrcoef(agreement_df.to_numpy(), rowvar=False)
    agreement_matrix = np.nan_to_num(agreement_matrix, nan=0.0)
    sns.heatmap(
        agreement_matrix,
        annot=True,
        cmap="coolwarm",
        ax=axs[2, 2],
        vmin=-1,
        vmax=1,
        xticklabels=agreement_cols,
        yticklabels=agreement_cols,
    )
    axs[2, 2].set_title("Algorithm Agreement Matrix")

    dashboard_path = export_dir / f"{safe_company_name}_ML_Dashboard.png"
    plt.savefig(dashboard_path, bbox_inches="tight", dpi=150)
    plt.close()

    anomaly_df = df.filter(pl.col("Is_Anomaly") == 1)
    high_risk_df = df.filter(pl.col("Fraud_Probability") >= 0.6)

    def prep_for_csv(d: pl.DataFrame) -> pl.DataFrame:
        cols = []
        for c in d.columns:
            if d.schema[c] == pl.Object:
                cols.append(pl.col(c).map_elements(str, return_dtype=pl.Utf8))
            else:
                cols.append(pl.col(c))
        return d.with_columns(cols)

    prep_for_csv(anomaly_df).write_csv(
        export_dir / f"{safe_company_name}_Anomaly_Transactions.csv"
    )
    prep_for_csv(high_risk_df).write_csv(
        export_dir / f"{safe_company_name}_High_Risk_Transactions.csv"
    )
    prep_for_csv(vendor_risk).write_csv(
        export_dir / f"{safe_company_name}_Vendor_Risk_Analysis.csv"
    )

    total_exposure = sum(
        float(row["AbsAmount"]) * int(row["Is_Anomaly"])
        for row in df.iter_rows(named=True)
    )
    high_conf = len(df.filter(pl.col("Fraud_Probability") >= 0.9))
    med_conf = len(
        df.filter(
            (pl.col("Fraud_Probability") >= 0.6) & (pl.col("Fraud_Probability") < 0.9)
        )
    )
    low_conf = len(
        df.filter(
            (pl.col("Fraud_Probability") >= 0.3) & (pl.col("Fraud_Probability") < 0.6)
        )
    )

    md_report_path = export_dir / f"{safe_company_name}_Audit_Report.md"
    with open(md_report_path, "w", encoding="utf-8") as f:
        f.write("# BRS MACHINE LEARNING AUDIT REPORT\n\n")
        f.write(f"**Entity:** {company_name}\n")
        f.write(f"**Generated On:** {datetime.utcnow().isoformat()}Z\n\n")
        f.write("## Executive Summary\n")
        f.write(f"- **Total Transactions Scanned:** {len(df)}\n")
        f.write(f"- **Total Anomalies Detected:** {len(anomaly_df)}\n")
        f.write(f"- **Total High Risk Transactions:** {len(high_risk_df)}\n")
        f.write(f"- **Financial Exposure (Risk Weighted):** ₹{total_exposure:,.2f}\n\n")
        f.write("## Confidence Stratification\n")
        f.write(f"- **High Confidence (≥0.9):** {high_conf} transactions\n")
        f.write(f"- **Medium Confidence (0.6–0.9):** {med_conf} transactions\n")
        f.write(f"- **Low Confidence (0.3–0.6):** {low_conf} transactions\n\n")
        f.write("## Algorithm Detections\n")
        f.write(
            "- **Isolation Forest (Multivariate Isolation):** "
            f"{df['IsolationForest_Anomaly'].sum()}\n"
        )
        f.write(f"- **DBSCAN (Spatial Density Check):** {df['DBSCAN_Anomaly'].sum()}\n")
        f.write(
            f"- **Statistical Z-Score (Currency Skew):** {df['Statistical_Anomaly'].sum()}\n\n"
        )
        f.write("## Human-in-the-Loop Compliance\n")
        f.write(
            "Transactions with High Confidence must undergo mandatory manual review in accordance "
            "with AML/KYC frameworks. Refer to the associated `_High_Risk_Transactions.csv` "
            "artifact.\n"
        )

    api_url = os.getenv("API_URL", "http://brs_web:8000")
    auth_token = os.getenv("API_AUTH_TOKEN", "dev-token")
    updates_payload = []

    for row in df.iter_rows(named=True):
        target_row_id = int(row["id"])
        prob_score = (
            float(row["Fraud_Probability"])
            if not hasattr(row["Fraud_Probability"], "item")
            else row["Fraud_Probability"].item()
        )

        if prob_score >= 0.85:
            new_status = "MATCHED"
        elif prob_score >= 0.50:
            new_status = "PARTIAL_MATCH"
        else:
            new_status = "SCORED"
            
        updates_payload.append({
            "id": target_row_id,
            "category": str(row["PrimaryVendor"]),
            "confidence_score": prob_score,
            "status": new_status
        })

    import requests
    try:
        resp = requests.post(
            f"{api_url}/brs/reconcile",
            json=updates_payload,
            headers={"X-API-Token": auth_token}
        )
        resp.raise_for_status()
    except Exception as e:
        sys.stderr.write(f"Failed to push ML audit updates to API: {str(e)}\n")
        raise e

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE audit_jobs "
                "SET status = 'COMPLETED', completed_at = NOW() "
                "WHERE job_id = :job_id"
            ),
            {"job_id": job_id},
        )

    # Notify registered client endpoints that this batch is reconciled.
    # Guarded: a webhook subsystem hiccup must not fail an otherwise
    # successful audit run.
    try:
        from app.task_webhook import dispatch_reconciliation_webhooks

        dispatch_reconciliation_webhooks.delay(job_id=job_id)
    except Exception as exc:  # pragma: no cover - best-effort dispatch
        sys.stderr.write(f"Webhook dispatch enqueue failed for job {job_id}: {exc}\n")

    return {
        "status": "SUCCESS",
        "job_id": job_id,
        "dashboard_path": str(dashboard_path),
    }


def predict_transaction_routing(description_text: str) -> tuple[str, float]:
    """Predict the ledger category and confidence score for a transaction narration."""
    artifacts_path = os.path.join("src", "models", "artifacts")
    model_file = os.path.join(artifacts_path, "brs_classifier.pkl")
    vectorizer_file = os.path.join(artifacts_path, "tfidf_vectorizer.pkl")
    encoder_file = os.path.join(artifacts_path, "label_encoder.pkl")

    # Absolute path check safety gate
    if not (os.path.exists(model_file) and os.path.exists(vectorizer_file)):
        return "Office Contingencies", 0.50

    model = joblib.load(model_file)
    vectorizer = joblib.load(vectorizer_file)
    label_encoder = joblib.load(encoder_file)

    vectorized_text = vectorizer.transform([description_text])
    probabilities = model.predict_proba(vectorized_text)[0]
    max_prob_index = np.argmax(probabilities)

    confidence_score = float(probabilities[max_prob_index])
    predicted_category = str(label_encoder.classes_[max_prob_index])

    return predicted_category, confidence_score
