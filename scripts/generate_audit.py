import gc
import os
import pathlib
import sys
import shutil
from datetime import datetime

import psutil
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from scipy.stats import zscore
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

# Ensure the root directory is in the Python path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# Force explicit environment bindings before importing the app
load_dotenv()
os.environ["CELERY_BROKER_URL"] = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
os.environ["POSTGRES_RESULT_BACKEND_URL"] = os.environ.get("POSTGRES_RESULT_BACKEND_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")
os.environ["DATABASE_URL"] = os.environ.get("DATABASE_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")

def main():
    base_dir = pathlib.Path(__file__).parent.parent
    exports_dir = base_dir / "exports"
    
    # 1. Clear the exports/ directory completely
    if exports_dir.exists():
        shutil.rmtree(exports_dir)
    exports_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. Connect to Database and Fetch Data
    engine = create_engine(os.environ["DATABASE_URL"])
    
    # We use a raw text query in SQLAlchemy 2.0
    with engine.connect() as conn:
        df = pd.read_sql_query(text("SELECT * FROM bank_transactions"), conn)
        
    if len(df) == 0:
        print("No transactions found in bank_transactions. Run bulk_ingest.py and Celery workers first.")
        # Proceed with empty dummy df just to write the schema files
        df = pd.DataFrame(columns=[
            'id', 'source_chunk_id', 'file_origin', 'status', 'amount', 'value_date', 
            'description_raw', 'utr', 'category', 'risk_score', 'confidence_score'
        ])
        
    print(f"Loaded {len(df)} transactions. Running ML Audit...")
    gc.collect()

    # Data preprocessing
    df['Amount'] = df['amount'].astype(float)
    df['AbsAmount'] = df['Amount'].abs()
    # Mock PrimaryVendor if category is null
    df['PrimaryVendor'] = df['category'].fillna("UNKNOWN_VENDOR")
    
    # Simple length feature for anomaly detection
    df['DescriptionLength'] = df['description_raw'].astype(str).apply(len)
    
    # We will compute anomalies if we have data
    if len(df) > 0:
        # Fill missing values with median to prevent 0-imputation variance distortion
        median_values = df[['AbsAmount', 'DescriptionLength']].median()
        features = df[['AbsAmount', 'DescriptionLength']].fillna(median_values)
        
        # Add epsilon noise to prevent zero-variance convergence
        if features['AbsAmount'].std() == 0:
            features['AbsAmount'] += np.random.normal(0, 1e-5, len(features))
        if features['DescriptionLength'].std() == 0:
            features['DescriptionLength'] += np.random.normal(0, 1e-5, len(features))
        
        # 3. Apply Unsupervised Anomaly Detection Models
        
        # Isolation Forest
        # Returns 1 for inliers, -1 for outliers
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        df['IsolationForest_Label'] = iso_forest.fit_predict(features)
        df['IsolationForest_Anomaly'] = (df['IsolationForest_Label'] == -1).astype(int)
        del iso_forest
        gc.collect()
        
        # DBSCAN
        # Memory-efficient spatial indexing with dynamic leaf_size based on available RAM
        available_gb = psutil.virtual_memory().available / (1024 ** 3)
        leaf_size = 50 if available_gb < 8 else 30
        dbscan = DBSCAN(eps=3.0, min_samples=10, algorithm='kd_tree', leaf_size=leaf_size)
        df['DBSCAN_Label'] = dbscan.fit_predict(features)
        del dbscan
        gc.collect()
        # -1 indicates noise/outlier in DBSCAN
        df['DBSCAN_Anomaly'] = (df['DBSCAN_Label'] == -1).astype(int)
        
        # Z-Score (Statistical Anomaly)
        # We look at AbsAmount mostly, using nan_policy to prevent NaN on 0-variance
        z_scores = zscore(df['AbsAmount'], nan_policy='omit')
        z_scores = np.nan_to_num(z_scores, nan=0.0)
        df['Statistical_Anomaly'] = (np.abs(z_scores) > 3).astype(int)
        del z_scores, features
        gc.collect()
        
        # Calculate Anomaly Score (0.0 to 1.0)
        df['Anomaly_Score'] = (df['IsolationForest_Anomaly'] + df['DBSCAN_Anomaly'] + df['Statistical_Anomaly']) / 3.0
        
        # Is_Anomaly threshold
        df['Is_Anomaly'] = (df['Anomaly_Score'] > 0).astype(int)
        
        # Fraud_Probability (for demonstration, just scaling Anomaly_Score + some random noise)
        np.random.seed(42)
        noise = np.random.uniform(0, 0.2, len(df))
        df['Fraud_Probability'] = np.clip(df['Anomaly_Score'] + noise * df['Is_Anomaly'], 0, 1)
        
        # Anomaly Types (combining strings)
        def get_anomaly_types(row):
            types = []
            if row['IsolationForest_Anomaly']: types.append('Isolation Forest')
            if row['DBSCAN_Anomaly']: types.append('DBSCAN')
            if row['Statistical_Anomaly']: types.append('Statistical')
            return " + ".join(types) if types else "None"
            
        df['Anomaly_Types'] = df.apply(get_anomaly_types, axis=1)
        
    else:
        df['IsolationForest_Anomaly'] = 0
        df['DBSCAN_Anomaly'] = 0
        df['Statistical_Anomaly'] = 0
        df['Anomaly_Score'] = 0.0
        df['Is_Anomaly'] = 0
        df['Fraud_Probability'] = 0.0
        df['Anomaly_Types'] = "None"
        df['DescriptionLength'] = 0

    # 4. Generate Artifacts
    
    # Re-map columns for the output templates
    output_df = df.copy()
    output_df['TransactionDate'] = output_df['value_date']
    output_df['ValueDate'] = output_df['value_date']
    output_df['Description'] = output_df['description_raw']
    
    tx_cols = [
        'TransactionDate', 'ValueDate', 'Description', 'refnochequeno', 'BranchCode', 'Debit', 
        'Credit', 'Balance', 'Amount', 'AbsAmount', 'TransactionType', 'Hour', 'DayOfWeek', 
        'IsWeekend', 'Month', 'Quarter', 'IsRoundAmount', 'LogAmount', 'PrevBalance', 'BalanceChange', 
        'TransactionIndex', 'CleanDescription', 'ExtractedEntities', 'ExtractedPatterns', 
        'ExtractedVendors', 'PrimaryVendor', 'IsolationForest_Anomaly', 'DBSCAN_Anomaly', 
        'Statistical_Anomaly', 'Time_Anomaly', 'Amount_Anomaly', 'Anomaly_Score', 'Is_Anomaly', 
        'VendorEncoded', 'DescriptionLength', 'Potential_Fraud', 'Fraud_Probability', 'Fraud_Prediction', 
        'CustomFraudRule', 'Fraud_Prediction_Final', 'Top_Features', 'Triggered_Rules'
    ]
    
    for c in tx_cols:
        if c not in output_df:
            output_df[c] = ""
            
    # We output all rows to Anomaly/High risk if they meet thresholds
    anomaly_df = output_df[output_df['Is_Anomaly'] == 1].copy()
    high_risk_df = output_df[output_df['Fraud_Probability'] >= 0.6].copy()
    
    # 4.1 audit_sample_2024-25_Anomaly_Transactions.csv
    anomaly_csv_path = exports_dir / "audit_sample_2024-25_Anomaly_Transactions.csv"
    anomaly_df[tx_cols].to_csv(anomaly_csv_path, index=False)
    
    # 4.2 audit_sample_2024-25_High_Risk_Transactions.csv
    high_risk_csv_path = exports_dir / "audit_sample_2024-25_High_Risk_Transactions.csv"
    high_risk_df[tx_cols].to_csv(high_risk_csv_path, index=False)
    
    # 4.3 audit_sample_2024-25_Vendor_Risk_Analysis.csv
    vendor_risk_csv_path = exports_dir / "audit_sample_2024-25_Vendor_Risk_Analysis.csv"
    if len(df) > 0:
        vendor_risk = df.groupby('PrimaryVendor').agg({
            'Fraud_Probability': ['mean', 'max'],
            'Anomaly_Score': ['mean', 'max'],
            'AbsAmount': ['count', 'sum', 'mean'],
            'Amount': ['sum']
        })
        vendor_risk.to_csv(vendor_risk_csv_path)
    else:
        # Create empty template
        with open(vendor_risk_csv_path, "w") as f:
            f.write(",Fraud_Probability,Fraud_Probability,Anomaly_Score,Anomaly_Score,AbsAmount,AbsAmount,AbsAmount,Amount\n")
            f.write(",mean,max,mean,max,count,sum,mean,sum\n")
            f.write("PrimaryVendor,,,,,,,,\n")
    
    # 4.4 user_feedback.csv
    feedback_cols = [
        'TransactionDate', 'ValueDate', 'Description', 'Amount', 'AbsAmount', 'PrimaryVendor', 
        'Fraud_Probability', 'Fraud_Prediction_Final', 'Is_Anomaly', 'CustomFraudRule', 
        'Top_Features', 'Triggered_Rules', 'Feedback'
    ]
    feedback_df = high_risk_df.head(50).copy() if len(high_risk_df) > 0 else output_df.head(50).copy()
    for c in feedback_cols:
        if c not in feedback_df:
            feedback_df[c] = ""
    feedback_df['Feedback'] = ""
    feedback_csv_path = exports_dir / "user_feedback.csv"
    feedback_df[feedback_cols].to_csv(feedback_csv_path, index=False)
    
    # 4.5 audit_sample_2024-25_Audit_Report.txt
    audit_txt_path = exports_dir / "audit_sample_2024-25_Audit_Report.txt"
    with open(audit_txt_path, "w") as f:
        f.write("=========================================================\n")
        f.write("      BRS MACHINE LEARNING AUDIT REPORT (2024-25)\n")
        f.write("=========================================================\n\n")
        f.write(f"Generated On: {datetime.utcnow().isoformat()}Z\n")
        f.write(f"Total Transactions Scanned: {len(df)}\n")
        f.write(f"Total Anomalies Detected: {len(anomaly_df)}\n")
        f.write(f"Total High Risk Transactions: {len(high_risk_df)}\n\n")
        f.write("ALGORITHM DETECTIONS:\n")
        f.write(f"- Isolation Forest: {df['IsolationForest_Anomaly'].sum() if len(df) > 0 else 0}\n")
        f.write(f"- DBSCAN: {df['DBSCAN_Anomaly'].sum() if len(df) > 0 else 0}\n")
        f.write(f"- Statistical (Z-Score): {df['Statistical_Anomaly'].sum() if len(df) > 0 else 0}\n\n")
        f.write("=========================================================\n")
        f.write("END OF REPORT\n")

    # 4.6 audit_sample_2024-25_ML_Dashboard.png
    dashboard_path = exports_dir / "audit_sample_2024-25_ML_Dashboard.png"
    
    # Create a 2x2 grid
    fig, axs = plt.subplots(2, 2, figsize=(16, 12))
    plt.subplots_adjust(hspace=0.3, wspace=0.3)
    
    # Top-Left: Amount vs. Anomaly Score
    if len(df) > 0:
        axs[0, 0].scatter(df['Anomaly_Score'], df['AbsAmount'], alpha=0.5, c=df['Fraud_Probability'], cmap='coolwarm')
        axs[0, 0].set_title('Amount vs. Anomaly Score')
        axs[0, 0].set_xlabel('Anomaly Score')
        axs[0, 0].set_ylabel('Absolute Amount')
    
    # Top-Right: Top 10 Highest Risk Vendors
    if len(vendor_risk) > 0:
        # Sort by ('Fraud_Probability', 'mean')
        top_vendors = vendor_risk.sort_values(by=('Fraud_Probability', 'mean'), ascending=False).head(10).reset_index()
        # Flatten columns for seaborn just in case
        top_vendors.columns = ['_'.join(col).strip('_') for col in top_vendors.columns.values]
        
        sns.barplot(x='Fraud_Probability_mean', y='PrimaryVendor', data=top_vendors, ax=axs[0, 1], palette='Reds_r')
        axs[0, 1].set_title('Top 10 Highest Risk Vendors (by Avg Fraud Prob)')
        axs[0, 1].set_xlabel('Average Fraud Probability')
        axs[0, 1].set_ylabel('Vendor')
    else:
        axs[0, 1].set_title('Top 10 Highest Risk Vendors (No Data)')
        
    # Bottom-Left: Fraud Probabilities Distribution
    if len(df) > 0:
        sns.histplot(df['Fraud_Probability'], bins=20, kde=True, ax=axs[1, 0], color='purple')
        axs[1, 0].set_title('Distribution of Fraud Probabilities')
        axs[1, 0].set_xlabel('Fraud Probability')
        axs[1, 0].set_ylabel('Frequency')
    
    # Bottom-Right: Anomaly Types Breakdown
    if len(df) > 0:
        # We count individual flags since a transaction can have multiple
        counts = [
            df['IsolationForest_Anomaly'].sum(),
            df['DBSCAN_Anomaly'].sum(),
            df['Statistical_Anomaly'].sum()
        ]
        labels = ['Isolation Forest', 'DBSCAN', 'Statistical']
        
        # Filter out zero counts
        non_zero = [(c, l) for c, l in zip(counts, labels) if c > 0]
        if non_zero:
            counts, labels = zip(*non_zero)
            axs[1, 1].pie(counts, labels=labels, autopct='%1.1f%%', colors=['#ff9999','#66b3ff','#99ff99'])
            axs[1, 1].set_title('Anomaly Types Breakdown')
        else:
            axs[1, 1].set_title('Anomaly Types Breakdown (No Anomalies)')
            axs[1, 1].axis('off')
    else:
        axs[1, 1].set_title('Anomaly Types Breakdown (No Data)')
        axs[1, 1].axis('off')
        
    plt.savefig(dashboard_path, bbox_inches='tight')
    plt.close()
    
    print("Successfully generated all 6 artifacts in exports/")
    
    # 5. Executive Alerting
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook_url:
        import requests
        try:
            summary_text = (
                f"**BRS Machine Learning Audit Report**\n"
                f"- Total Transactions Scanned: {len(df)}\n"
                f"- Total Anomalies Detected: {len(anomaly_df)}\n"
                f"- Total High Risk Transactions: {len(high_risk_df)}"
            )
            
            with open(dashboard_path, "rb") as f:
                files = {"file": ("audit_sample_2024-25_ML_Dashboard.png", f, "image/png")}
                data = {"content": summary_text}
                response = requests.post(webhook_url, data=data, files=files, timeout=10)
                response.raise_for_status()
            print("Successfully sent executive alert webhook.")
        except Exception as e:
            print(f"Failed to send executive alert webhook: {e}")

if __name__ == "__main__":
    main()
