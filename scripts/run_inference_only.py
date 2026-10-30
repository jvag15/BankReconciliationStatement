import os
import sys

sys.path.insert(0, "/app")

os.environ['DATABASE_URL'] = 'postgresql://brs_user:brs_password@brs_postgres:5432/brs_db'
os.environ['CELERY_BROKER_URL'] = 'redis://brs_redis:6379/0'

from app.task_inference import run_ml_audit

try:
    run_ml_audit(job_id='11111111-1111-1111-1111-111111111111')
    print("Inference completed successfully.")
except Exception as e:
    print(f"Error during inference: {e}")
