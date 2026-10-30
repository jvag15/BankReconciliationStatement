import os
from celery import Celery

broker_url = "redis://localhost:6379/0"
app = Celery('bank_statement_pipeline', broker=broker_url, backend=broker_url)

print("[*] Dispatching run_ml_audit task for 250k-row stuck job to brs_worker...")
try:
    # Explicit signature matching container registry
    job_id = "d210aab7-ebd4-4d5d-94ca-bd98e9c71362"
    result = app.send_task(
        'run_ml_audit',
        kwargs={"job_id": job_id},
        queue='ml_predict'
    )
    print(f"[+] Task sent successfully! Task ID: {result.id}")
    print("[*] Waiting 15 seconds for worker response state...")
    import time
    time.sleep(15)
    print(f"[+] Worker state response: {result.status}")
    if result.status == "FAILURE":
        print(f"[-] Failure info: {result.result}")
    else:
        print(f"[+] Result value: {result.result}")
except Exception as e:
    print(f"[-] Handoff failure: {str(e)}")
