import csv
import os
import asyncio
import httpx
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TARGET_CSV = "var/data/reconciled_ledger_dump_50k.csv"
API_URL = "http://localhost/api/v1/statements/ingest"
API_KEY = os.environ.get("API_AUTH_TOKEN", "super-secret-auth-token")

def generate_50k_ledger():
    os.makedirs(os.path.dirname(TARGET_CSV), exist_ok=True)
    if os.path.exists(TARGET_CSV):
        print(f"{TARGET_CSV} already exists. Skipping generation.")
        return
        
    print(f"Generating 50k ledger dump at {TARGET_CSV}...")
    with open(TARGET_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "value_date", "description", "description_raw", "amount", "balance", "utr", "source_chunk_id", "file_origin"])
        
        for i in range(1, 50001):
            writer.writerow([
                "2024-04-01", "2024-04-01", f"LEDGER-ENTRY-{i}", f"RAW-LEDGER-{i}", "50.00", "1500.00", f"UTR{i:013d}", "chunk_burst", "reconciled_ledger_dump_50k.csv"
            ])
            
    from cryptography.fernet import Fernet
    fernet_key = os.environ.get("FERNET_KEY", "Fz-2zO9Qh8k6u1Xn6bK_oB5vT8lF3wN7yP1mG5cR8A0=").encode()
    f_crypt = Fernet(fernet_key)
    with open(TARGET_CSV, "rb") as f_in:
        plaintext = f_in.read()
    with open(TARGET_CSV + ".enc", "wb") as f_out:
        f_out.write(f_crypt.encrypt(plaintext))

async def upload_file(client, req_id):
    enc_file = TARGET_CSV + ".enc"
    headers = {"X-API-Key": API_KEY}
    print(f"[Req {req_id}] Dispatching...")
    start_time = time.time()
    try:
        with open(enc_file, "rb") as f:
            files = {"file": (os.path.basename(enc_file), f, "application/octet-stream")}
            response = await client.post(API_URL, headers=headers, files=files, timeout=60.0)
            elapsed = time.time() - start_time
            print(f"[Req {req_id}] Status: {response.status_code} | Time: {elapsed:.2f}s | {response.text}")
            return response.status_code
    except Exception as e:
        print(f"[Req {req_id}] Failed: {str(e)}")
        return str(e)

async def fire_burst(burst_name, num_requests=5):
    print(f"--- Firing {burst_name} ({num_requests} concurrent requests) ---")
    async with httpx.AsyncClient() as client:
        tasks = [upload_file(client, i) for i in range(1, num_requests + 1)]
        results = await asyncio.gather(*tasks)
    print(f"--- {burst_name} Results: {results} ---")

async def main():
    generate_50k_ledger()
    
    # Pass 2-A
    await fire_burst("Pass 2-A")
    
    # Pass 2-B
    await fire_burst("Pass 2-B")

if __name__ == "__main__":
    asyncio.run(main())
