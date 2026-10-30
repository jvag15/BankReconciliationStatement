import csv
import random
import os
import asyncio
import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TARGET_CSV_V1 = "var/data/chaos_corrupted_v1.csv"
TARGET_CSV_V2 = "var/data/chaos_corrupted_v2.csv"
API_URL = "http://localhost/api/v1/statements/ingest"

API_KEY = os.environ.get("API_AUTH_TOKEN", "super-secret-auth-token")

def generate_chaos(target_file):
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    with open(target_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "value_date", "description", "description_raw", "amount", "balance", "utr", "source_chunk_id", "file_origin"])
        
        for i in range(1, 101):
            date_str = "2024-04-01"
            amount = "100.00"
            balance = "1000.00"
            desc = f"TEST-DESC-{i}"
            desc_raw = f"RAW-DESC-{i}"
            utr = f"UTR{i:013d}"
            
            # Omit amount field entirely on 20 rows
            if i <= 20:
                amount = ""
                
            # Inject string anomalies into numeric indexes
            if 20 < i <= 40:
                anomaly = random.choice(["NULL", "TEN THOUSAND", "NaN", "$%^&*"])
                if random.random() > 0.5:
                    amount = anomaly
                else:
                    balance = anomaly
                    
            # Pad description_raw with multi-paragraph string blocks
            if i > 80:
                desc_raw = "A" * 5000 + "\n\n" + "B" * 5000
                
            writer.writerow([
                date_str, date_str, desc, desc_raw, amount, balance, utr, "chunk1", os.path.basename(target_file)
            ])
            
async def ingest_file(target_file):
    from cryptography.fernet import Fernet
    fernet_key = os.environ.get("FERNET_KEY", "Fz-2zO9Qh8k6u1Xn6bK_oB5vT8lF3wN7yP1mG5cR8A0=").encode()
    f_crypt = Fernet(fernet_key)
    
    with open(target_file, "rb") as f_in:
        plaintext = f_in.read()
    ciphertext = f_crypt.encrypt(plaintext)
    
    enc_file = target_file + ".enc"
    with open(enc_file, "wb") as f_out:
        f_out.write(ciphertext)
        
    headers = {"X-API-Key": API_KEY}
    async with httpx.AsyncClient() as client:
        with open(enc_file, "rb") as f:
            files = {"file": (os.path.basename(enc_file), f, "application/octet-stream")}
            response = await client.post(API_URL, headers=headers, files=files, timeout=30.0)
            print(f"[{enc_file}] Status: {response.status_code}")
            print(f"[{enc_file}] Response: {response.text}")

async def main():
    print("Generating Chaos Files...")
    generate_chaos(TARGET_CSV_V1)
    generate_chaos(TARGET_CSV_V2)
    
    print("Ingesting Pass 1-A...")
    await ingest_file(TARGET_CSV_V1)
    
    print("Ingesting Pass 1-B...")
    await ingest_file(TARGET_CSV_V2)

if __name__ == "__main__":
    asyncio.run(main())
