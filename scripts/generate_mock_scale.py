import csv
import random
import os
import uuid
import string
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

# Configuration
NUM_ROWS = 200000
TARGET_CSV = os.path.join("var", "data", "scale_mock_statement.csv")
TARGET_ENC = "E:\\bank-statement\\var\\data\\scale_mock_statement.csv.enc"

START_DATE = datetime(2024, 4, 1)
END_DATE = datetime(2025, 3, 31)

NULL_INJECTION_RATE = 0.02
ZERO_VARIANCE_RATE = 0.05
DUPLICATE_UTR_RATE = 0.005
CORRUPTION_RATE = 0.01

IDENTICAL_AMOUNT = 50000.00
DUPLICATE_UTR_VAL = "HDFC1234567890DUPLICAT"

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_random_utr():
    return "UTR" + "".join(random.choices(string.digits, k=13))

def generate_corrupted_string(length=10000):
    return "".join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=length))

def generate_mock_data():
    os.makedirs(os.path.dirname(TARGET_CSV), exist_ok=True)
    
    print(f"Generating {NUM_ROWS} synthetic rows...")
    
    with open(TARGET_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "value_date", "description", "description_raw", "amount", "balance", "utr", "source_chunk_id", "file_origin"])
        
        balance = 100000000.00 # Starting balance
        
        for i in range(NUM_ROWS):
            # Date Range: FY 2024-25
            r_date = random_date(START_DATE, END_DATE)
            date_str = r_date.strftime("%Y-%m-%d")
            
            # Zero-Variance Columns: 5% of rows with identical amounts
            if random.random() < ZERO_VARIANCE_RATE:
                amount = IDENTICAL_AMOUNT
            else:
                amount = round(random.uniform(0.01, 9999999.99), 2)
                if random.random() < 0.5:
                    amount = -amount # Debit
            
            balance += amount
            
            # Duplicate UTR Rate: 0.5%
            if random.random() < DUPLICATE_UTR_RATE:
                utr = DUPLICATE_UTR_VAL
            else:
                utr = generate_random_utr()
                
            # Null Injection Rate: 2% across all nullable columns (utr)
            if random.random() < NULL_INJECTION_RATE:
                utr = ""
                
            # Description Corruption: 1% rows with 10,000+ character strings
            if random.random() < CORRUPTION_RATE:
                desc = generate_corrupted_string(10000)
                desc_raw = desc
            else:
                desc = f"VENDOR-PAYMENT-{i}"
                desc_raw = f"NEFT-HDFC-{desc}"
                
            chunk_id = str(uuid.uuid4())
            file_origin = "scale_mock_statement.csv"
            
            writer.writerow([
                date_str, date_str, desc, desc_raw, f"{amount:.2f}", f"{balance:.2f}", utr, chunk_id, file_origin
            ])
            
            if (i + 1) % 50000 == 0:
                print(f"Generated {i + 1} rows...")

    print(f"Mock data generated at: {TARGET_CSV}")

    # Encrypt the file as expected by the pipeline
    key_str = os.environ.get("FERNET_KEY", "Fz-2zO9Qh8k6u1Xn6bK_oB5vT8lF3wN7yP1mG5cR8A0=")
    print(f"Encrypting file using key: {key_str}")
    f_cipher = Fernet(key_str.encode('utf-8'))
    
    with open(TARGET_CSV, "rb") as f_in:
        raw_csv = f_in.read()
        
    encrypted_payload = f_cipher.encrypt(raw_csv)
    
    with open(TARGET_ENC, "wb") as f_out:
        f_out.write(encrypted_payload)
        
    print(f"Encrypted statement generated at: {TARGET_ENC}")

if __name__ == "__main__":
    generate_mock_data()
