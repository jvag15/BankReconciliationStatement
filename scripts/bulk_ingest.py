import os
import pathlib
import uuid
import sys
import psutil
import pandas as pd
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Ensure the root directory is in the Python path
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# Force explicit environment bindings before importing the app
load_dotenv()
os.environ["CELERY_BROKER_URL"] = os.environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
os.environ["POSTGRES_RESULT_BACKEND_URL"] = os.environ.get("POSTGRES_RESULT_BACKEND_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")
os.environ["DATABASE_URL"] = os.environ.get("DATABASE_URL", "postgresql://brs_user:brs_password@127.0.0.1:5432/brs_db")

from app.task_ingestion import process_statement_chunk

def get_dynamic_chunk_size() -> int:
    """Determine optimal chunk size based on available system RAM."""
    available_bytes = psutil.virtual_memory().available
    available_gb = available_bytes / (1024 ** 3)
    
    if available_gb < 4:
        return 1000
    elif available_gb < 8:
        return 2500
    else:
        return 5000

def main():
    base_dir = pathlib.Path(__file__).parent.parent
    data_file = base_dir / "var" / "data" / "reconciled_ledger_dump_75k.csv"
    upload_dir = base_dir / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    if not data_file.exists():
        print(f"Error: Data file not found at {data_file}")
        sys.exit(1)

    fernet_key = os.environ.get("FERNET_KEY")
    if not fernet_key:
        print("Error: FERNET_KEY not found in environment")
        sys.exit(1)

    f = Fernet(fernet_key.encode("utf-8"))
    
    # Dynamic hardware interrogation
    total_cores = os.cpu_count() or 1
    mem_info = psutil.virtual_memory()
    available_gb = mem_info.available / (1024 ** 3)
    total_gb = mem_info.total / (1024 ** 3)
    chunk_size = get_dynamic_chunk_size()
    
    print(f"[Hardware Detection] Cores: {total_cores} | Total RAM: {total_gb:.1f} GB | Available RAM: {available_gb:.1f} GB")
    print(f"[Adaptive Chunking] Selected CHUNK_SIZE = {chunk_size}")
    
    file_origin = "reconciled_ledger_dump_75k.csv"
    
    print(f"Starting bulk ingestion of {file_origin} in chunks of {chunk_size}...")
    
    chunk_iterator = pd.read_csv(data_file, chunksize=chunk_size)
    
    for i, chunk in enumerate(chunk_iterator):
        chunk_id = f"chunk_{uuid.uuid4().hex[:8]}"
        
        # Inject required columns for Polars ingestion
        chunk["source_chunk_id"] = chunk_id
        chunk["file_origin"] = file_origin
        
        # Serialize to CSV byte stream
        csv_payload = chunk.to_csv(index=False).encode("utf-8")
        
        # Encrypt the stream
        encrypted_payload = f.encrypt(csv_payload)
        
        # Write to disk
        enc_file = upload_dir / f"{chunk_id}.enc"
        enc_file.write_bytes(encrypted_payload)
        
        # Dispatch to Celery worker pool
        process_statement_chunk.apply_async(
            kwargs={
                "chunk_id": chunk_id,
                "file_origin": file_origin,
                "file_path_encrypted": str(enc_file),
            }
        )
        
        print(f"Dispatched chunk {i+1} (Rows: {len(chunk)}, Chunk ID: {chunk_id})")

    print("Bulk ingestion spooling complete. Tasks dispatched to Celery background workers.")

if __name__ == "__main__":
    main()
