#!/usr/bin/env python
"""Synthetic stress-data generator for the BRS Engine load harness.

Produces encrypted, multi-batch bank statement payloads plus matching
ledger export payloads, sized for high-volume load runs (10,000+ rows
across several concurrent batches).

Design notes
------------
- Statement CSVs carry the exact contract the ingest API enforces: a
  header containing both ``amount`` and ``description_raw`` (see
  app/main.py header validation) and the four columns
  app/task_ingestion.py selects (``date, value_date, description_raw,
  amount``). ``amount_paisa`` is emitted as a derived integer column
  (rupees * 100) for parity with the domain baseline; the pipeline
  ignores extra columns.
- Ledger CSVs use the alias-flexible shape app/polars_ledger_ingestion.py
  accepts (``ledger_date, description, amount, account_code, utr``). A
  configurable fraction of ledger rows are amount/date aligned to
  statement rows so the matching engine has real counterparties.
- A controlled anomaly ratio injects date mismatches, missing/duplicate
  reference IDs and zero-variance amounts to exercise matching edge cases.
- An optional corrupt batch (random bytes with a .enc suffix) exercises
  DLQ routing: the worker's Fernet decrypt fails and the chunk is routed
  to dlq_transactions.

Encryption uses the same Fernet pattern as the rest of the codebase.
Files and a manifest.json are written under the output directory; the
load harness (scripts/run_stress_test.py) reads that manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import sys
import uuid
from datetime import date, timedelta

# Ensure the repo root is importable when run as ``python scripts/...``.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from cryptography.fernet import Fernet  # noqa: E402

_DEV_FALLBACK_KEY = "Fz-2zO9Qh8k6u1Xn6bK_oB5vT8lF3wN7yP1mG5cR8A0="

_VENDORS = [
    "ACME SUPPLIES", "GLOBEX CORP", "STARK INDUSTRIES", "WAYNE ENTERPRISES",
    "UMBRELLA LOGISTICS", "INITECH SERVICES", "HOOLI CLOUD", "SOYLENT FOODS",
    "MASSIVE DYNAMIC", "TYRELL SYSTEMS",
]
_MODES = ["NEFT", "RTGS", "IMPS", "UPI"]
_ACCOUNT_CODES = ["4001", "5001", "5200", "6001", "7001"]

# Recent window: the matching engine only queries ledger rows within the last
# 30 days (app/task_matching_engine.py), so statement/ledger dates must be
# recent for reconciliation to find real counterparties.
_DATE_END = date.today()
_DATE_START = _DATE_END - timedelta(days=25)


def _fernet() -> Fernet:
    key = os.environ.get("FERNET_KEY", _DEV_FALLBACK_KEY)
    return Fernet(key.encode("utf-8"))


def _random_date(rng: random.Random, start: date = _DATE_START, end: date = _DATE_END) -> date:
    span = (end - start).days
    return start + timedelta(days=rng.randint(0, span))


def _random_utr(rng: random.Random) -> str:
    return "".join(rng.choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(16))


def _amount(rng: random.Random) -> float:
    """A realistic signed rupee amount with two decimal places."""
    magnitude = round(rng.uniform(100.0, 500000.0), 2)
    return magnitude if rng.random() < 0.55 else -magnitude


def _to_paisa(amount: float) -> int:
    return int(round(amount * 100))


def _csv_escape(value: str) -> str:
    """Minimal CSV-cell quoting for description text."""
    if any(c in value for c in [',', '"', '\n']):
        return '"' + value.replace('"', '""') + '"'
    return value


def _build_statement_batch(
    rng: random.Random, rows: int, anomaly_rate: float,
) -> tuple[bytes, list[dict]]:
    """Return (csv_bytes, records) for one statement batch."""
    lines = ["date,value_date,description_raw,amount,amount_paisa,utr"]
    records: list[dict] = []
    for _ in range(rows):
        d = _random_date(rng)
        vendor = rng.choice(_VENDORS)
        mode = rng.choice(_MODES)
        amt = _amount(rng)
        utr = _random_utr(rng)
        is_anomaly = rng.random() < anomaly_rate
        anomaly_kind = None
        if is_anomaly:
            anomaly_kind = rng.choice(["missing_ref", "date_shift", "zero_variance", "dup_ref"])
            if anomaly_kind == "missing_ref":
                utr = ""
            elif anomaly_kind == "zero_variance":
                amt = 50000.00
            elif anomaly_kind == "dup_ref":
                utr = "DUPUTR0000000001"
            # date_shift is applied on the ledger side to break the window
        desc = f"{mode} PAYMENT TO {vendor} REF {utr or 'NA'}"
        lines.append(
            f"{d.isoformat()},{d.isoformat()},{_csv_escape(desc)},"
            f"{amt:.2f},{_to_paisa(amt)},{utr}"
        )
        records.append({
            "date": d.isoformat(), "amount": amt, "utr": utr,
            "vendor": vendor, "anomaly": anomaly_kind,
        })
    return ("\n".join(lines) + "\n").encode("utf-8"), records


def _build_ledger_batch(
    rng: random.Random, statement_records: list[dict], match_fraction: float,
) -> tuple[bytes, int]:
    """Build a ledger batch, aligning a fraction of rows to statement rows.

    Returns (csv_bytes, row_count) so the manifest can record exactly how
    many ledger entries were generated for post-run reconciliation checks.
    """
    lines = ["ledger_date,description,amount,account_code,utr"]
    for rec in statement_records:
        # Only a fraction of statement rows get a ledger counterpart.
        if rng.random() > match_fraction:
            continue
        led_date = date.fromisoformat(rec["date"])
        # An anomalous date_shift statement row gets an out-of-window ledger date.
        if rec["anomaly"] == "date_shift":
            led_date = led_date + timedelta(days=rng.randint(10, 60))
        acct = rng.choice(_ACCOUNT_CODES)
        desc = f"LEDGER {rec['vendor']} SETTLEMENT"
        utr = "" if rec["anomaly"] == "missing_ref" else rec["utr"]
        lines.append(
            f"{led_date.isoformat()},{_csv_escape(desc)},"
            f"{rec['amount']:.2f},{acct},{utr}"
        )
    return ("\n".join(lines) + "\n").encode("utf-8"), len(lines) - 1


def generate(
    out_dir: pathlib.Path,
    total_rows: int,
    batches: int,
    tenants: int,
    anomaly_rate: float,
    match_fraction: float,
    dlq_batches: int,
    seed: int,
) -> dict:
    """Generate all payloads and a manifest; return the manifest dict."""
    rng = random.Random(seed)
    fernet = _fernet()
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_per_batch = max(1, total_rows // max(1, batches))
    manifest: dict = {
        "seed": seed,
        "total_rows_target": total_rows,
        "rows_per_batch": rows_per_batch,
        "batches": batches,
        "tenants": tenants,
        "anomaly_rate": anomaly_rate,
        "match_fraction": match_fraction,
        "dlq_batches": dlq_batches,
        "run_id": uuid.uuid4().hex[:12],
        "statements": [],
        "ledgers": [],
        "corrupt": [],
    }

    rows_written = 0
    ledger_rows_written = 0
    for b in range(batches):
        tenant = b % tenants
        company = f"StressTenant_{tenant:02d}"

        stmt_bytes, records = _build_statement_batch(rng, rows_per_batch, anomaly_rate)
        rows_written += len(records)
        stmt_name = f"stress_stmt_{manifest['run_id']}_{b:03d}.enc"
        (out_dir / stmt_name).write_bytes(fernet.encrypt(stmt_bytes))

        ledger_bytes, ledger_rows = _build_ledger_batch(rng, records, match_fraction)
        ledger_rows_written += ledger_rows
        ledger_name = f"stress_ledger_{manifest['run_id']}_{b:03d}.enc"
        (out_dir / ledger_name).write_bytes(fernet.encrypt(ledger_bytes))

        manifest["statements"].append({
            "file": stmt_name, "rows": len(records),
            "tenant": tenant, "company": company,
        })
        manifest["ledgers"].append({
            "file": ledger_name, "rows": ledger_rows,
            "tenant": tenant, "company": company,
        })

    # Optional corrupt batches for DLQ routing exercise.
    for c in range(dlq_batches):
        corrupt_name = f"stress_corrupt_{manifest['run_id']}_{c:03d}.enc"
        (out_dir / corrupt_name).write_bytes(os.urandom(2048))
        manifest["corrupt"].append({"file": corrupt_name})

    manifest["rows_written"] = rows_written
    manifest["ledger_rows_written"] = ledger_rows_written
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _default_out_dir() -> pathlib.Path:
    base = os.environ.get("STORAGE_DIR", "/tmp/brs_storage")
    return pathlib.Path(base) / "stress"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate BRS stress-test payloads.")
    parser.add_argument("--rows", type=int, default=10000, help="Total statement rows across all batches.")
    parser.add_argument("--batches", type=int, default=10, help="Number of concurrent statement/ledger batches.")
    parser.add_argument("--tenants", type=int, default=4, help="Simulated tenant accounts.")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Fraction of anomalous rows.")
    parser.add_argument("--match-fraction", type=float, default=0.6, help="Fraction of statement rows given a ledger counterpart.")
    parser.add_argument("--dlq-batches", type=int, default=1, help="Number of corrupt batches for DLQ routing.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=str, default=None, help="Output directory (default: STORAGE_DIR/stress).")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out) if args.out else _default_out_dir()
    manifest = generate(
        out_dir=out_dir,
        total_rows=args.rows,
        batches=args.batches,
        tenants=args.tenants,
        anomaly_rate=args.anomaly_rate,
        match_fraction=args.match_fraction,
        dlq_batches=args.dlq_batches,
        seed=args.seed,
    )
    print(
        f"[generate_stress_data] run_id={manifest['run_id']} "
        f"rows={manifest['rows_written']} ledger_rows={manifest['ledger_rows_written']} "
        f"statements={len(manifest['statements'])} "
        f"ledgers={len(manifest['ledgers'])} corrupt={len(manifest['corrupt'])} "
        f"-> {out_dir}"
    )


if __name__ == "__main__":
    main()
