# This file is part of the data generation module for synthetic payments data. 
# It generates daily CSV files containing transaction records.
# This script was made using ChatGPT-5, a large language model by OpenAI.


"""
Generate synthetic payments data as daily CSV files.

Outputs:
  data/raw/transactions/ingest_date=YYYY-MM-DD/transactions_YYYY-MM-DD.csv

Schema (header included):
  txn_id,merchant_id,user_id,amount,currency,status,txn_ts,country

Notes:
  - Timestamps are UTC ISO-8601 without timezone suffix (e.g. 2025-08-12T14:03:21).
  - Set --invalid-rate > 0 to inject a few invalid rows for DQ testing.
  - Keep rows_per_day modest for cost control when testing in AWS.

Example:
  python src/data_generator/generate_transaction_data.py \
      --start-date 2025-08-10 --end-date 2025-08-12 \
      --rows-per-day 1000 --invalid-rate 0.02 --seed 42
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Tuple

# ---- Domain lists (tweak freely) ---------------------------------------------

CURRENCIES: List[str] = ["GBP", "USD", "EUR", "JPY", "AUD", "CAD"]
STATUSES: List[str] = ["AUTHORISED", "SETTLED", "REFUNDED", "CHARGEBACK", "DECLINED", "PENDING", "SUCCESS", "FAILED"]
COUNTRIES: List[str] = ["GB", "FR", "DE", "ES", "IE", "NL", "IT", "JP", "AU", "CA"]

# A small merchant pool for realism
MERCHANT_IDS: List[str] = [f"m_{i:04d}" for i in range(1, 51)]  # m_0001..m_0050

# ---- Data model ---------------------------------------------------------------

@dataclass(frozen=True)
class Transaction:
    txn_id: str
    merchant_id: str
    user_id: str
    amount: float
    currency: str
    status: str
    txn_ts: str        # ISO-8601 date-time, UTC naive (e.g. 2025-08-12T12:34:56)
    country: str

    def as_row(self) -> List[str]:
        return [
            self.txn_id,
            self.merchant_id,
            self.user_id,
            f"{self.amount:.2f}",
            self.currency,
            self.status,
            self.txn_ts,
            self.country,
        ]


# ---- Helpers -----------------------------------------------------------------

def daterange(start: date, end: date) -> Iterable[date]:
    """Inclusive date range from start to end."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def random_amount() -> float:
    # Skew towards small ticket sizes, with occasional larger payments
    base = random.random()
    if base < 0.85:
        return round(random.uniform(1.0, 80.0), 2)
    elif base < 0.98:
        return round(random.uniform(80.0, 400.0), 2)
    else:
        return round(random.uniform(400.0, 2000.0), 2)


def pick_valid_tuple() -> Tuple[str, str, str, float, str, str]:
    merchant = random.choice(MERCHANT_IDS)
    user = f"u_{random.randint(1, 5000):06d}"
    amount = random_amount()
    currency = random.choice(CURRENCIES)
    status = random.choices(
        STATUSES,
        weights=[30, 40, 5, 2, 15, 5, 2, 1], # mostly authorised/settled/declined
        k=1,
    )[0]
    country = random.choice(COUNTRIES)
    return merchant, user, country, amount, currency, status


def maybe_invalidate(row: Transaction, invalid_rate: float) -> Transaction:
    """Return a possibly invalid variant to exercise DQ rules."""
    if random.random() >= invalid_rate:
        return row

    # Choose a type of invalidity
    issue = random.choice(
        ["neg_amount", "weird_currency", "bad_status", "blank_user"]
    )
    if issue == "neg_amount":
        return Transaction(
            row.txn_id, row.merchant_id, row.user_id, -abs(row.amount),
            row.currency, row.status, row.txn_ts, row.country
        )
    if issue == "weird_currency":
        return Transaction(
            row.txn_id, row.merchant_id, row.user_id, row.amount,
            "ZZZ", row.status, row.txn_ts, row.country
        )
    if issue == "bad_status":
        return Transaction(
            row.txn_id, row.merchant_id, row.user_id, row.amount,
            row.currency, "PENDINGISH", row.txn_ts, row.country
        )
    if issue == "blank_user":
        return Transaction(
            row.txn_id, row.merchant_id, "", row.amount,
            row.currency, row.status, row.txn_ts, row.country
        )
    return row


def generate_for_day(d: date, rows: int, invalid_rate: float) -> List[Transaction]:
    """Create a list of synthetic transactions for a single day."""
    out: List[Transaction] = []
    # Spread events across the day
    for _ in range(rows):
        merchant, user, country, amount, currency, status = pick_valid_tuple()
        # Random second within the day
        seconds = random.randint(0, 86399)
        dt = datetime(d.year, d.month, d.day) + timedelta(seconds=seconds)
        txn = Transaction(
            txn_id=str(uuid.uuid4()),
            merchant_id=merchant,
            user_id=user,
            amount=amount,
            currency=currency,
            status=status,
            txn_ts=dt.strftime("%Y-%m-%dT%H:%M:%S"),
            country=country,
        )
        txn = maybe_invalidate(txn, invalid_rate)
        out.append(txn)
    return out


def write_csv(path: Path, rows: Iterable[Transaction]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["txn_id", "merchant_id", "user_id", "amount", "currency", "status", "txn_ts", "country"]
        )
        for r in rows:
            writer.writerow(r.as_row())


# ---- CLI ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic payments CSVs by day."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--rows-per-day", type=int, default=1000,
                        help="How many rows per day (default: 1000)")
    parser.add_argument("--invalid-rate", type=float, default=0.01,
                        help="Probability (0..1) a row is intentionally invalid (default: 0.01)")
    parser.add_argument("--out-root", default="data/raw/transactions",
                        help="Local output root (default: data/raw/transactions)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducible output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    out_root = Path(args.out_root)

    for day in daterange(start, end):
        ingest_prefix = out_root / f"ingest_date={day.isoformat()}"
        out_path = ingest_prefix / f"transactions_{day.isoformat()}.csv"
        rows = generate_for_day(day, args.rows_per_day, args.invalid_rate)
        write_csv(out_path, rows)
        print(f"Wrote {out_path} ({len(rows)} rows)")

    print("\nDone. Example S3 layout when uploaded:")
    print("  s3://<your-bucket>/raw/transactions/ingest_date=YYYY-MM-DD/transactions_YYYY-MM-DD.csv")


if __name__ == "__main__":
    main()
