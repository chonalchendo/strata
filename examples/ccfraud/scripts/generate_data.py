#!/usr/bin/env python3
"""Generate synthetic credit card fraud detection data.

Creates CSV files for the linked entity model:
    merchants.csv  -- 50 merchants with categories and locations
    banks.csv      -- 10 banks with credit ratings
    accounts.csv   -- 200 accounts linked to banks
    cards.csv      -- 300 cards linked to accounts
    transactions.csv -- 5000 transactions linked to cards and merchants

Fraud patterns (~5% rate):
    - High-amount transactions at unusual hours
    - Burst patterns (multiple rapid transactions)

Uses only Python stdlib + csv. No external dependencies.

Usage:
    python scripts/generate_data.py
"""

from __future__ import annotations

import csv
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
SEED = 42
random.seed(SEED)

# Output directory
DATA_DIR = Path(__file__).parent.parent / "data"

# --- Constants ---

MERCHANT_CATEGORIES = [
    "electronics",
    "grocery",
    "travel",
    "restaurant",
    "gas_station",
    "clothing",
    "entertainment",
    "health",
    "home_improvement",
    "online_retail",
]

COUNTRIES = [
    "US",
    "UK",
    "DE",
    "FR",
    "NL",
    "SE",
    "CA",
    "AU",
    "JP",
    "BR",
]

BANK_NAMES = [
    "First National Bank",
    "Pacific Trust",
    "Atlantic Savings",
    "Northern Credit Union",
    "Southern Federal",
    "Central Reserve",
    "Eastern Alliance",
    "Western Capital",
    "Global Finance Corp",
    "Metro Banking Group",
]

CREDIT_RATINGS = ["AAA", "AA", "A", "BBB", "BB", "B"]

CARD_TYPES = ["debit", "credit", "prepaid"]
CARD_BRANDS = ["visa", "mastercard", "amex"]

# Locations (lat, lon) for merchants -- major city coords
CITY_COORDS = [
    (40.7128, -74.0060),   # New York
    (51.5074, -0.1278),    # London
    (52.5200, 13.4050),    # Berlin
    (48.8566, 2.3522),     # Paris
    (52.3676, 4.9041),     # Amsterdam
    (59.3293, 18.0686),    # Stockholm
    (43.6532, -79.3832),   # Toronto
    (-33.8688, 151.2093),  # Sydney
    (35.6762, 139.6503),   # Tokyo
    (-23.5505, -46.6333),  # Sao Paulo
]


def generate_merchants(n: int = 50) -> list[dict]:
    """Generate merchant records with categories and locations."""
    merchants = []
    for i in range(n):
        city_idx = i % len(CITY_COORDS)
        lat, lon = CITY_COORDS[city_idx]
        # Add some jitter to coordinates
        lat += random.uniform(-0.05, 0.05)
        lon += random.uniform(-0.05, 0.05)

        merchants.append({
            "merchant_id": f"M{i + 1:04d}",
            "merchant_name": f"Merchant_{i + 1}",
            "category": random.choice(MERCHANT_CATEGORIES),
            "country": COUNTRIES[city_idx],
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })
    return merchants


def generate_banks(n: int = 10) -> list[dict]:
    """Generate bank records with credit ratings."""
    banks = []
    base_date = datetime(2023, 1, 1)
    for i in range(n):
        rating_date = base_date + timedelta(days=random.randint(0, 365))
        banks.append({
            "bank_id": f"B{i + 1:03d}",
            "bank_name": BANK_NAMES[i % len(BANK_NAMES)],
            "country": COUNTRIES[i % len(COUNTRIES)],
            "credit_rating": random.choice(CREDIT_RATINGS),
            "credit_rating_date": rating_date.strftime("%Y-%m-%d"),
        })
    return banks


def generate_accounts(n: int = 200, banks: list[dict] | None = None) -> list[dict]:
    """Generate account records linked to banks."""
    if banks is None:
        banks = generate_banks()

    bank_ids = [b["bank_id"] for b in banks]
    accounts = []

    for i in range(n):
        accounts.append({
            "account_id": f"A{i + 1:05d}",
            "bank_id": random.choice(bank_ids),
            "home_country": random.choice(COUNTRIES),
            "email": f"user{i + 1}@{'corporate' if random.random() < 0.3 else 'gmail'}.com",
        })
    return accounts


def generate_cards(
    n: int = 300,
    accounts: list[dict] | None = None,
) -> list[dict]:
    """Generate card records linked to accounts."""
    if accounts is None:
        accounts = generate_accounts()

    account_ids = [a["account_id"] for a in accounts]
    cards = []
    base_date = datetime(2020, 1, 1)

    for i in range(n):
        issue_date = base_date + timedelta(days=random.randint(0, 1000))
        expiry_date = issue_date + timedelta(days=random.randint(730, 1825))  # 2-5 years
        # Generate a realistic-looking card number (16 digits)
        cc_num = f"{random.randint(4000, 5999):04d}{random.randint(0, 9999):04d}{random.randint(0, 9999):04d}{random.randint(0, 9999):04d}"

        cards.append({
            "cc_num": cc_num,
            "account_id": random.choice(account_ids),
            "issue_date": issue_date.strftime("%Y-%m-%d"),
            "expiry_date": expiry_date.strftime("%Y-%m-%d"),
            "card_type": random.choice(CARD_TYPES),
            "brand": random.choice(CARD_BRANDS),
        })
    return cards


def generate_transactions(
    n: int = 5000,
    cards: list[dict] | None = None,
    merchants: list[dict] | None = None,
    fraud_rate: float = 0.05,
) -> list[dict]:
    """Generate transaction records with fraud labels.

    Fraud patterns:
        - High-amount transactions (3x-10x normal)
        - Unusual hours (2am-5am)
        - Some burst patterns (multiple rapid transactions)
    """
    if cards is None:
        cards = generate_cards()
    if merchants is None:
        merchants = generate_merchants()

    cc_nums = [c["cc_num"] for c in cards]
    merchant_ids = [m["merchant_id"] for m in merchants]
    merchant_lookup = {m["merchant_id"]: m for m in merchants}

    transactions = []
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 10, 1)
    date_range_seconds = int((end_date - start_date).total_seconds())

    # Pre-select which transactions will be fraudulent
    fraud_indices = set(random.sample(range(n), int(n * fraud_rate)))

    for i in range(n):
        is_fraud = i in fraud_indices
        cc_num = random.choice(cc_nums)
        merchant_id = random.choice(merchant_ids)
        merchant = merchant_lookup[merchant_id]

        # Transaction datetime
        offset = random.randint(0, date_range_seconds)
        txn_datetime = start_date + timedelta(seconds=offset)

        if is_fraud:
            # Fraud: high amounts, unusual hours
            amount = round(random.uniform(500, 5000), 2)
            # Shift to unusual hours (2am-5am)
            txn_datetime = txn_datetime.replace(
                hour=random.randint(2, 5),
                minute=random.randint(0, 59),
            )
        else:
            # Normal: typical amounts with category-based variation
            category = merchant["category"]
            if category in ("grocery", "gas_station"):
                amount = round(random.uniform(5, 150), 2)
            elif category in ("electronics", "travel"):
                amount = round(random.uniform(50, 800), 2)
            elif category in ("restaurant", "entertainment"):
                amount = round(random.uniform(10, 200), 2)
            else:
                amount = round(random.uniform(10, 500), 2)

        # IP address (simplified)
        ip_address = f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

        # Card present (fraud more likely card-not-present)
        card_present = 0 if (is_fraud and random.random() < 0.8) else int(random.random() < 0.7)

        transactions.append({
            "t_id": f"T{i + 1:06d}",
            "cc_num": cc_num,
            "merchant_id": merchant_id,
            "amount": amount,
            "datetime": txn_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "ip_address": ip_address,
            "card_present": card_present,
            "is_fraud": int(is_fraud),
            "latitude": round(merchant["latitude"] + random.uniform(-0.01, 0.01), 6),
            "longitude": round(merchant["longitude"] + random.uniform(-0.01, 0.01), 6),
        })

    # Sort by datetime for realistic ordering
    transactions.sort(key=lambda t: t["datetime"])
    return transactions


def write_csv(filepath: Path, rows: list[dict]) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows)} rows to {filepath}")


def main() -> None:
    """Generate all CSV data files."""
    print("Generating ccfraud synthetic data...")
    print(f"Output directory: {DATA_DIR}\n")

    # Generate entities in dependency order
    merchants = generate_merchants(50)
    write_csv(DATA_DIR / "merchants.csv", merchants)

    banks = generate_banks(10)
    write_csv(DATA_DIR / "banks.csv", banks)

    accounts = generate_accounts(200, banks=banks)
    write_csv(DATA_DIR / "accounts.csv", accounts)

    cards = generate_cards(300, accounts=accounts)
    write_csv(DATA_DIR / "cards.csv", cards)

    transactions = generate_transactions(5000, cards=cards, merchants=merchants)
    write_csv(DATA_DIR / "transactions.csv", transactions)

    # Summary
    fraud_count = sum(1 for t in transactions if t["is_fraud"])
    print(f"\nGeneration complete:")
    print(f"  Merchants:    {len(merchants)}")
    print(f"  Banks:        {len(banks)}")
    print(f"  Accounts:     {len(accounts)}")
    print(f"  Cards:        {len(cards)}")
    print(f"  Transactions: {len(transactions)} ({fraud_count} fraudulent, {fraud_count / len(transactions) * 100:.1f}%)")


if __name__ == "__main__":
    main()
