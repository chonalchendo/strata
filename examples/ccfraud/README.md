# Credit Card Fraud Detection Example

End-to-end fraud detection pipeline demonstrating online feature serving with Strata. Features are batch-computed, published to the SQLite online store, and served in real-time via FastAPI using `lookup_features()`.

## Architecture

```
Data Generation (scripts/generate_data.py)
  -> Synthetic merchants, banks, accounts, cards, transactions (linked entity model)

Feature Pipeline (strata up + build)
  -> Materialises transaction_features (per-card aggregates: count/sum 1d/7d/30d)
  -> Materialises merchant_features (per-merchant transaction count 30d)

Training Pipeline (pipelines/training.py)
  -> read_features() for 2024-01-01 to 2024-08-01
  -> Trains XGBClassifier with class-weight balancing
  -> Saves model to models/fraud_model.joblib

Online Store (strata publish)
  -> Syncs online=True feature tables to SQLite online store

Serving (app/api.py + app/frontend.py)
  -> FastAPI endpoint: lookup_features() -> model.predict() -> fraud probability
  -> Streamlit dashboard: visual risk indicators and feature display
```

## Data Model

The linked entity hierarchy mirrors real-world credit card systems:

```
merchants (merchant_id PK)
  |
banks (bank_id PK)
  |
accounts (account_id PK, bank_id FK)
  |
cards (cc_num PK, account_id FK)
  |
transactions (t_id PK, cc_num FK, merchant_id FK)
```

## Project Structure

```
src/ccfraud/
  entities.py           -- Entity definitions (Merchant, Bank, Account, Card)
  tables/
    transactions.py     -- Transaction SourceTable + FeatureTable (online=True)
    merchants.py        -- Merchant SourceTable + FeatureTable (online=True)
    accounts.py         -- Account SourceTable
    cards.py            -- Card SourceTable
  datasets/
    fraud_detection.py  -- Dataset: 7 transaction features + is_fraud label
  pipelines/
    training.py         -- XGBoost training via read_features()
    inference.py        -- Batch inference via read_features() + write_table()
  app/
    api.py              -- FastAPI endpoint using lookup_features()
    frontend.py         -- Streamlit dashboard
scripts/
  generate_data.py      -- Synthetic data generator (5000 transactions, 5% fraud)
  run_pipeline.py       -- End-to-end orchestrator
```

## Quick Start

```bash
# From the examples/ccfraud/ directory:
python scripts/run_pipeline.py
```

Or run individual steps:

```bash
python scripts/generate_data.py      # Generate synthetic data
strata up --yes                       # Sync definitions
strata build                          # Materialize feature tables
python -c "from ccfraud.pipelines.training import train; train()"
strata publish                        # Sync to online store

# Serve predictions
uvicorn ccfraud.app.api:app --port 8000
streamlit run src/ccfraud/app/frontend.py
```

## Features

The `fraud_detection` Dataset includes:
- `txn_count_1d` -- 1-day transaction count per card
- `txn_count_7d` -- 7-day transaction count per card
- `txn_count_30d` -- 30-day transaction count per card
- `txn_count_90d` -- 90-day transaction count per card
- `txn_amount_sum_1d` -- 1-day total spend per card
- `txn_amount_sum_7d` -- 7-day total spend per card
- `txn_amount_avg_30d` -- 30-day average transaction amount per card
- **Label**: `is_fraud` -- fraud indicator (prediction target)
