"""Transaction source and feature tables for fraud detection.

Defines:
    transactions_source  -- BatchSource for raw transaction CSV
    transactions_st      -- SourceTable with transaction schema
    txn_features_ft      -- FeatureTable with windowed aggregates (online=True)

The FeatureTable has online=True to enable online serving via strata publish.
Aggregates compute per-card transaction patterns at 1d, 7d, 30d windows.
"""

from datetime import timedelta

import ccfraud.entities as entities
import strata as st

# -- Source --

transactions_source = st.BatchSource(
    name="Credit card transactions",
    description="Raw credit card transaction data",
    config=st.LocalSourceConfig(path="data/transactions.csv", format="csv"),
    timestamp_field="datetime",
)


class TransactionsSchema(st.Schema):
    t_id = st.Field(description="Transaction ID", dtype="string", not_null=True)
    cc_num = st.Field(description="Credit card number", dtype="string", not_null=True)
    merchant_id = st.Field(description="Merchant ID", dtype="string", not_null=True)
    amount = st.Field(description="Transaction amount", dtype="float64", ge=0)
    datetime = st.Field(description="Transaction timestamp", dtype="datetime")
    ip_address = st.Field(description="IP address of transaction", dtype="string")
    card_present = st.Field(description="Whether card was physically present", dtype="int64")
    is_fraud = st.Field(description="Fraud label (0=legit, 1=fraud)", dtype="int64")
    latitude = st.Field(description="Transaction latitude", dtype="float64")
    longitude = st.Field(description="Transaction longitude", dtype="float64")


transactions_st = st.SourceTable(
    name="transactions",
    description="Raw credit card transactions",
    source=transactions_source,
    entity=entities.card,
    timestamp_field="datetime",
    schema=TransactionsSchema,
)

# -- Feature table: per-card transaction aggregates --

txn_features_ft = st.FeatureTable(
    name="transaction_features",
    description="Per-card transaction aggregates for fraud detection",
    source=transactions_source,
    entity=entities.card,
    timestamp_field="datetime",
    schedule="hourly",
    online=True,
)

# Transaction count windows
txn_count_1d = txn_features_ft.aggregate(
    name="txn_count_1d",
    field=st.Field(dtype="int64", description="Transaction count in last 1 day", ge=0),
    column="amount",
    function="count",
    window=timedelta(days=1),
)

txn_count_7d = txn_features_ft.aggregate(
    name="txn_count_7d",
    field=st.Field(dtype="int64", description="Transaction count in last 7 days", ge=0),
    column="amount",
    function="count",
    window=timedelta(days=7),
)

txn_count_30d = txn_features_ft.aggregate(
    name="txn_count_30d",
    field=st.Field(dtype="int64", description="Transaction count in last 30 days", ge=0),
    column="amount",
    function="count",
    window=timedelta(days=30),
)

# Transaction amount sum windows
txn_amount_sum_1d = txn_features_ft.aggregate(
    name="txn_amount_sum_1d",
    field=st.Field(dtype="float64", description="Total amount in last 1 day", ge=0),
    column="amount",
    function="sum",
    window=timedelta(days=1),
)

txn_amount_sum_7d = txn_features_ft.aggregate(
    name="txn_amount_sum_7d",
    field=st.Field(dtype="float64", description="Total amount in last 7 days", ge=0),
    column="amount",
    function="sum",
    window=timedelta(days=7),
)

# Average amount (30d)
txn_amount_avg_30d = txn_features_ft.aggregate(
    name="txn_amount_avg_30d",
    field=st.Field(dtype="float64", description="Average transaction amount in last 30 days"),
    column="amount",
    function="avg",
    window=timedelta(days=30),
)

# Is-fraud label (latest value via 1d avg -- used as Dataset label)
is_fraud = txn_features_ft.aggregate(
    name="is_fraud",
    field=st.Field(dtype="float64", description="Fraud label (0=legit, 1=fraud)"),
    column="is_fraud",
    function="avg",
    window=timedelta(days=1),
)
