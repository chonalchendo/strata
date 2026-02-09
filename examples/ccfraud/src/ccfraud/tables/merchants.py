"""Merchant source and feature tables for fraud detection.

Defines:
    merchants_source  -- BatchSource for merchant details CSV
    merchants_st      -- SourceTable with merchant schema
    merchant_ft       -- FeatureTable with merchant-level aggregates (online=True)

Merchant features capture per-merchant transaction patterns that help
identify high-risk merchants (e.g., merchants with many transactions
may correlate with fraud rings).
"""

from datetime import timedelta

import ccfraud.entities as entities
import strata as st

# -- Source --

merchants_source = st.BatchSource(
    name="Merchant details",
    description="Merchant reference data with categories and locations",
    config=st.LocalSourceConfig(path="data/merchants.csv", format="csv"),
    timestamp_field="merchant_id",
)


class MerchantsSchema(st.Schema):
    merchant_id = st.Field(description="Merchant ID", dtype="string", not_null=True)
    merchant_name = st.Field(description="Merchant name", dtype="string")
    category = st.Field(description="Merchant category", dtype="string")
    country = st.Field(description="Merchant country", dtype="string")
    latitude = st.Field(description="Merchant latitude", dtype="float64")
    longitude = st.Field(description="Merchant longitude", dtype="float64")


merchants_st = st.SourceTable(
    name="merchants",
    description="Merchant reference data",
    source=merchants_source,
    entity=entities.merchant,
    timestamp_field="merchant_id",
    schema=MerchantsSchema,
)

# -- Feature table: merchant transaction aggregates --
# Uses the transactions source for aggregation at merchant level

txn_source_for_merchant = st.BatchSource(
    name="Transactions for merchant features",
    description="Transaction data aggregated by merchant",
    config=st.LocalSourceConfig(path="data/transactions.csv", format="csv"),
    timestamp_field="datetime",
)

merchant_ft = st.FeatureTable(
    name="merchant_features",
    description="Per-merchant transaction aggregates",
    source=txn_source_for_merchant,
    entity=entities.merchant,
    timestamp_field="datetime",
    schedule="daily",
    online=True,
)

merchant_txn_count_30d = merchant_ft.aggregate(
    name="merchant_txn_count_30d",
    field=st.Field(
        dtype="int64",
        description="Merchant transaction count in last 30 days",
        ge=0,
    ),
    column="amount",
    function="count",
    window=timedelta(days=30),
)
