"""Dataset definition for credit card fraud detection.

Groups features from transaction and merchant feature tables into a single
Dataset used for training and inference pipelines. The is_fraud field serves
as the label for supervised learning.
"""

import ccfraud.tables.merchants as merchant_tables
import ccfraud.tables.transactions as txn_tables
import strata as st

fraud_detection = st.Dataset(
    name="fraud_detection",
    description="Features for credit card fraud detection model",
    label=txn_tables.txn_features_ft.is_fraud,
    features=[
        txn_tables.txn_features_ft.txn_count_1d,
        txn_tables.txn_features_ft.txn_count_7d,
        txn_tables.txn_features_ft.txn_count_30d,
        txn_tables.txn_features_ft.txn_amount_sum_1d,
        txn_tables.txn_features_ft.txn_amount_sum_7d,
        txn_tables.txn_features_ft.txn_amount_avg_30d,
        merchant_tables.merchant_ft.merchant_txn_count_30d,
    ],
)
