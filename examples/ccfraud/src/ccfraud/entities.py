"""Entity definitions for the credit card fraud detection example.

Defines the linked entity hierarchy:
    merchant  -- keyed by merchant_id
    bank      -- keyed by bank_id
    account   -- keyed by account_id
    card      -- keyed by cc_num (primary entity for fraud detection)
"""

import strata as st

merchant = st.Entity(
    name="Merchant",
    description="Merchant where transactions occur",
    join_keys=["merchant_id"],
)

bank = st.Entity(
    name="Bank",
    description="Bank issuing accounts",
    join_keys=["bank_id"],
)

account = st.Entity(
    name="Account",
    description="Customer bank account",
    join_keys=["account_id"],
)

card = st.Entity(
    name="Card",
    description="Credit/debit card used for transactions",
    join_keys=["cc_num"],
)
