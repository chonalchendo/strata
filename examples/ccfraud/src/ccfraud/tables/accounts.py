"""Account source table for fraud detection.

Defines:
    accounts_source  -- BatchSource for account details CSV
    accounts_st      -- SourceTable with account schema

Account data links customers to banks and provides home country
information used in geographic risk analysis.
"""

import ccfraud.entities as entities
import strata as st

# -- Source --

accounts_source = st.BatchSource(
    name="Account details",
    description="Customer account reference data",
    config=st.LocalSourceConfig(path="data/accounts.csv", format="csv"),
    timestamp_field="account_id",
)


class AccountsSchema(st.Schema):
    account_id = st.Field(description="Account ID", dtype="string", not_null=True)
    bank_id = st.Field(description="Bank ID (FK)", dtype="string", not_null=True)
    home_country = st.Field(description="Account holder home country", dtype="string")
    email = st.Field(description="Account holder email", dtype="string")


accounts_st = st.SourceTable(
    name="accounts",
    description="Customer account reference data",
    source=accounts_source,
    entity=entities.account,
    timestamp_field="account_id",
    schema=AccountsSchema,
)
