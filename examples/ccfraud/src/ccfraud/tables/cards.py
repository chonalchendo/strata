"""Card source table for fraud detection.

Defines:
    cards_source  -- BatchSource for card details CSV
    cards_st      -- SourceTable with card schema

Card data links credit cards to accounts and provides card metadata
(type, brand, issue/expiry dates).
"""

import ccfraud.entities as entities
import strata as st

# -- Source --

cards_source = st.BatchSource(
    name="Card details",
    description="Credit/debit card reference data",
    config=st.LocalSourceConfig(path="data/cards.csv", format="csv"),
)


class CardsSchema(st.Schema):
    cc_num = st.Field(description="Credit card number", dtype="string", not_null=True)
    account_id = st.Field(description="Account ID (FK)", dtype="string", not_null=True)
    issue_date = st.Field(description="Card issue date", dtype="string")
    expiry_date = st.Field(description="Card expiry date", dtype="string")
    card_type = st.Field(description="Card type (debit/credit/prepaid)", dtype="string")
    brand = st.Field(description="Card brand (visa/mastercard/amex)", dtype="string")


cards_st = st.SourceTable(
    name="cards",
    description="Credit/debit card reference data",
    source=cards_source,
    entity=entities.card,
    schema=CardsSchema,
)
