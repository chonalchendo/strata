import pytest

import strata.core as core
import strata.sources as sources
from strata.plugins.duckdb import DuckDBSourceConfig


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def transactions_source():
    return sources.BatchSource(
        name="transactions",
        config=DuckDBSourceConfig(path="./data/transactions.parquet"),
        timestamp_field="event_timestamp",
    )


class TestFeatureTable:
    def test_creates_with_batch_source(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        assert table.name == "user_transactions"
        assert table.entity.name == "user"
        assert table.timestamp_field == "event_timestamp"
        assert not table.is_derived

    def test_creates_with_schedule(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
            schedule="hourly",
        )
        assert table.schedule == "hourly"

    def test_creates_with_feature_table_source_dag(self, user_entity, transactions_source):
        # Base table
        base_table = core.FeatureTable(
            name="filtered_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        # Derived table (DAG)
        derived_table = core.FeatureTable(
            name="user_transactions",
            source=base_table,  # FeatureTable as source
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        assert derived_table.is_derived
        assert derived_table.source_name == "filtered_transactions"

    def test_source_name_with_batch_source(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        assert table.source_name == "transactions"

    def test_accepts_optional_metadata(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
            description="User transaction features",
            owner="data-science-team",
            tags={"domain": "financial"},
        )
        assert table.description == "User transaction features"
        assert table.owner == "data-science-team"

    def test_features_list_empty_initially(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        assert table.features_list() == []

    def test_unknown_feature_raises_error(self, user_entity, transactions_source):
        table = core.FeatureTable(
            name="user_transactions",
            source=transactions_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        with pytest.raises(AttributeError, match="has no feature"):
            _ = table.some_feature
