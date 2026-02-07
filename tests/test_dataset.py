import pytest

import strata.core as core


@pytest.fixture
def feature_refs():
    return [
        core.Feature(name="spend_90d", table_name="user_transactions"),
        core.Feature(name="txn_count", table_name="user_transactions"),
        core.Feature(name="lifetime_value", table_name="customer_features"),
    ]


class TestDataset:
    def test_creates_with_features(self, feature_refs):
        dataset = core.Dataset(
            name="fraud_detection",
            features=feature_refs,
        )
        assert dataset.name == "fraud_detection"
        assert len(dataset.features) == 3

    def test_output_columns_prefixed_by_default(self, feature_refs):
        dataset = core.Dataset(name="fraud_detection", features=feature_refs)
        columns = dataset.output_columns()
        assert columns == [
            "user_transactions__spend_90d",
            "user_transactions__txn_count",
            "customer_features__lifetime_value",
        ]

    def test_output_columns_short_names_when_disabled(self, feature_refs):
        dataset = core.Dataset(
            name="fraud_detection",
            features=feature_refs,
            prefix_features=False,
        )
        columns = dataset.output_columns()
        assert columns == ["spend_90d", "txn_count", "lifetime_value"]

    def test_alias_overrides_prefix(self, feature_refs):
        aliased_ref = feature_refs[0].alias("total_spend")
        dataset = core.Dataset(
            name="fraud_detection",
            features=[aliased_ref, feature_refs[1]],
        )
        columns = dataset.output_columns()
        assert columns[0] == "total_spend"  # Alias wins
        assert columns[1] == "user_transactions__txn_count"  # Default prefix

    def test_tables_referenced(self, feature_refs):
        dataset = core.Dataset(name="fraud_detection", features=feature_refs)
        tables = dataset.tables_referenced()
        assert tables == {"user_transactions", "customer_features"}

    def test_rejects_duplicate_output_columns(self):
        refs = [
            core.Feature(name="amount", table_name="table1"),
            core.Feature(name="amount", table_name="table2"),
        ]
        with pytest.raises(ValueError, match="Duplicate output column name"):
            core.Dataset(name="test", features=refs, prefix_features=False)

    def test_accepts_optional_metadata(self, feature_refs):
        dataset = core.Dataset(
            name="fraud_detection",
            features=feature_refs,
            description="Fraud detection features",
            owner="ml-team",
            tags={"domain": "fraud"},
        )
        assert dataset.description == "Fraud detection features"
        assert dataset.owner == "ml-team"


class TestFullSDKIntegration:
    """Test the complete SDK workflow."""

    def test_entity_to_dataset_workflow(self):
        from datetime import timedelta
        from strata.backends.duckdb import DuckDBSourceConfig
        import strata.sources as sources

        # 1. Define entity
        user = core.Entity(name="user", join_keys=["user_id"])

        # 2. Define source
        transactions = sources.BatchSource(
            name="transactions",
            config=DuckDBSourceConfig(path="./data/transactions.parquet"),
            timestamp_field="event_timestamp",
        )

        # 3. Define feature table
        user_txn = core.FeatureTable(
            name="user_transactions",
            source=transactions,
            entity=user,
            timestamp_field="event_timestamp",
        )

        # 4. Define features
        spend = user_txn.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64", ge=0),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        # 5. Create dataset
        fraud_ds = core.Dataset(
            name="fraud_detection",
            features=[spend],
        )

        assert fraud_ds.output_columns() == ["user_transactions__spend_90d"]
