from datetime import timedelta

import pytest

import strata.core as core
import strata.sources as sources
from strata.backends.duckdb import DuckDBSourceConfig


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


@pytest.fixture
def feature_table(user_entity, transactions_source):
    return core.FeatureTable(
        name="user_transactions",
        source=transactions_source,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )


class TestAggregate:
    def test_creates_aggregation_feature(self, feature_table):
        feature = feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64", ge=0, not_null=True),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        assert isinstance(feature, core.Feature)
        assert feature.name == "spend_90d"
        assert feature.table_name == "user_transactions"

    def test_feature_accessible_as_attribute(self, feature_table):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        feature = feature_table.spend_90d
        assert feature.name == "spend_90d"

    def test_stores_aggregation_definition(self, feature_table):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64", ge=0),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        assert len(feature_table._aggregates) == 1
        assert feature_table._aggregates[0]["column"] == "amount"
        assert feature_table._aggregates[0]["function"] == "sum"

    def test_feature_has_field(self, feature_table):
        feature = feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64", ge=0, le=10_000_000),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        assert feature.field is not None
        assert feature.field.ge == 0


class TestFeatureDecorator:
    def test_creates_custom_feature(self, feature_table):
        @feature_table.feature(
            name="spend_velocity",
            field=core.Field(dtype="float64"),
        )
        def spend_velocity(t):
            return t.amount  # Simplified for test

        assert isinstance(spend_velocity, core.Feature)
        assert spend_velocity.name == "spend_velocity"

    def test_stores_function(self, feature_table):
        @feature_table.feature(
            name="custom_feature",
            field=core.Field(dtype="float64"),
        )
        def my_feature(t):
            return t.amount * 2

        assert len(feature_table._custom_features) == 1
        assert callable(feature_table._custom_features[0]["func"])

    def test_feature_accessible_as_attribute(self, feature_table):
        @feature_table.feature(
            name="velocity",
            field=core.Field(dtype="float64"),
        )
        def velocity(t):
            return t.amount

        assert feature_table.velocity.name == "velocity"


class TestTransformDecorator:
    def test_registers_transform(self, feature_table):
        @feature_table.transform()
        def filter_valid(t):
            return t  # Simplified for test

        assert len(feature_table._transforms) == 1

    def test_returns_original_function(self, feature_table):
        @feature_table.transform()
        def my_transform(t):
            return t

        assert callable(my_transform)
        assert my_transform.__name__ == "my_transform"

    def test_multiple_transforms(self, feature_table):
        @feature_table.transform()
        def filter_active(t):
            return t

        @feature_table.transform()
        def filter_recent(t):
            return t

        assert len(feature_table._transforms) == 2


class TestFieldValidation:
    def test_field_accepts_all_params(self):
        field = core.Field(
            dtype="float64",
            description="Test field",
            ge=0,
            le=100,
            not_null=True,
            max_null_pct=0.1,
        )
        assert field.dtype == "float64"
        assert field.ge == 0
        assert field.not_null is True

    def test_field_with_allowed_values(self):
        field = core.Field(
            dtype="string",
            allowed_values=["active", "inactive"],
        )
        assert field.allowed_values == ["active", "inactive"]
