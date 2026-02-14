import pytest

import strata.core as core
import strata.sources as sources
from strata.infra.backends.duckdb import DuckDBSourceConfig


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def customer_source():
    return sources.BatchSource(
        name="customer_data",
        config=DuckDBSourceConfig(path="./features.parquet"),
        timestamp_field="updated_at",
    )


class CustomerSchema(core.Schema):
    lifetime_value = core.Field(dtype="float64", ge=0)
    churn_risk = core.Field(dtype="float64", ge=0, le=1)


class TestSourceTable:
    def test_creates_with_required_fields(self, user_entity, customer_source):
        table = core.SourceTable(
            name="customer_features",
            source=customer_source,
            entity=user_entity,
            timestamp_field="updated_at",
        )
        assert table.name == "customer_features"
        assert table.entity.name == "user"

    def test_creates_with_schema(self, user_entity, customer_source):
        table = core.SourceTable(
            name="customer_features",
            source=customer_source,
            entity=user_entity,
            timestamp_field="updated_at",
            schema=CustomerSchema,
        )
        assert table.schema_ == CustomerSchema

    def test_feature_access_via_attribute(self, user_entity, customer_source):
        table = core.SourceTable(
            name="customer_features",
            source=customer_source,
            entity=user_entity,
            timestamp_field="updated_at",
            schema=CustomerSchema,
        )
        feature = table.lifetime_value
        assert feature.name == "lifetime_value"
        assert feature.table_name == "customer_features"

    def test_feature_access_unknown_raises_error(
        self, user_entity, customer_source
    ):
        table = core.SourceTable(
            name="customer_features",
            source=customer_source,
            entity=user_entity,
            timestamp_field="updated_at",
            schema=CustomerSchema,
        )
        with pytest.raises(AttributeError, match="has no feature 'unknown'"):
            _ = table.unknown

    def test_features_list(self, user_entity, customer_source):
        table = core.SourceTable(
            name="customer_features",
            source=customer_source,
            entity=user_entity,
            timestamp_field="updated_at",
            schema=CustomerSchema,
        )
        features = table.features_list()
        names = [f.name for f in features]
        assert "lifetime_value" in names
        assert "churn_risk" in names


class TestFeature:
    def test_output_name_with_table(self):
        feature = core.Feature(name="spend", table_name="user_txn")
        assert feature.output_name == "user_txn__spend"

    def test_alias_overrides_output_name(self):
        feature = core.Feature(name="spend", table_name="user_txn")
        aliased = feature.alias("total_spend")
        assert aliased.output_name == "total_spend"
        # Original unchanged
        assert feature.output_name == "user_txn__spend"

    def test_qualified_name(self):
        feature = core.Feature(name="spend", table_name="user_txn")
        assert feature.qualified_name == "user_txn.spend"
