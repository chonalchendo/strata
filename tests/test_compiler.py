"""Tests for the Ibis-based feature compiler."""

from datetime import timedelta

import pytest

import strata.compiler as compiler
import strata.core as core
import strata.errors as errors
import strata.sources as sources
from strata.backends.local.storage import LocalSourceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def multi_key_entity():
    return core.Entity(name="user_device", join_keys=["user_id", "device_id"])


@pytest.fixture
def transaction_source():
    return sources.BatchSource(
        name="transactions",
        config=LocalSourceConfig(path="./data/transactions.parquet"),
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def source_schema():
    """Realistic source schema for compilation tests."""
    return {
        "user_id": "string",
        "amount": "float64",
        "event_timestamp": "datetime",
        "status": "string",
        "merchant_id": "string",
    }


@pytest.fixture
def feature_table(user_entity, transaction_source):
    return core.FeatureTable(
        name="user_transactions",
        source=transaction_source,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def ibis_compiler():
    return compiler.IbisCompiler()


# ---------------------------------------------------------------------------
# CompiledQuery dataclass
# ---------------------------------------------------------------------------


class TestCompiledQuery:
    def test_fields_present(self, feature_table, ibis_compiler, source_schema):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert isinstance(result.sql, str)
        assert result.table_name == "user_transactions"
        assert result.source_tables == ["transactions"]
        assert result.ibis_expr is not None

    def test_compiled_query_is_frozen(
        self, feature_table, ibis_compiler, source_schema
    ):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        with pytest.raises(AttributeError):
            result.sql = "SELECT 1"


# ---------------------------------------------------------------------------
# Empty / no-features table
# ---------------------------------------------------------------------------


class TestEmptyTable:
    def test_compile_empty_table(self, feature_table, ibis_compiler, source_schema):
        """A table with no features should compile to a simple SELECT *."""
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "SELECT" in result.sql
        assert result.table_name == "user_transactions"
        assert result.source_tables == ["transactions"]


# ---------------------------------------------------------------------------
# Aggregate compilation
# ---------------------------------------------------------------------------


class TestAggregateCompilation:
    def test_single_aggregate(self, feature_table, ibis_compiler, source_schema):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "SUM" in result.sql
        assert "spend_90d" in result.sql
        assert "PARTITION BY" in result.sql
        assert "user_id" in result.sql

    def test_multiple_aggregates(self, feature_table, ibis_compiler, source_schema):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        feature_table.aggregate(
            name="txn_count_30d",
            field=core.Field(dtype="int64"),
            column="amount",
            function="count",
            window=timedelta(days=30),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "SUM" in result.sql
        assert "COUNT" in result.sql
        assert "spend_90d" in result.sql
        assert "txn_count_30d" in result.sql

    def test_different_windows_use_range(
        self, feature_table, ibis_compiler, source_schema
    ):
        """Aggregates with different windows should use RANGE BETWEEN."""
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        feature_table.aggregate(
            name="spend_30d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=30),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "RANGE BETWEEN" in result.sql
        assert "90" in result.sql
        assert "30" in result.sql

    def test_multi_join_key_entity(
        self, multi_key_entity, transaction_source, ibis_compiler
    ):
        table = core.FeatureTable(
            name="user_device_events",
            source=transaction_source,
            entity=multi_key_entity,
            timestamp_field="event_timestamp",
        )
        table.aggregate(
            name="event_count",
            field=core.Field(dtype="int64"),
            column="amount",
            function="count",
            window=timedelta(days=30),
        )
        schema = {
            "user_id": "string",
            "device_id": "string",
            "amount": "float64",
            "event_timestamp": "datetime",
        }
        result = ibis_compiler.compile_table(table, source_schema=schema)

        assert "user_id" in result.sql
        assert "device_id" in result.sql

    @pytest.mark.parametrize(
        "function,sql_fragment",
        [
            ("sum", "SUM"),
            ("count", "COUNT"),
            ("avg", "AVG"),
            ("min", "MIN"),
            ("max", "MAX"),
            ("count_distinct", "COUNT(DISTINCT"),
        ],
    )
    def test_aggregation_functions(
        self,
        feature_table,
        ibis_compiler,
        source_schema,
        function,
        sql_fragment,
    ):
        feature_table.aggregate(
            name="result",
            field=core.Field(dtype="float64"),
            column="amount",
            function=function,
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert sql_fragment in result.sql

    def test_unsupported_function_raises(self, feature_table):
        with pytest.raises(errors.StrataError, match="Unsupported aggregation function"):
            feature_table.aggregate(
                name="bad",
                field=core.Field(dtype="float64"),
                column="amount",
                function="median",
                window=timedelta(days=30),
            )


# ---------------------------------------------------------------------------
# Custom feature compilation
# ---------------------------------------------------------------------------


class TestCustomFeatureCompilation:
    def test_custom_feature(self, feature_table, ibis_compiler, source_schema):
        @feature_table.feature(name="is_big", field=core.Field(dtype="bool"))
        def is_big(t):
            return t.amount > 100

        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "is_big" in result.sql
        assert "100" in result.sql

    def test_multiple_custom_features(
        self, feature_table, ibis_compiler, source_schema
    ):
        @feature_table.feature(name="is_big", field=core.Field(dtype="bool"))
        def is_big(t):
            return t.amount > 100

        @feature_table.feature(name="amount_doubled", field=core.Field(dtype="float64"))
        def amount_doubled(t):
            return t.amount * 2

        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "is_big" in result.sql
        assert "amount_doubled" in result.sql


# ---------------------------------------------------------------------------
# Transform compilation
# ---------------------------------------------------------------------------


class TestTransformCompilation:
    def test_transform_filters_data(self, feature_table, ibis_compiler, source_schema):
        @feature_table.transform()
        def filter_valid(t):
            return t.filter(t.amount > 0)

        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        assert "amount" in result.sql
        assert "> 0" in result.sql

    def test_transform_with_aggregate(
        self, feature_table, ibis_compiler, source_schema
    ):
        """Transforms are applied before aggregates."""

        @feature_table.transform()
        def filter_completed(t):
            return t.filter(t.status == "completed")

        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        # Both the filter and the aggregate should be in the SQL
        assert "completed" in result.sql
        assert "SUM" in result.sql
        assert "spend_90d" in result.sql

    def test_multiple_transforms_applied_in_order(
        self, feature_table, ibis_compiler, source_schema
    ):
        @feature_table.transform()
        def filter_positive(t):
            return t.filter(t.amount > 0)

        @feature_table.transform()
        def filter_recent(t):
            return t.filter(t.status == "active")

        result = ibis_compiler.compile_table(feature_table, source_schema=source_schema)

        # Both filters should be present
        assert "> 0" in result.sql
        assert "active" in result.sql


# ---------------------------------------------------------------------------
# SQL determinism
# ---------------------------------------------------------------------------


class TestSQLDeterminism:
    def test_sql_is_deterministic(self, user_entity, transaction_source, ibis_compiler):
        """Same input should produce identical SQL."""

        def _make_table():
            table = core.FeatureTable(
                name="user_transactions",
                source=transaction_source,
                entity=user_entity,
                timestamp_field="event_timestamp",
            )
            table.aggregate(
                name="spend_90d",
                field=core.Field(dtype="float64"),
                column="amount",
                function="sum",
                window=timedelta(days=90),
            )
            table.aggregate(
                name="txn_count_30d",
                field=core.Field(dtype="int64"),
                column="amount",
                function="count",
                window=timedelta(days=30),
            )
            return table

        schema = {
            "user_id": "string",
            "amount": "float64",
            "event_timestamp": "datetime",
        }

        result1 = ibis_compiler.compile_table(_make_table(), source_schema=schema)
        result2 = ibis_compiler.compile_table(_make_table(), source_schema=schema)

        assert result1.sql == result2.sql


# ---------------------------------------------------------------------------
# DAG / derived tables
# ---------------------------------------------------------------------------


class TestDAGCompilation:
    def test_derived_table_references_upstream(
        self, user_entity, transaction_source, ibis_compiler
    ):
        base = core.FeatureTable(
            name="filtered_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived = core.FeatureTable(
            name="user_risk",
            source=base,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived.aggregate(
            name="risk_score",
            field=core.Field(dtype="float64"),
            column="amount",
            function="avg",
            window=timedelta(days=30),
        )
        result = ibis_compiler.compile_table(derived)

        assert "filtered_transactions" in result.sql
        assert result.source_tables == ["filtered_transactions"]
        assert result.table_name == "user_risk"


# ---------------------------------------------------------------------------
# Schema inference (no source_schema provided)
# ---------------------------------------------------------------------------


class TestSchemaInference:
    def test_infer_schema_from_aggregates(self, feature_table, ibis_compiler):
        """Compiler should infer schema from aggregate definitions."""
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        # No source_schema -- rely on inference
        result = ibis_compiler.compile_table(feature_table)

        assert "SUM" in result.sql
        assert "spend_90d" in result.sql

    def test_infer_schema_includes_join_keys(self, feature_table, ibis_compiler):
        feature_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
        result = ibis_compiler.compile_table(feature_table)

        assert "user_id" in result.sql
