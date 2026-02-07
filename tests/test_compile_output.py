"""Tests for compile output generation."""

import json
from datetime import timedelta

import pytest

import strata.compile_output as compile_output
import strata.compiler as compiler_mod
import strata.core as core
import strata.discovery as discovery
import strata.sources as sources
from strata.backends.local.storage import LocalSourceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def transaction_source():
    return sources.BatchSource(
        name="transactions",
        config=LocalSourceConfig(path="./data/transactions.parquet"),
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def feature_table(user_entity, transaction_source):
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
    return table


@pytest.fixture
def compiled_query(feature_table):
    """Compile the feature table using the real compiler."""
    ibis_compiler = compiler_mod.IbisCompiler()
    return ibis_compiler.compile_table(feature_table)


@pytest.fixture
def disc_object(feature_table):
    """Create a DiscoveredObject for the feature table."""
    return discovery.DiscoveredObject(
        kind="feature_table",
        name="user_transactions",
        obj=feature_table,
        source_file="tables/user_features.py",
    )


# ---------------------------------------------------------------------------
# Output directory creation
# ---------------------------------------------------------------------------


class TestOutputDirectory:
    def test_creates_output_directory_per_table(
        self, tmp_path, compiled_query, disc_object
    ):
        """Output directory should be created for each table."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        assert table_dir.exists()
        assert table_dir.name == "user_transactions"

    def test_creates_nested_directory(self, tmp_path, compiled_query, disc_object):
        """Should create parent directories if they don't exist."""
        output_dir = tmp_path / "nested" / "compiled"
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=output_dir,
            env="dev",
            strata_version="0.1.0",
        )

        assert table_dir.exists()


# ---------------------------------------------------------------------------
# query.sql
# ---------------------------------------------------------------------------


class TestQuerySql:
    def test_writes_query_sql(self, tmp_path, compiled_query, disc_object):
        """Should write query.sql with compiled SQL."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        query_path = table_dir / "query.sql"
        assert query_path.exists()

        content = query_path.read_text()
        assert "SELECT" in content
        assert "user_transactions" in content
        assert "tables/user_features.py" in content

    def test_query_sql_contains_real_sql(self, tmp_path, compiled_query, disc_object):
        """query.sql should contain actual compiled SQL, not placeholders."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        content = (table_dir / "query.sql").read_text()
        # Should contain real SQL from the compiler (has SUM for aggregate)
        assert "SUM" in content
        assert "spend_90d" in content


# ---------------------------------------------------------------------------
# ibis_expr.txt
# ---------------------------------------------------------------------------


class TestIbisExpr:
    def test_writes_ibis_expr(self, tmp_path, compiled_query, disc_object):
        """Should write ibis_expr.txt with Ibis expression."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        ibis_path = table_dir / "ibis_expr.txt"
        assert ibis_path.exists()

        content = ibis_path.read_text()
        # Ibis expression repr should contain table/aggregation info
        assert len(content) > 0


# ---------------------------------------------------------------------------
# lineage.json
# ---------------------------------------------------------------------------


class TestLineageJson:
    def test_writes_lineage_json(self, tmp_path, compiled_query, disc_object):
        """Should write lineage.json with source tables."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        lineage_path = table_dir / "lineage.json"
        assert lineage_path.exists()

        lineage = json.loads(lineage_path.read_text())
        assert lineage["table"] == "user_transactions"
        assert lineage["source_file"] == "tables/user_features.py"
        assert "transactions" in lineage["source_tables"]
        assert lineage["entity"] == "user"
        assert "spend_90d" in lineage["aggregates"]

    def test_lineage_json_has_source_info(
        self, tmp_path, compiled_query, disc_object
    ):
        """Lineage should include source reference."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        lineage = json.loads((table_dir / "lineage.json").read_text())
        assert lineage["source"]["type"] == "batch_source"
        assert lineage["source"]["name"] == "transactions"


# ---------------------------------------------------------------------------
# build_context.json
# ---------------------------------------------------------------------------


class TestBuildContextJson:
    def test_writes_build_context_json(self, tmp_path, compiled_query, disc_object):
        """Should write build_context.json with all metadata fields."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
            registry_serial=42,
        )

        context_path = table_dir / "build_context.json"
        assert context_path.exists()

        context = json.loads(context_path.read_text())
        assert context["env"] == "dev"
        assert context["strata_version"] == "0.1.0"
        assert context["registry_serial"] == 42
        assert "compiled_at" in context
        assert "table_spec_hash" in context
        assert context["source"] is not None

    def test_build_context_compiled_at_is_iso(
        self, tmp_path, compiled_query, disc_object
    ):
        """compiled_at should be an ISO timestamp."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        context = json.loads((table_dir / "build_context.json").read_text())
        # ISO format should contain T separator and timezone info
        assert "T" in context["compiled_at"]

    def test_build_context_table_spec_hash(
        self, tmp_path, compiled_query, disc_object
    ):
        """table_spec_hash should be a short hex string."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        context = json.loads((table_dir / "build_context.json").read_text())
        spec_hash = context["table_spec_hash"]
        assert len(spec_hash) == 8
        assert all(c in "0123456789abcdef" for c in spec_hash)

    def test_build_context_registry_serial_optional(
        self, tmp_path, compiled_query, disc_object
    ):
        """registry_serial should be None when not provided."""
        table_dir = compile_output.write_compile_output(
            compiled=compiled_query,
            disc=disc_object,
            output_dir=tmp_path,
            env="dev",
            strata_version="0.1.0",
        )

        context = json.loads((table_dir / "build_context.json").read_text())
        assert context["registry_serial"] is None
