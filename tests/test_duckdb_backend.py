"""Integration tests for DuckDBBackend.

Tests backend operations: table paths, format delegation, connect(),
write/read round-trip, drop_table, table_exists.
"""

from pathlib import Path

import pyarrow as pa
import pytest

import strata.backends.duckdb.backend as duckdb_backend
import strata.formats as formats


# --- Test data helpers ---


def _sample_table() -> pa.Table:
    """Create a small PyArrow table for testing."""
    return pa.table(
        {
            "user_id": ["u1", "u2", "u3"],
            "amount": [10.0, 20.0, 30.0],
            "ts": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )


# --- DuckDBBackend configuration tests ---


class TestDuckDBBackendConfig:
    """Test DuckDBBackend configuration and construction."""

    def test_default_format_is_parquet(self, tmp_path):
        """Default format should be ParquetFormat."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        assert isinstance(backend.format, formats.ParquetFormat)
        assert backend.format.kind == "parquet"

    def test_custom_delta_format(self, tmp_path):
        """Backend can be configured with DeltaFormat."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
            format=formats.DeltaFormat(),
        )
        assert isinstance(backend.format, formats.DeltaFormat)

    def test_default_database_is_memory(self, tmp_path):
        """Default database is :memory:."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        assert backend.database == ":memory:"

    def test_kind_is_duckdb(self, tmp_path):
        """Kind discriminator is 'duckdb'."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        assert backend.kind == "duckdb"


# --- Table path resolution ---


class TestTablePath:
    """Test table path resolution."""

    def test_table_path_construction(self, tmp_path):
        """_table_path resolves to {path}/{catalog}/{table_name}."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        result = backend._table_path("user_features")
        expected = tmp_path / "data" / "features" / "user_features"
        assert result == expected


# --- Connect tests ---


class TestConnect:
    """Test Ibis DuckDB connection."""

    def test_connect_returns_ibis_backend(self, tmp_path):
        """connect() returns a working Ibis DuckDB backend."""
        import ibis

        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        conn = backend.connect()
        assert conn is not None
        # Verify it's a working connection by listing tables
        assert isinstance(conn.list_tables(), list)

    def test_connect_with_extensions(self, tmp_path):
        """connect() installs and loads requested extensions."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
            extensions=["json"],
        )
        conn = backend.connect()
        # If extension loaded successfully, connection works
        assert conn is not None


# --- Write and read round-trip ---


class TestWriteRead:
    """Test write/read round-trip with format delegation."""

    def test_parquet_write_read_round_trip(self, tmp_path):
        """Write then read via ParquetFormat preserves data."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
            format=formats.ParquetFormat(),
        )
        table = _sample_table()

        backend.write_table("user_features", table)
        result = backend.read_table("user_features")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.column("user_id").to_pylist() == ["u1", "u2", "u3"]

    def test_delta_write_read_round_trip(self, tmp_path):
        """Write then read via DeltaFormat preserves data."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
            format=formats.DeltaFormat(),
        )
        table = _sample_table()

        backend.write_table("user_features", table)
        result = backend.read_table("user_features")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3


# --- Table lifecycle ---


class TestTableLifecycle:
    """Test table existence checking and dropping."""

    def test_table_exists_false_initially(self, tmp_path):
        """table_exists returns False for non-existent table."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        assert backend.table_exists("nonexistent") is False

    def test_table_exists_true_after_write(self, tmp_path):
        """table_exists returns True after writing data."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        table = _sample_table()
        backend.write_table("user_features", table)

        assert backend.table_exists("user_features") is True

    def test_drop_table_removes_data(self, tmp_path):
        """drop_table removes the table directory."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        table = _sample_table()
        backend.write_table("user_features", table)
        assert backend.table_exists("user_features") is True

        backend.drop_table("user_features")
        assert backend.table_exists("user_features") is False

    def test_drop_nonexistent_table_is_noop(self, tmp_path):
        """drop_table on non-existent table does not raise."""
        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        # Should not raise
        backend.drop_table("nonexistent")


# --- Execute ---


class TestExecute:
    """Test Ibis expression execution."""

    def test_execute_ibis_expression(self, tmp_path):
        """execute() runs an Ibis expression against the DuckDB connection."""
        import ibis

        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        conn = backend.connect()

        # Create a simple in-memory table
        t = ibis.memtable(
            {"x": [1, 2, 3], "y": [10, 20, 30]},
        )
        result = backend.execute(conn, t)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3


# --- Source registration ---


class TestRegisterSource:
    """Test external source registration."""

    def test_register_parquet_source(self, tmp_path):
        """register_source registers a parquet file with DuckDB."""
        import pyarrow.parquet as pq

        # Create a parquet file
        table = _sample_table()
        parquet_path = tmp_path / "source.parquet"
        pq.write_table(table, str(parquet_path))

        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        conn = backend.connect()

        # Create a mock config with path and format
        from strata.backends.local.storage import LocalSourceConfig

        config = LocalSourceConfig(path=str(parquet_path), format="parquet")
        backend.register_source(conn, "my_source", config)

        # The source should now be queryable
        assert "my_source" in conn.list_tables()

    def test_register_csv_source(self, tmp_path):
        """register_source registers a CSV file with DuckDB."""
        # Create a CSV file
        csv_path = tmp_path / "source.csv"
        csv_path.write_text("user_id,amount\nu1,10\nu2,20\n")

        backend = duckdb_backend.DuckDBBackend(
            path=str(tmp_path / "data"),
            catalog="features",
        )
        conn = backend.connect()

        from strata.backends.local.storage import LocalSourceConfig

        config = LocalSourceConfig(path=str(csv_path), format="csv")
        backend.register_source(conn, "csv_source", config)

        assert "csv_source" in conn.list_tables()
