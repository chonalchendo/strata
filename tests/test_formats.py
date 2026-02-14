"""Tests for format classes -- read/write logic for each output format."""

from pathlib import Path

import pyarrow as pa
import pytest

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


# --- BaseFormat tests ---


class TestBaseFormat:
    """Test BaseFormat ABC contract."""

    def test_cannot_instantiate_directly(self):
        """BaseFormat is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract"):
            formats.BaseFormat(kind="test")

    def test_delete_range_raises_not_implemented(self):
        """Default delete_range raises NotImplementedError."""
        fmt = formats.ParquetFormat()
        with pytest.raises(NotImplementedError, match="parquet"):
            fmt.delete_range(
                path=Path("/tmp/test"),
                partition_col="ts",
                start="2024-01-01",
                end="2024-02-01",
            )


# --- FormatKind tests ---


class TestFormatKind:
    """Test FormatKind union type."""

    def test_delta_is_format_kind(self):
        """DeltaFormat is part of FormatKind union."""
        fmt = formats.DeltaFormat()
        assert isinstance(fmt, formats.BaseFormat)
        assert fmt.kind == "delta"

    def test_parquet_is_format_kind(self):
        """ParquetFormat is part of FormatKind union."""
        fmt = formats.ParquetFormat()
        assert isinstance(fmt, formats.BaseFormat)
        assert fmt.kind == "parquet"

    def test_default_format_is_parquet(self):
        """When no format specified, default should be ParquetFormat."""
        # This mirrors the BaseBackend default
        default = formats.ParquetFormat()
        assert default.kind == "parquet"
        assert default.compression == "snappy"


# --- ParquetFormat tests ---


class TestParquetFormat:
    """Test ParquetFormat read/write logic."""

    def test_default_compression(self):
        """Default compression is snappy."""
        fmt = formats.ParquetFormat()
        assert fmt.compression == "snappy"

    def test_custom_compression(self):
        """Compression can be overridden."""
        fmt = formats.ParquetFormat(compression="gzip")
        assert fmt.compression == "gzip"

    def test_write_creates_file(self, tmp_path):
        """write() creates a parquet file at the given path."""
        fmt = formats.ParquetFormat()
        table = _sample_table()
        output = tmp_path / "test.parquet"

        fmt.write(path=output, data=table)
        assert output.exists()

    def test_read_returns_table(self, tmp_path):
        """read() returns a PyArrow table from a parquet file."""
        fmt = formats.ParquetFormat()
        table = _sample_table()
        output = tmp_path / "test.parquet"

        fmt.write(path=output, data=table)
        result = fmt.read(path=output)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_round_trip_preserves_data(self, tmp_path):
        """Write then read preserves all data."""
        fmt = formats.ParquetFormat(compression="zstd")
        table = _sample_table()
        output = tmp_path / "test.parquet"

        fmt.write(path=output, data=table)
        result = fmt.read(path=output)

        assert result.column("user_id").to_pylist() == ["u1", "u2", "u3"]
        assert result.column("amount").to_pylist() == [10.0, 20.0, 30.0]

    def test_merge_mode_raises_error(self, tmp_path):
        """Parquet does not support merge writes."""
        fmt = formats.ParquetFormat()
        table = _sample_table()
        output = tmp_path / "test.parquet"

        with pytest.raises(NotImplementedError, match="merge"):
            fmt.write(
                path=output, data=table, mode="merge", merge_keys=["user_id"]
            )

    def test_write_creates_parent_dirs(self, tmp_path):
        """write() creates parent directories if they don't exist."""
        fmt = formats.ParquetFormat()
        table = _sample_table()
        output = tmp_path / "nested" / "deep" / "test.parquet"

        fmt.write(path=output, data=table)
        assert output.exists()

    def test_none_compression(self, tmp_path):
        """compression='none' writes uncompressed parquet."""
        fmt = formats.ParquetFormat(compression="none")
        table = _sample_table()
        output = tmp_path / "test.parquet"

        fmt.write(path=output, data=table)
        result = fmt.read(path=output)
        assert result.num_rows == 3


# --- DeltaFormat tests ---


class TestDeltaFormat:
    """Test DeltaFormat read/write logic."""

    def test_default_settings(self):
        """Default DeltaFormat has expected defaults."""
        fmt = formats.DeltaFormat()
        assert fmt.kind == "delta"
        assert fmt.enable_cdf is False
        assert fmt.partition_columns is None

    def test_custom_settings(self):
        """DeltaFormat accepts custom settings."""
        fmt = formats.DeltaFormat(
            enable_cdf=True,
            partition_columns=["date"],
        )
        assert fmt.enable_cdf is True
        assert fmt.partition_columns == ["date"]

    def test_write_creates_delta_table(self, tmp_path):
        """write() creates a Delta table at the given path."""
        import deltalake as dl

        fmt = formats.DeltaFormat()
        table = _sample_table()
        output = tmp_path / "delta_table"

        fmt.write(path=output, data=table)
        assert dl.DeltaTable.is_deltatable(str(output))

    def test_read_returns_table(self, tmp_path):
        """read() returns a PyArrow table from a Delta table."""
        fmt = formats.DeltaFormat()
        table = _sample_table()
        output = tmp_path / "delta_table"

        fmt.write(path=output, data=table)
        result = fmt.read(path=output)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_append_adds_rows(self, tmp_path):
        """Append mode adds rows to existing Delta table."""
        fmt = formats.DeltaFormat()
        table = _sample_table()
        output = tmp_path / "delta_table"

        fmt.write(path=output, data=table, mode="append")
        fmt.write(path=output, data=table, mode="append")

        result = fmt.read(path=output)
        assert result.num_rows == 6  # 3 + 3

    def test_merge_upserts(self, tmp_path):
        """Merge mode upserts rows by merge keys."""
        fmt = formats.DeltaFormat()
        initial = _sample_table()
        output = tmp_path / "delta_table"

        # Initial write
        fmt.write(
            path=output, data=initial, mode="merge", merge_keys=["user_id"]
        )

        # Update u1's amount, add u4
        update = pa.table(
            {
                "user_id": ["u1", "u4"],
                "amount": [99.0, 40.0],
                "ts": ["2024-01-01", "2024-01-04"],
            }
        )
        fmt.write(
            path=output, data=update, mode="merge", merge_keys=["user_id"]
        )

        result = fmt.read(path=output)
        result_dict = {
            row["user_id"]: row["amount"] for row in result.to_pylist()
        }
        assert result_dict["u1"] == 99.0  # Updated
        assert result_dict["u2"] == 20.0  # Unchanged
        assert result_dict["u4"] == 40.0  # Inserted
        assert result.num_rows == 4

    def test_delete_range(self, tmp_path):
        """delete_range removes rows within the specified range."""
        fmt = formats.DeltaFormat()
        table = _sample_table()
        output = tmp_path / "delta_table"

        fmt.write(path=output, data=table)

        # Delete rows where ts >= '2024-01-02' AND ts < '2024-01-04'
        fmt.delete_range(
            path=output,
            partition_col="ts",
            start="2024-01-02",
            end="2024-01-04",
        )

        result = fmt.read(path=output)
        remaining_ts = result.column("ts").to_pylist()
        assert "2024-01-01" in remaining_ts
        assert "2024-01-02" not in remaining_ts
        assert "2024-01-03" not in remaining_ts

    def test_delete_range_noop_on_empty(self, tmp_path):
        """delete_range on non-existent table is a no-op."""
        fmt = formats.DeltaFormat()
        output = tmp_path / "nonexistent_delta"

        # Should not raise
        fmt.delete_range(
            path=output,
            partition_col="ts",
            start="2024-01-01",
            end="2024-02-01",
        )


# --- Pydantic model tests ---


class TestFormatPydanticBehavior:
    """Test Pydantic model behavior of format classes."""

    def test_frozen_rejects_mutation(self):
        """Formats are frozen -- attributes cannot be changed."""
        fmt = formats.ParquetFormat()
        with pytest.raises(Exception):
            fmt.compression = "gzip"

    def test_extra_fields_rejected(self):
        """Extra fields are rejected (extra='forbid')."""
        with pytest.raises(Exception):
            formats.ParquetFormat(unknown_field="value")

    def test_strict_type_validation(self):
        """Strict mode rejects wrong types."""
        with pytest.raises(Exception):
            formats.ParquetFormat(compression=123)
