"""Format classes for reading and writing feature data.

Each format class owns its serialization logic. Backends delegate
output I/O to a format instance rather than implementing read/write directly.
"""

from __future__ import annotations

import abc
from pathlib import Path
from typing import Literal, override

import pyarrow as pa
import pyarrow.parquet as pq
import pydantic as pdt


class BaseFormat(abc.ABC, pdt.BaseModel, frozen=True, strict=True, extra="forbid"):
    """Abstract base for data formats.

    Each concrete format handles its own read/write logic.
    Backends pass resolved paths; formats handle serialization.
    """

    kind: str

    @abc.abstractmethod
    def read(
        self,
        path: Path,
        version: int | None = None,
        timestamp: str | None = None,
    ) -> pa.Table:
        """Read data from the given path.

        Args:
            path: Resolved filesystem path to the data.
            version: Optional version for time-travel reads (Delta only).
            timestamp: Optional timestamp for time-travel reads (Delta only).

        Returns:
            PyArrow Table with the data.
        """
        ...

    @abc.abstractmethod
    def write(
        self,
        path: Path,
        data: pa.Table,
        mode: str = "append",
        merge_keys: list[str] | None = None,
    ) -> None:
        """Write data to the given path.

        Args:
            path: Resolved filesystem path for output.
            data: PyArrow Table to write.
            mode: Write mode -- "append" or "merge".
            merge_keys: Keys for merge upsert (required when mode="merge").
        """
        ...

    def delete_range(
        self,
        path: Path,
        partition_col: str,
        start: str,
        end: str,
    ) -> None:
        """Delete data within a partition range.

        Only supported by formats with ACID delete (e.g. Delta).
        Default raises NotImplementedError.

        Args:
            path: Resolved filesystem path to the data.
            partition_col: Column to filter on.
            start: Range start value (inclusive).
            end: Range end value (exclusive).
        """
        msg = f"delete_range not supported for format: {self.kind}"
        raise NotImplementedError(msg)


class DeltaFormat(BaseFormat):
    """Delta Lake format with ACID transactions and time-travel.

    Supports append, merge (upsert), and range deletes.
    """

    kind: Literal["delta"] = "delta"

    enable_cdf: bool = False  # Enable Change Data Feed for CDC tracking
    partition_columns: list[str] | None = None

    @override
    def read(
        self,
        path: Path,
        version: int | None = None,
        timestamp: str | None = None,
    ) -> pa.Table:
        """Read a Delta table, optionally at a specific version or timestamp."""
        import deltalake as dl

        dt = dl.DeltaTable(str(path), version=version)
        if timestamp is not None:
            dt.load_as_version(timestamp)
        return dt.to_pyarrow_table()

    @override
    def write(
        self,
        path: Path,
        data: pa.Table,
        mode: str = "append",
        merge_keys: list[str] | None = None,
    ) -> None:
        """Write data to a Delta table.

        For mode="merge", performs an upsert using merge_keys.
        For mode="append", appends data to the table.
        """
        import deltalake as dl

        str_path = str(path)

        if mode == "merge" and merge_keys:
            if dl.DeltaTable.is_deltatable(str_path):
                dt = dl.DeltaTable(str_path)
                predicate = " AND ".join(
                    f"target.{k} = source.{k}" for k in merge_keys
                )
                (
                    dt.merge(
                        source=data,
                        predicate=predicate,
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
            else:
                # First write -- create the table
                dl.write_deltalake(
                    str_path,
                    data,
                    mode="overwrite",
                    partition_by=self.partition_columns,
                )
        else:
            # Append mode
            dl.write_deltalake(
                str_path,
                data,
                mode="append",
                partition_by=self.partition_columns,
            )

    @override
    def delete_range(
        self,
        path: Path,
        partition_col: str,
        start: str,
        end: str,
    ) -> None:
        """Delete rows from a Delta table within a partition range."""
        import deltalake as dl

        str_path = str(path)
        if not dl.DeltaTable.is_deltatable(str_path):
            return  # Nothing to delete

        dt = dl.DeltaTable(str_path)
        predicate = f"{partition_col} >= '{start}' AND {partition_col} < '{end}'"
        dt.delete(predicate)


class ParquetFormat(BaseFormat):
    """Parquet format for columnar storage.

    Supports append writes. Merge writes are not natively supported
    -- use Delta for merge/upsert semantics.
    """

    kind: Literal["parquet"] = "parquet"

    compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"

    @override
    def read(
        self,
        path: Path,
        version: int | None = None,
        timestamp: str | None = None,
    ) -> pa.Table:
        """Read a Parquet file or directory of Parquet files."""
        if path.is_dir():
            return pq.read_table(str(path))
        return pq.read_table(str(path))

    @override
    def write(
        self,
        path: Path,
        data: pa.Table,
        mode: str = "append",
        merge_keys: list[str] | None = None,
    ) -> None:
        """Write data to a Parquet file.

        For append mode, overwrites the file (Parquet doesn't support
        native appends -- the caller should manage file naming).
        Merge mode is not supported for Parquet.
        """
        if mode == "merge":
            msg = (
                "Parquet format does not support merge writes. "
                "Use Delta format for upsert semantics."
            )
            raise NotImplementedError(msg)

        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            data,
            str(path),
            compression=self.compression if self.compression != "none" else None,
        )


FormatKind = DeltaFormat | ParquetFormat
