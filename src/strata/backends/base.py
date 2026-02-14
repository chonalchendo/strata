"""Base classes for Strata backends.

Backends provide implementations for registry and data operations.
Each environment in strata.yaml specifies which backend to use.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

import pyarrow as pa
import pydantic as pdt

import strata.formats as formats

if TYPE_CHECKING:
    import ibis

    import strata.registry as registry


class BaseRegistry(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Registry backend interface.

    Registries store feature metadata and handle state management.
    Concrete implementations (SQLite, DuckDB, etc.) provide the actual storage.

    Methods are defined here with NotImplementedError to allow configuration
    loading without requiring full implementation. This enables incremental
    development across phases.
    """

    def initialize(self) -> None:
        """Create tables if they don't exist.

        Must be called before any other operations. Idempotent.
        """
        raise NotImplementedError("Registry.initialize() not implemented")

    def get_object(self, kind: str, name: str) -> "registry.ObjectRecord | None":
        """Fetch a single object by kind and name.

        Args:
            kind: Object type ("entity", "feature_table", "dataset", "source_table").
            name: Unique name within the kind.

        Returns:
            ObjectRecord if found, None otherwise.
        """
        raise NotImplementedError("Registry.get_object() not implemented")

    def list_objects(self, kind: str | None = None) -> "list[registry.ObjectRecord]":
        """List all objects, optionally filtered by kind.

        Args:
            kind: If provided, filter to only this object type.

        Returns:
            List of ObjectRecord instances.
        """
        raise NotImplementedError("Registry.list_objects() not implemented")

    def put_object(self, obj: "registry.ObjectRecord", applied_by: str) -> None:
        """Upsert an object and log the change.

        If object exists (same kind/name), increments version and logs "update".
        If object is new, sets version=1 and logs "create".

        Args:
            obj: The object record to store.
            applied_by: Identity string (user@hostname) for changelog.
        """
        raise NotImplementedError("Registry.put_object() not implemented")

    def delete_object(self, kind: str, name: str, applied_by: str) -> None:
        """Delete an object and log the change.

        Args:
            kind: Object type.
            name: Object name.
            applied_by: Identity string (user@hostname) for changelog.
        """
        raise NotImplementedError("Registry.delete_object() not implemented")

    def get_meta(self, key: str) -> str | None:
        """Get a metadata value.

        Args:
            key: Metadata key.

        Returns:
            Value if found, None otherwise.
        """
        raise NotImplementedError("Registry.get_meta() not implemented")

    def set_meta(self, key: str, value: str) -> None:
        """Set a metadata value.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        raise NotImplementedError("Registry.set_meta() not implemented")

    def get_changelog(self, limit: int = 100) -> "list[registry.ChangelogEntry]":
        """Get recent changelog entries.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ChangelogEntry instances, newest first.
        """
        raise NotImplementedError("Registry.get_changelog() not implemented")

    def put_quality_result(self, result: "registry.QualityResultRecord") -> None:
        """Store a quality validation result.

        Args:
            result: Quality result record to persist.
        """
        raise NotImplementedError("Registry.put_quality_result() not implemented")

    def get_quality_results(
        self, table_name: str, limit: int = 10
    ) -> "list[registry.QualityResultRecord]":
        """Get recent quality results for a table.

        Args:
            table_name: Name of the table to query results for.
            limit: Maximum number of results to return.

        Returns:
            List of QualityResultRecord instances, newest first.
        """
        raise NotImplementedError("Registry.get_quality_results() not implemented")

    def put_build_record(self, record: "registry.BuildRecord") -> None:
        """Store a build execution record.

        Args:
            record: Build record to persist.
        """
        raise NotImplementedError("Registry.put_build_record() not implemented")

    def get_latest_build(self, table_name: str) -> "registry.BuildRecord | None":
        """Get the most recent build record for a table.

        Args:
            table_name: Name of the table to query.

        Returns:
            Most recent BuildRecord if any exist, None otherwise.
        """
        raise NotImplementedError("Registry.get_latest_build() not implemented")

    def get_build_records(
        self, table_name: str | None = None, limit: int = 10
    ) -> "list[registry.BuildRecord]":
        """Get recent build records, optionally filtered by table.

        Args:
            table_name: If provided, filter to only this table.
            limit: Maximum number of records to return.

        Returns:
            List of BuildRecord instances, newest first.
        """
        raise NotImplementedError("Registry.get_build_records() not implemented")


class BaseBackend(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Backend interface -- single abstraction per deployment target.

    Replaces the old BaseStorage + BaseCompute split. In practice, storage
    and compute are always coupled per deployment target (DuckDB reads parquet
    directly, Databricks reads Delta natively). The backend wraps an Ibis
    connection (compute) and delegates output I/O to a format (storage).

    Ibis IS the compute abstraction. The compiler builds backend-agnostic
    Ibis expressions. The backend handles: connection, source registration,
    execution, and output I/O via format delegation.

    The ``format`` field accepts either a string shorthand (``"delta"``,
    ``"parquet"``) or a full dict with format-specific options::

        # Simple
        format: delta

        # With options
        format:
          kind: delta
          partition_columns: [date]
    """

    kind: str
    format: formats.FormatKind = formats.ParquetFormat()

    @pdt.model_validator(mode="before")
    @classmethod
    def _coerce_format_string(cls, data: Any) -> Any:
        """Coerce ``format: "delta"`` shorthand to ``{"kind": "delta"}``."""
        if isinstance(data, dict) and isinstance(data.get("format"), str):
            data = {**data, "format": {"kind": data["format"]}}
        return data

    def connect(self) -> "ibis.BaseBackend":
        """Create and return an Ibis backend connection.

        Returns:
            Connected Ibis backend instance.
        """
        raise NotImplementedError("Backend.connect() not implemented")

    def register_source(
        self,
        conn: "ibis.BaseBackend",
        name: str,
        config: BaseSourceConfig,
    ) -> None:
        """Register an external data source with the Ibis connection.

        Args:
            conn: Active Ibis connection.
            name: Table name to register as.
            config: Source configuration with connection details.
        """
        raise NotImplementedError("Backend.register_source() not implemented")

    def execute(self, conn: "ibis.BaseBackend", expr: "ibis.Expr") -> pa.Table:
        """Execute an Ibis expression and return results.

        Args:
            conn: Active Ibis connection.
            expr: Ibis expression tree to execute.

        Returns:
            PyArrow Table with query results.
        """
        raise NotImplementedError("Backend.execute() not implemented")

    def write_table(
        self,
        table_name: str,
        data: pa.Table,
        mode: str = "append",
        merge_keys: list[str] | None = None,
    ) -> None:
        """Write data to a named table via format delegation.

        Args:
            table_name: Logical table name.
            data: PyArrow Table to write.
            mode: Write mode -- "append" or "merge".
            merge_keys: Keys for merge upsert.
        """
        raise NotImplementedError("Backend.write_table() not implemented")

    def read_table(
        self,
        table_name: str,
        version: int | None = None,
    ) -> pa.Table:
        """Read data from a named table via format delegation.

        Args:
            table_name: Logical table name.
            version: Optional version for time-travel reads.

        Returns:
            PyArrow Table with the data.
        """
        raise NotImplementedError("Backend.read_table() not implemented")

    def drop_table(self, table_name: str) -> None:
        """Remove a table and its data.

        Args:
            table_name: Logical table name to drop.
        """
        raise NotImplementedError("Backend.drop_table() not implemented")

    def delete_range(
        self,
        table_name: str,
        partition_col: str,
        start: str,
        end: str,
    ) -> None:
        """Delete data within a partition range.

        Args:
            table_name: Logical table name.
            partition_col: Column to filter on.
            start: Range start (inclusive).
            end: Range end (exclusive).
        """
        raise NotImplementedError("Backend.delete_range() not implemented")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Logical table name.

        Returns:
            True if table exists.
        """
        raise NotImplementedError("Backend.table_exists() not implemented")


class BaseSourceConfig(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Base class for source connection configurations.

    Source configs define how to connect to a data source.
    Each backend (DuckDB, S3, Unity Catalog) provides its own config class.
    """

    pass
