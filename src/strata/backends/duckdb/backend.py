"""DuckDB backend -- wraps Ibis DuckDB connection with format-based I/O.

Supports local files, S3, and MotherDuck via database/path configuration.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, override

import pyarrow as pa
import pydantic as pdt

import strata.backends.base as base
import strata.formats as formats

if TYPE_CHECKING:
    import ibis


class DuckDBBackend(base.BaseBackend):
    """DuckDB backend for local development and small-scale production.

    Wraps an Ibis DuckDB connection. Delegates output I/O to its format
    instance (Parquet or Delta). Supports local files, S3 paths, and
    MotherDuck via the database and path configuration.

    Configuration examples:
        Local development:
            DuckDBBackend(path=".strata/data", catalog="features")

        S3 storage:
            DuckDBBackend(
                database=":memory:",
                path="s3://bucket/features",
                catalog="features",
                extensions=["httpfs"],
            )

        MotherDuck:
            DuckDBBackend(
                database="md:my_db",
                path=".strata/data",
                catalog="features",
                motherduck_token="...",
            )
    """

    kind: Literal["duckdb"] = "duckdb"
    database: str = ":memory:"
    extensions: list[str] = pdt.Field(default_factory=list)
    path: str
    catalog: str
    format: Annotated[formats.FormatKind, pdt.Field(discriminator="kind")] = formats.ParquetFormat()
    motherduck_token: str | None = None

    def _table_path(self, table_name: str) -> Path:
        """Resolve the filesystem path for a named table.

        Returns:
            Path like: {path}/{catalog}/{table_name}
        """
        return Path(self.path) / self.catalog / table_name

    @override
    def connect(self) -> "ibis.BaseBackend":
        """Create an Ibis DuckDB connection.

        Installs requested extensions and loads them.
        Sets MotherDuck token if configured.
        """
        import ibis

        conn = ibis.duckdb.connect(database=self.database)

        if self.motherduck_token is not None:
            conn.raw_sql(f"SET motherduck_token = '{self.motherduck_token}'")

        for ext in self.extensions:
            conn.raw_sql(f"INSTALL {ext}")
            conn.raw_sql(f"LOAD {ext}")

        return conn

    @override
    def register_source(
        self,
        conn: "ibis.BaseBackend",
        name: str,
        config: base.BaseSourceConfig,
    ) -> None:
        """Register a source file with DuckDB via Ibis.

        Reads the source file format and registers it as a named table
        in the DuckDB connection.

        Args:
            conn: Active Ibis DuckDB connection.
            name: Table name to register as.
            config: Source configuration with path and format info.
        """
        # BaseSourceConfig subclasses have path and format attributes
        source_path = getattr(config, "path", None)
        source_format = getattr(config, "format", "parquet")

        if source_path is None:
            msg = f"Source config for '{name}' is missing a 'path' attribute."
            raise ValueError(msg)

        if source_format == "parquet":
            conn.read_parquet(source_path, table_name=name)
        elif source_format == "csv":
            conn.read_csv(source_path, table_name=name)
        elif source_format == "json":
            conn.raw_sql(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_json_auto('{source_path}')"
            )
        elif source_format == "delta":
            conn.raw_sql(
                f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM delta_scan('{source_path}')"
            )
        else:
            msg = f"Unsupported source format: {source_format}"
            raise ValueError(msg)

    @override
    def execute(self, conn: "ibis.BaseBackend", expr: "ibis.Expr") -> pa.Table:
        """Execute an Ibis expression via the DuckDB connection.

        Args:
            conn: Active Ibis DuckDB connection.
            expr: Ibis expression tree (from the compiler).

        Returns:
            PyArrow Table with query results.
        """
        return conn.to_pyarrow(expr)

    @override
    def write_table(
        self,
        table_name: str,
        data: pa.Table,
        mode: str = "append",
        merge_keys: list[str] | None = None,
    ) -> None:
        """Write data to a table, delegating I/O to the format.

        Args:
            table_name: Logical table name.
            data: PyArrow Table to write.
            mode: Write mode -- "append" or "merge".
            merge_keys: Keys for merge upsert.
        """
        table_path = self._table_path(table_name)
        self.format.write(
            path=table_path,
            data=data,
            mode=mode,
            merge_keys=merge_keys,
        )

    @override
    def read_table(
        self,
        table_name: str,
        version: int | None = None,
    ) -> pa.Table:
        """Read data from a table, delegating I/O to the format.

        Args:
            table_name: Logical table name.
            version: Optional version for time-travel reads.

        Returns:
            PyArrow Table with the data.
        """
        table_path = self._table_path(table_name)
        return self.format.read(path=table_path, version=version)

    @override
    def drop_table(self, table_name: str) -> None:
        """Remove a table and all its data (file or directory).

        Args:
            table_name: Logical table name to drop.
        """
        table_path = self._table_path(table_name)
        if table_path.is_dir():
            shutil.rmtree(table_path)
        elif table_path.is_file():
            table_path.unlink()

    @override
    def delete_range(
        self,
        table_name: str,
        partition_col: str,
        start: str,
        end: str,
    ) -> None:
        """Delete data within a partition range via format delegation.

        Args:
            table_name: Logical table name.
            partition_col: Column to filter on.
            start: Range start (inclusive).
            end: Range end (exclusive).
        """
        table_path = self._table_path(table_name)
        self.format.delete_range(
            path=table_path,
            partition_col=partition_col,
            start=start,
            end=end,
        )

    @override
    def table_exists(self, table_name: str) -> bool:
        """Check if a table directory exists on disk.

        Args:
            table_name: Logical table name.

        Returns:
            True if the table directory exists.
        """
        return self._table_path(table_name).exists()
