"""Build execution engine for feature table materialization.

Orchestrates compilation, execution, and storage of feature tables
according to their DAG dependencies. BuildEngine takes a single backend
field via discriminated union (BackendKind), matching how EnvironmentSettings
works in settings.py.

The backend handles everything -- connection, source registration,
execution, and output I/O. Table write semantics (append/merge) come
from table.write_mode. Build orchestration (full_refresh, date range)
comes from CLI flags passed as method params.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

import pydantic as pdt

import strata.backends as backends
import strata.compiler as compiler_mod
import strata.dag as dag_mod

if TYPE_CHECKING:
    import ibis

    import strata.core as core

logger = logging.getLogger(__name__)


class BuildStatus(enum.Enum):
    """Status of a single table build."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class TableBuildResult:
    """Result of building a single table."""

    table_name: str
    status: BuildStatus
    error: str | None = None
    row_count: int | None = None
    duration_ms: float | None = None


@dataclass
class BuildResult:
    """Aggregate result of a full build run."""

    table_results: list[TableBuildResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        """Number of tables that built successfully."""
        return sum(1 for r in self.table_results if r.status == BuildStatus.SUCCESS)

    @property
    def failed_count(self) -> int:
        """Number of tables that failed."""
        return sum(1 for r in self.table_results if r.status == BuildStatus.FAILED)

    @property
    def skipped_count(self) -> int:
        """Number of tables skipped due to upstream failure."""
        return sum(1 for r in self.table_results if r.status == BuildStatus.SKIPPED)

    @property
    def is_success(self) -> bool:
        """True if all tables built successfully (no failures or skips)."""
        return self.failed_count == 0 and self.skipped_count == 0


class BuildEngine(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Execution engine for materializing feature tables.

    Takes a single backend via discriminated union (BackendKind),
    matching how EnvironmentSettings already works. The backend handles
    connect, register_source, execute, and write_table.

    Write mode comes from table.write_mode (table config). Build
    orchestration (full_refresh, start/end) comes from CLI flags
    passed through as method params.

    Example:
        cfg = settings.load_strata_settings(env="dev")
        env_cfg = cfg.active_environment
        engine = BuildEngine(backend=env_cfg.backend)
        result = engine.build(tables=[user_transactions, user_risk])
    """

    backend: backends.BackendKind = pdt.Field(..., discriminator="kind")

    def build(
        self,
        tables: list[core.FeatureTable],
        *,
        targets: list[str] | None = None,
        full_refresh: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> BuildResult:
        """Build feature tables in DAG order.

        Args:
            tables: All FeatureTable definitions to consider.
            targets: Optional list of table names to build. When provided,
                only these tables and their upstream dependencies are built.
            full_refresh: If True, drop and rebuild tables using overwrite
                mode (CLI override).
            start: Start of date range for backfill (inclusive). When
                provided with end, filters data and deletes existing data
                in range before writing.
            end: End of date range for backfill (exclusive).

        Returns:
            BuildResult with per-table status tracking.
        """
        # Build the DAG
        dag = dag_mod.DAG()
        dag.add_tables(tables)

        # Determine execution order
        if targets:
            # Build only target tables and their upstream deps
            build_names: list[str] = []
            for target in targets:
                upstream = dag.get_upstream(target, include_self=True)
                for name in upstream:
                    if name not in build_names:
                        build_names.append(name)
        else:
            build_names = dag.topological_sort()

        # Create compiler and connect
        ibis_compiler = compiler_mod.IbisCompiler()
        conn = self.backend.connect()

        result = BuildResult()
        failed_tables: set[str] = set()

        for table_name in build_names:
            table = dag.get_table(table_name)

            # Check if any upstream dependency failed
            upstream_deps = dag.get_upstream(table_name, include_self=False)
            upstream_failed = [dep for dep in upstream_deps if dep in failed_tables]

            if upstream_failed:
                result.table_results.append(
                    TableBuildResult(
                        table_name=table_name,
                        status=BuildStatus.SKIPPED,
                        error=f"Upstream table(s) failed: {', '.join(upstream_failed)}",
                    )
                )
                failed_tables.add(table_name)
                logger.warning(
                    "Skipping '%s': upstream table(s) failed: %s",
                    table_name,
                    ", ".join(upstream_failed),
                )
                continue

            try:
                table_result = self._build_table(
                    table=table,
                    conn=conn,
                    compiler=ibis_compiler,
                    full_refresh=full_refresh,
                    start=start,
                    end=end,
                )
                result.table_results.append(table_result)
                if table_result.status == BuildStatus.FAILED:
                    failed_tables.add(table_name)
            except Exception as exc:
                result.table_results.append(
                    TableBuildResult(
                        table_name=table_name,
                        status=BuildStatus.FAILED,
                        error=str(exc),
                    )
                )
                failed_tables.add(table_name)
                logger.error("Failed to build '%s': %s", table_name, exc)

        return result

    def _build_table(
        self,
        table: core.FeatureTable,
        conn: ibis.BaseBackend,
        compiler: compiler_mod.IbisCompiler,
        full_refresh: bool,
        start: datetime | None,
        end: datetime | None,
    ) -> TableBuildResult:
        """Build a single table: compile, execute, write.

        Args:
            table: The FeatureTable to build.
            conn: Active Ibis connection from backend.connect().
            compiler: IbisCompiler instance.
            full_refresh: If True, drop table and use overwrite mode.
            start: Start of date range for backfill (inclusive).
            end: End of date range for backfill (exclusive).

        Returns:
            TableBuildResult with status and metadata.
        """
        import strata.core as core

        start_time = datetime.now()

        try:
            # Register the source if it's an external source (not a FeatureTable)
            if not isinstance(table.source, (core.FeatureTable, core.SourceTable)):
                self.backend.register_source(
                    conn=conn,
                    name=table.source_name,
                    config=table.source.config,
                )

            # Compile the table to an Ibis expression
            # Date range filter is applied at the source level (before
            # aggregation) so the timestamp column is still available.
            date_range = (start, end) if start is not None and end is not None else None
            compiled = compiler.compile_table(table, date_range=date_range)

            # Execute the expression via the backend
            data = self.backend.execute(conn, compiled.ibis_expr)

            # Handle full_refresh: drop table first, then write with overwrite
            if full_refresh:
                self.backend.drop_table(table.name)
                self.backend.write_table(
                    table_name=table.name,
                    data=data,
                    mode="overwrite",
                )
            elif start is not None and end is not None:
                # Delete existing data in the range, then append
                self.backend.delete_range(
                    table_name=table.name,
                    partition_col=table.timestamp_field,
                    start=start.isoformat(),
                    end=end.isoformat(),
                )
                self.backend.write_table(
                    table_name=table.name,
                    data=data,
                    mode="append",
                )
            else:
                # Normal write using table's write_mode
                write_mode = table.write_mode
                merge_keys = (
                    table.effective_merge_keys if write_mode == "merge" else None
                )
                self.backend.write_table(
                    table_name=table.name,
                    data=data,
                    mode=write_mode,
                    merge_keys=merge_keys,
                )

            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            row_count = len(data) if data is not None else 0

            return TableBuildResult(
                table_name=table.name,
                status=BuildStatus.SUCCESS,
                row_count=row_count,
                duration_ms=elapsed_ms,
            )

        except Exception as exc:
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            logger.error("Build failed for '%s': %s", table.name, exc)
            return TableBuildResult(
                table_name=table.name,
                status=BuildStatus.FAILED,
                error=str(exc),
                duration_ms=elapsed_ms,
            )
