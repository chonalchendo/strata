"""Build execution engine for feature table materialization.

Orchestrates compilation, execution, and storage of feature tables
according to their DAG dependencies. BuildEngine takes a single backend
field via discriminated union (BackendKind), matching how EnvironmentSettings
works in settings.py.

The backend handles everything -- connection, source registration,
execution, and output I/O. Table write semantics (append/merge) come
from table.write_mode. Build orchestration (full_refresh, date range)
comes from CLI flags passed as method params.

Validation integration: after execute but before write, data is validated
against Field constraints. Failed validation prevents data from being
written (previous good version remains). Use skip_quality=True to bypass.
"""

from __future__ import annotations

import enum
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pydantic as pdt

import strata.infra as infra
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
    validation_passed: bool | None = None  # None if validation was skipped
    validation_warnings: int = 0  # Count of warn-severity failures


@dataclass
class BuildResult:
    """Aggregate result of a full build run."""

    table_results: list[TableBuildResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        """Number of tables that built successfully."""
        return sum(
            1 for r in self.table_results if r.status == BuildStatus.SUCCESS
        )

    @property
    def failed_count(self) -> int:
        """Number of tables that failed."""
        return sum(
            1 for r in self.table_results if r.status == BuildStatus.FAILED
        )

    @property
    def skipped_count(self) -> int:
        """Number of tables skipped due to upstream failure."""
        return sum(
            1 for r in self.table_results if r.status == BuildStatus.SKIPPED
        )

    @property
    def is_success(self) -> bool:
        """True if all tables built successfully (no failures or skips)."""
        return self.failed_count == 0 and self.skipped_count == 0

    @property
    def validation_count(self) -> int:
        """Number of tables that were validated."""
        return sum(
            1 for r in self.table_results if r.validation_passed is not None
        )

    @property
    def validation_warning_count(self) -> int:
        """Total number of validation warnings across all tables."""
        return sum(r.validation_warnings for r in self.table_results)


class BuildEngine(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Execution engine for materializing feature tables.

    Takes a single backend via discriminated union (BackendKind),
    matching how EnvironmentSettings already works. The backend handles
    connect, register_source, execute, and write_table.

    Write mode comes from table.write_mode (table config). Build
    orchestration (full_refresh, start/end) comes from CLI flags
    passed through as method params.

    Validation: after execute but before write, data is validated
    against Field constraints (unless skip_quality=True). Failed
    validation prevents data from being written.

    Example:
        cfg = settings.load_strata_settings(env="dev")
        env_cfg = cfg.active_environment
        engine = BuildEngine(backend=env_cfg.backend, registry=env_cfg.registry)
        result = engine.build(tables=[user_transactions, user_risk])
    """

    backend: infra.BackendKind = pdt.Field(..., discriminator="kind")
    registry: infra.RegistryKind | None = pdt.Field(
        default=None, discriminator="kind"
    )

    def build(
        self,
        tables: list[core.FeatureTable],
        *,
        targets: list[str] | None = None,
        full_refresh: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
        skip_quality: bool = False,
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
            skip_quality: If True, bypass data validation. Useful for
                development/debugging iteration.

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
            upstream_failed = [
                dep for dep in upstream_deps if dep in failed_tables
            ]

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
                    skip_quality=skip_quality,
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
        skip_quality: bool = False,
    ) -> TableBuildResult:
        """Build a single table: compile, execute, validate, write.

        Validation runs after execute but BEFORE write. Failed validation
        prevents data from being written (previous good version remains).

        Args:
            table: The FeatureTable to build.
            conn: Active Ibis connection from backend.connect().
            compiler: IbisCompiler instance.
            full_refresh: If True, drop table and use overwrite mode.
            start: Start of date range for backfill (inclusive).
            end: End of date range for backfill (exclusive).
            skip_quality: If True, skip validation.

        Returns:
            TableBuildResult with status and metadata.
        """
        import strata.core as core

        start_time = datetime.now()

        try:
            # Register the source if it's an external source (not a FeatureTable)
            if not isinstance(
                table.source, (core.FeatureTable, core.SourceTable)
            ):
                self.backend.register_source(
                    conn=conn,
                    name=table.source_name,
                    config=table.source.config,
                )

            # Compile the table to an Ibis expression
            # Date range filter is applied at the source level (before
            # aggregation) so the timestamp column is still available.
            date_range = (
                (start, end) if start is not None and end is not None else None
            )
            compiled = compiler.compile_table(table, date_range=date_range)

            # Execute the expression via the backend
            data = self.backend.execute(conn, compiled.ibis_expr)

            # Validate data BEFORE writing (unless skip_quality is True)
            validation_passed: bool | None = None
            validation_warnings: int = 0

            if not skip_quality:
                import strata.quality as quality

                validation_result = quality.validate_table(
                    table=table,
                    data=data,
                    sample_pct=table.sample_pct,
                )

                validation_passed = validation_result.passed
                validation_warnings = sum(
                    1
                    for fr in validation_result.field_results
                    for cr in fr.constraints
                    if not cr.passed and cr.severity == "warn"
                )

                # Persist quality result to registry
                self._persist_quality_result(table.name, validation_result)

                if not validation_result.passed:
                    elapsed_ms = (
                        datetime.now() - start_time
                    ).total_seconds() * 1000

                    # Collect failed error-severity constraints for message
                    failed_constraints = []
                    for fr in validation_result.field_results:
                        for cr in fr.constraints:
                            if not cr.passed and cr.severity == "error":
                                failed_constraints.append(
                                    f"{cr.field_name}.{cr.constraint}: "
                                    f"expected {cr.expected}, got {cr.actual}"
                                )

                    error_msg = (
                        f"Validating '{table.name}': "
                        f"data quality check failed. "
                        f"{'; '.join(failed_constraints)}. "
                        f"Fix the data or use --skip-quality to bypass."
                    )
                    logger.error(
                        "Validation failed for '%s': %s",
                        table.name,
                        error_msg,
                    )

                    # Persist build record as failed
                    self._persist_build_record(
                        table_name=table.name,
                        status="failed",
                        row_count=len(data) if data is not None else 0,
                        duration_ms=elapsed_ms,
                        data=data,
                        timestamp_field=table.timestamp_field,
                    )

                    return TableBuildResult(
                        table_name=table.name,
                        status=BuildStatus.FAILED,
                        error=error_msg,
                        duration_ms=elapsed_ms,
                        validation_passed=False,
                        validation_warnings=validation_warnings,
                    )

                # Log warnings if any
                if validation_result.has_warnings:
                    logger.warning(
                        "Validation passed with %d warning(s) for '%s'",
                        validation_warnings,
                        table.name,
                    )

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
                if table.timestamp_field is None:
                    msg = (
                        f"Cannot backfill table '{table.name}': "
                        f"--start/--end requires a timestamp_field."
                    )
                    raise ValueError(msg)
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
                    table.effective_merge_keys
                    if write_mode == "merge"
                    else None
                )
                self.backend.write_table(
                    table_name=table.name,
                    data=data,
                    mode=write_mode,
                    merge_keys=merge_keys,
                )

            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            row_count = len(data) if data is not None else 0

            # Persist build record as success
            self._persist_build_record(
                table_name=table.name,
                status="success",
                row_count=row_count,
                duration_ms=elapsed_ms,
                data=data,
                timestamp_field=table.timestamp_field,
            )

            return TableBuildResult(
                table_name=table.name,
                status=BuildStatus.SUCCESS,
                row_count=row_count,
                duration_ms=elapsed_ms,
                validation_passed=validation_passed,
                validation_warnings=validation_warnings,
            )

        except Exception as exc:
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            logger.error("Build failed for '%s': %s", table.name, exc)

            # Persist build record as failed
            self._persist_build_record(
                table_name=table.name,
                status="failed",
                row_count=None,
                duration_ms=elapsed_ms,
                data=None,
                timestamp_field=table.timestamp_field,
            )

            return TableBuildResult(
                table_name=table.name,
                status=BuildStatus.FAILED,
                error=str(exc),
                duration_ms=elapsed_ms,
            )

    def _persist_quality_result(
        self,
        table_name: str,
        validation_result: Any,
    ) -> None:
        """Persist a quality validation result to the registry."""
        if self.registry is None:
            return

        from dataclasses import asdict

        import strata.registry as reg_types

        record = reg_types.QualityResultRecord(
            id=None,
            timestamp=datetime.now(timezone.utc),
            table_name=table_name,
            passed=validation_result.passed,
            has_warnings=validation_result.has_warnings,
            rows_checked=validation_result.rows_checked,
            results_json=json.dumps(asdict(validation_result), default=str),
        )

        try:
            self.registry.put_quality_result(record)
        except Exception:
            logger.warning(
                "Could not save quality result for '%s'"
                " (run 'strata up' to enable full tracking)",
                table_name,
            )

    def _persist_build_record(
        self,
        table_name: str,
        status: str,
        row_count: int | None,
        duration_ms: float,
        data: Any,
        timestamp_field: str | None,
    ) -> None:
        """Persist a build record to the registry."""
        if self.registry is None:
            return

        import strata.registry as reg_types

        # Extract max data timestamp if available
        data_timestamp_max: str | None = None
        if (
            data is not None
            and timestamp_field is not None
            and timestamp_field in data.column_names
        ):
            try:
                import pyarrow.compute as pc

                max_ts = pc.max(data.column(timestamp_field))
                if max_ts.is_valid:
                    data_timestamp_max = str(max_ts.as_py())
            except Exception:
                pass  # Best-effort timestamp extraction

        record = reg_types.BuildRecord(
            id=None,
            timestamp=datetime.now(timezone.utc),
            table_name=table_name,
            status=status,
            row_count=row_count,
            duration_ms=duration_ms,
            data_timestamp_max=data_timestamp_max,
        )

        try:
            self.registry.put_build_record(record)
        except Exception:
            logger.warning(
                "Could not save build record for '%s'"
                " (run 'strata up' to enable full tracking)",
                table_name,
            )
