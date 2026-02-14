"""Freshness monitoring for feature tables.

Calculates staleness by comparing build recency and data recency
against SLA thresholds.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import strata.checks as checks
    import strata.core as core
    import strata.registry as registry_types


@dataclass(frozen=True)
class TableFreshness:
    """Freshness status for a single table."""

    table_name: str
    last_build_at: datetime | None  # When the last build ran
    data_timestamp_max: datetime | None  # Newest timestamp in built data
    build_staleness: timedelta | None  # Time since last build
    data_staleness: timedelta | None  # Time since newest data point
    max_staleness: timedelta | None  # SLA threshold
    status: Literal["fresh", "warn", "error", "unknown"]
    severity: Literal["warn", "error"]  # From SLA
    row_count: int | None = None
    min_row_count: int | None = None


@dataclass(frozen=True)
class FreshnessResult:
    """Aggregate freshness status across tables."""

    tables: list[TableFreshness]
    has_stale: bool  # Any table exceeds SLA threshold
    has_unknown: bool  # Any table has no build records


def check_freshness(
    tables: list[core.FeatureTable],
    build_records: dict[str, registry_types.BuildRecord | None],
    now: datetime | None = None,
) -> FreshnessResult:
    """Check freshness of feature tables against SLA thresholds.

    For each table, calculates build staleness (time since last build) and
    data staleness (time since newest data point). The worse of the two is
    compared against the SLA max_staleness threshold to determine status.

    Args:
        tables: Feature tables to check.
        build_records: Mapping of table name to latest build record (None if never built).
        now: Override current time for testing. Defaults to UTC now.

    Returns:
        FreshnessResult with per-table freshness and aggregate flags.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    results: list[TableFreshness] = []
    has_stale = False
    has_unknown = False

    for table in tables:
        record = build_records.get(table.name)
        sla: checks.SLA | None = table.sla

        # Extract SLA properties
        max_staleness = sla.max_staleness if sla else None
        severity: Literal["warn", "error"] = sla.severity if sla else "warn"
        min_row_count = sla.min_row_count if sla else None

        if record is None:
            # No build record -- status unknown
            results.append(
                TableFreshness(
                    table_name=table.name,
                    last_build_at=None,
                    data_timestamp_max=None,
                    build_staleness=None,
                    data_staleness=None,
                    max_staleness=max_staleness,
                    status="unknown",
                    severity=severity,
                    row_count=None,
                    min_row_count=min_row_count,
                )
            )
            has_unknown = True
            continue

        # Calculate build staleness
        last_build_at = record.timestamp
        # Ensure timezone-aware comparison
        if last_build_at.tzinfo is None:
            last_build_at = last_build_at.replace(tzinfo=timezone.utc)
        build_staleness = now - last_build_at

        # Calculate data staleness (if data_timestamp_max is available)
        data_timestamp_max: datetime | None = None
        data_staleness: timedelta | None = None
        if record.data_timestamp_max is not None:
            data_timestamp_max = datetime.fromisoformat(
                record.data_timestamp_max
            )
            if data_timestamp_max.tzinfo is None:
                data_timestamp_max = data_timestamp_max.replace(
                    tzinfo=timezone.utc
                )
            data_staleness = now - data_timestamp_max

        # Determine status based on SLA
        status: Literal["fresh", "warn", "error", "unknown"] = "fresh"

        if max_staleness is not None:
            # Use the worse (larger) of the two staleness values
            effective_staleness = build_staleness
            if (
                data_staleness is not None
                and data_staleness > effective_staleness
            ):
                effective_staleness = data_staleness

            if effective_staleness > max_staleness:
                status = severity  # "warn" or "error" based on SLA severity
                has_stale = True

        # Also check min_row_count if SLA defines it
        if min_row_count is not None and record.row_count is not None:
            if record.row_count < min_row_count:
                # Row count violation uses SLA severity
                if status == "fresh":
                    status = severity
                    has_stale = True

        results.append(
            TableFreshness(
                table_name=table.name,
                last_build_at=last_build_at,
                data_timestamp_max=data_timestamp_max,
                build_staleness=build_staleness,
                data_staleness=data_staleness,
                max_staleness=max_staleness,
                status=status,
                severity=severity,
                row_count=record.row_count,
                min_row_count=min_row_count,
            )
        )

    return FreshnessResult(
        tables=results,
        has_stale=has_stale,
        has_unknown=has_unknown,
    )
