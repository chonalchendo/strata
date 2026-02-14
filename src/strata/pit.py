"""Point-in-time join engine using Ibis ASOF JOIN expressions.

Prevents future data leakage in training datasets by ensuring each observation
only sees features that were available at that point in time. Uses Ibis for
backend-agnostic PIT logic -- the same expressions compile to DuckDB ASOF JOIN
locally and window function equivalents on other backends.
"""

from __future__ import annotations

import dataclasses
import decimal
from datetime import timedelta

import ibis
import ibis.expr.types as ir
import pyarrow as pa


@dataclasses.dataclass(frozen=True)
class FeatureTableData:
    """Metadata and data for a feature table participating in a PIT join.

    Attributes:
        name: Unique name for this feature table.
        data: PyArrow table containing feature data.
        entity_keys: Column names used to match entities across tables.
        timestamp_column: Column name containing feature timestamps.
        feature_columns: Column names of features to include in the join.
        ttl: Optional time-to-live. Features older than TTL relative to
            the spine timestamp are nulled out (expired).
    """

    name: str
    data: pa.Table
    entity_keys: list[str]
    timestamp_column: str
    feature_columns: list[str]
    ttl: timedelta | None = None


def _ensure_ibis_duckdb() -> None:
    """Suppress decimal.InvalidOperation trap before importing ibis.duckdb.

    Works around a known issue where sqlglot's Oracle compiler triggers
    decimal.InvalidOperation on Python 3.14+ when ibis loads all SQL
    compiler backends.
    """
    ctx = decimal.getcontext()
    ctx.traps[decimal.InvalidOperation] = False


def _create_default_connection() -> ibis.BaseBackend:
    """Create an in-memory DuckDB connection via Ibis."""
    _ensure_ibis_duckdb()
    return ibis.duckdb.connect()


def _apply_ttl(
    expr: ir.Table,
    spine_ts_col: str,
    feature_ts_col: str,
    feature_columns: list[str],
    ttl: timedelta,
) -> ir.Table:
    """Null out feature columns where the feature is older than TTL.

    Compares spine timestamp against feature timestamp. If the difference
    exceeds TTL, all feature columns for that row are set to null.

    Args:
        expr: Ibis table expression after ASOF JOIN.
        spine_ts_col: Column name of the spine timestamp.
        feature_ts_col: Column name of the feature timestamp.
        feature_columns: Feature column names to null on expiry.
        ttl: Maximum age of a feature relative to spine timestamp.

    Returns:
        Ibis table expression with expired features nulled out.
    """
    ttl_seconds = int(ttl.total_seconds())
    cutoff = expr[spine_ts_col] - ibis.interval(seconds=ttl_seconds)
    # Feature is expired if its timestamp is before the cutoff,
    # or if the feature timestamp is null (no match found).
    expired = expr[feature_ts_col].isnull() | (expr[feature_ts_col] < cutoff)

    # Build replacements: null out each feature column where expired
    replacements = {}
    for col in feature_columns:
        replacements[col] = ibis.ifelse(expired, ibis.null(), expr[col])

    return expr.mutate(**replacements)


def pit_join(
    spine: pa.Table,
    feature_tables: list[FeatureTableData],
    connection: ibis.BaseBackend | None = None,
    spine_timestamp: str = "event_ts",
) -> pa.Table:
    """Execute PIT joins for multiple feature tables onto a spine.

    Uses Ibis ASOF JOIN expressions for backend-agnostic point-in-time
    correctness. Defaults to an in-memory DuckDB connection if none provided.

    For each row in the spine, finds the most recent feature row where
    feature.timestamp <= spine.timestamp, preventing future data leakage.

    Args:
        spine: Events table with entity keys and timestamps.
        feature_tables: List of feature table data with metadata.
        connection: Ibis backend connection. Defaults to in-memory DuckDB.
        spine_timestamp: Name of the timestamp column in the spine table.

    Returns:
        pa.Table with spine columns + all feature columns.
    """
    if connection is None:
        connection = _create_default_connection()

    # Register spine as an Ibis table
    current = connection.create_table("__pit_spine__", spine)

    # Track columns to keep in the final result
    result_columns = list(spine.column_names)

    for i, ft in enumerate(feature_tables):
        table_alias = f"__pit_feat_{i}__"
        feat_ibis = connection.create_table(table_alias, ft.data)

        # Build ASOF JOIN: most recent feature where feat.ts <= spine.ts
        joined = current.asof_join(
            feat_ibis,
            on=current[spine_timestamp] >= feat_ibis[ft.timestamp_column],
            predicates=[
                current[ft.entity_keys[0]] == feat_ibis[ft.entity_keys[0]]
                for _ in [None]
            ]
            if len(ft.entity_keys) == 1
            else [current[key] == feat_ibis[key] for key in ft.entity_keys],
        )

        # Apply TTL enforcement if specified
        if ft.ttl is not None:
            # We need the feature timestamp column for TTL check.
            # After asof_join, the right-side timestamp may be renamed.
            # Determine what the feature timestamp column is called in joined.
            feat_ts_in_joined = ft.timestamp_column
            if ft.timestamp_column in spine.column_names:
                # Ibis renames right-side columns with _right suffix on conflict
                feat_ts_in_joined = f"{ft.timestamp_column}_right"

            joined = _apply_ttl(
                expr=joined,
                spine_ts_col=spine_timestamp,
                feature_ts_col=feat_ts_in_joined,
                feature_columns=ft.feature_columns,
                ttl=ft.ttl,
            )

        # Select only the columns we want: current result columns + new features
        select_cols = list(result_columns) + list(ft.feature_columns)
        current = joined.select(*select_cols)
        result_columns = select_cols

    return current.to_pyarrow()
