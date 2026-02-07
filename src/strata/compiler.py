"""Ibis-based compiler for feature definitions.

Compiles FeatureTable definitions into Ibis expression trees and SQL.
The compiler is backend-agnostic -- it builds expressions, not connections.
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import TYPE_CHECKING

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir

if TYPE_CHECKING:
    import strata.core as core


# Map function names to ibis aggregation methods
_AGG_FUNCTIONS: dict[str, str] = {
    "sum": "sum",
    "count": "count",
    "avg": "mean",
    "min": "min",
    "max": "max",
    "count_distinct": "nunique",
}

# Map strata dtype strings to ibis data types
_DTYPE_MAP: dict[str, dt.DataType] = {
    "int64": dt.int64,
    "int32": dt.int32,
    "float64": dt.float64,
    "float32": dt.float32,
    "string": dt.string,
    "bool": dt.boolean,
    "datetime": dt.timestamp,
    "date": dt.date,
}


@dataclasses.dataclass(frozen=True)
class CompiledQuery:
    """Result of compiling a FeatureTable to SQL via Ibis."""

    sql: str
    ibis_expr: ir.Table
    table_name: str
    source_tables: list[str]


class IbisCompiler:
    """Compiles FeatureTable definitions to Ibis expressions and SQL.

    Stateless compiler -- does not manage backend connections.
    Builds Ibis expression trees from FeatureTable definitions and
    renders them to SQL using the DuckDB dialect.
    """

    def compile_table(
        self,
        table: core.FeatureTable,
        source_schema: dict[str, str] | None = None,
        date_range: tuple[object, object] | None = None,
    ) -> CompiledQuery:
        """Compile a FeatureTable into a CompiledQuery.

        Args:
            table: The FeatureTable to compile.
            source_schema: Optional column-name-to-dtype mapping from the
                actual data source. When provided (e.g. by the build engine),
                the compiler uses the real schema instead of inferring one.
                Keys are column names, values are strata dtype strings
                (e.g. ``{"user_id": "string", "amount": "float64"}``).
            date_range: Optional (start, end) tuple for filtering source data
                by the table's timestamp_field. Applied before transforms and
                aggregation so that the timestamp column is still available.
                Start is inclusive, end is exclusive.

        Returns:
            CompiledQuery with SQL, Ibis expression, table name,
            and list of source table names.
        """
        expr = self._build_expression(
            table, source_schema=source_schema, date_range=date_range
        )
        sql = self._to_sql(expr)
        source_tables = self._extract_source_tables(table)

        return CompiledQuery(
            sql=sql,
            ibis_expr=expr,
            table_name=table.name,
            source_tables=source_tables,
        )

    def _build_expression(
        self,
        table: core.FeatureTable,
        source_schema: dict[str, str] | None = None,
        date_range: tuple[object, object] | None = None,
    ) -> ir.Table:
        """Build an Ibis expression tree from a FeatureTable.

        Execution order: source -> date_range_filter -> transforms -> aggregates/custom_features
        """
        expr = self._create_source_expression(table, source_schema=source_schema)

        # Apply date range filter before transforms (timestamp col still available)
        if date_range is not None:
            start, end = date_range
            ts_col = expr[table.timestamp_field]
            expr = expr.filter((ts_col >= start) & (ts_col < end))

        # Apply transforms first (filter/reshape the source)
        expr = self._apply_transforms(expr, table)

        # Apply aggregates if present
        if table._aggregates:
            expr = self._apply_aggregates(expr, table)

        # Apply custom features
        expr = self._apply_custom_features(expr, table)

        return expr

    def _create_source_expression(
        self,
        table: core.FeatureTable,
        source_schema: dict[str, str] | None = None,
    ) -> ir.Table:
        """Create the base Ibis table expression from the source.

        When *source_schema* is provided the compiler uses the real column
        types.  Otherwise it infers a minimal schema from the FeatureTable
        definition (join keys, timestamp, aggregate columns).
        """
        import strata.core as core_module

        if source_schema is not None:
            schema = {
                col: _DTYPE_MAP.get(dtype, dt.string)
                for col, dtype in source_schema.items()
            }
        else:
            schema = self._infer_schema(table)

        if isinstance(table.source, core_module.FeatureTable):
            return ibis.table(schema=schema, name=table.source.name)
        return ibis.table(schema=schema, name=table.source_name)

    def _infer_schema(self, table: core.FeatureTable) -> dict[str, dt.DataType]:
        """Infer an Ibis schema from FeatureTable column references.

        Collects columns from entity join keys, timestamp field, and
        aggregate source columns. Defaults to string type for join keys
        and timestamp type for the timestamp field.
        """
        schema: dict[str, dt.DataType] = {}

        # Entity join keys (default to string)
        for key in table.entity.join_keys:
            schema[key] = dt.string

        # Timestamp field
        schema[table.timestamp_field] = dt.timestamp

        # Aggregate source columns -- infer type from field dtype if available
        for agg_def in table._aggregates:
            column = agg_def["column"]
            field = agg_def["field"]
            if column not in schema:
                schema[column] = _DTYPE_MAP.get(field.dtype, dt.float64)

        return schema

    def _apply_transforms(self, expr: ir.Table, table: core.FeatureTable) -> ir.Table:
        """Apply registered @transform functions in order."""
        for transform_func in table._transforms:
            expr = transform_func(expr)
        return expr

    def _apply_aggregates(self, expr: ir.Table, table: core.FeatureTable) -> ir.Table:
        """Compile aggregate() definitions to GROUP BY with FILTER.

        Uses conditional aggregation (FILTER WHERE) to support
        multiple aggregates with different time windows in a single query.
        """
        group_keys = list(table.entity.join_keys)
        agg_exprs: dict[str, ir.Column] = {}

        for agg_def in table._aggregates:
            name: str = agg_def["name"]
            column: str = agg_def["column"]
            function: str = agg_def["function"]
            window: timedelta = agg_def["window"]

            if function not in _AGG_FUNCTIONS:
                msg = (
                    f"Unsupported aggregation function '{function}'. "
                    f"Supported: {', '.join(sorted(_AGG_FUNCTIONS))}."
                )
                raise ValueError(msg)

            ibis_method = _AGG_FUNCTIONS[function]
            col_ref = expr[column]

            # Build time window filter
            ts_col = expr[table.timestamp_field]
            window_filter = ts_col >= ibis.now() - ibis.interval(days=window.days)

            # Call the ibis aggregation method with a where clause
            agg_col = getattr(col_ref, ibis_method)(where=window_filter)
            agg_exprs[name] = agg_col

        return expr.group_by(group_keys).agg(**agg_exprs)

    def _apply_custom_features(
        self, expr: ir.Table, table: core.FeatureTable
    ) -> ir.Table:
        """Apply @feature decorator functions as column expressions."""
        for custom_def in table._custom_features:
            name: str = custom_def["name"]
            func = custom_def["func"]
            col_expr = func(expr)
            expr = expr.mutate(**{name: col_expr})
        return expr

    def _to_sql(self, expr: ir.Table) -> str:
        """Render an Ibis expression to DuckDB-dialect SQL."""
        return ibis.to_sql(expr, dialect="duckdb")

    def _extract_source_tables(self, table: core.FeatureTable) -> list[str]:
        """Extract the names of all source tables for dependency tracking."""
        import strata.core as core_module

        source_tables: list[str] = []
        if isinstance(table.source, core_module.FeatureTable):
            source_tables.append(table.source.name)
        else:
            source_tables.append(table.source_name)
        return source_tables
