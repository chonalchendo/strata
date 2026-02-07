"""Ibis-based compiler for feature definitions.

Compiles FeatureTable definitions into Ibis expression trees and SQL.
The compiler is backend-agnostic -- it builds expressions, not connections.
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import TYPE_CHECKING

import ibis
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

    def compile_table(self, table: core.FeatureTable) -> CompiledQuery:
        """Compile a FeatureTable into a CompiledQuery.

        Args:
            table: The FeatureTable to compile.

        Returns:
            CompiledQuery with SQL, Ibis expression, table name,
            and list of source table names.
        """
        expr = self._build_expression(table)
        sql = self._to_sql(expr)
        source_tables = self._extract_source_tables(table)

        return CompiledQuery(
            sql=sql,
            ibis_expr=expr,
            table_name=table.name,
            source_tables=source_tables,
        )

    def _build_expression(self, table: core.FeatureTable) -> ir.Table:
        """Build an Ibis expression tree from a FeatureTable.

        Execution order: source -> transforms -> aggregates/custom_features
        """
        expr = self._create_source_expression(table)

        # Apply transforms first (filter/reshape the source)
        expr = self._apply_transforms(expr, table)

        # Apply aggregates if present
        if table._aggregates:
            expr = self._apply_aggregates(expr, table)

        # Apply custom features
        expr = self._apply_custom_features(expr, table)

        return expr

    def _create_source_expression(self, table: core.FeatureTable) -> ir.Table:
        """Create the base Ibis table expression from the source."""
        import strata.core as core_module

        if isinstance(table.source, core_module.FeatureTable):
            # DAG dependency -- reference the upstream table by name
            return ibis.table(name=table.source.name)
        return ibis.table(name=table.source_name)

    def _apply_transforms(self, expr: ir.Table, table: core.FeatureTable) -> ir.Table:
        """Apply registered @transform functions in order."""
        for transform_func in table._transforms:
            expr = transform_func(expr)
        return expr

    def _apply_aggregates(
        self, expr: ir.Table, table: core.FeatureTable
    ) -> ir.Table:
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
