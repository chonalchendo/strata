"""DAG resolution for feature table dependencies.

Builds a dependency graph from FeatureTable definitions and provides:
- Topological sort for correct execution order
- Cycle detection with clear error messages
- Upstream/downstream dependency queries
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import strata.errors as errors

if TYPE_CHECKING:
    import strata.core as core


@dataclass
class DAGNode:
    """A node in the feature table dependency graph."""

    name: str
    table: core.FeatureTable
    upstream: list[str] = field(default_factory=list)
    downstream: list[str] = field(default_factory=list)


class DAG:
    """Dependency graph for feature table execution ordering.

    Builds a directed acyclic graph from FeatureTable definitions,
    where edges represent source dependencies (one FeatureTable
    sourcing from another).

    Example:
        dag = DAG()
        dag.add_tables([base_table, derived_table])
        order = dag.topological_sort()  # ["base_table", "derived_table"]
    """

    def __init__(self) -> None:
        self._nodes: dict[str, DAGNode] = {}

    def add_table(self, table: core.FeatureTable) -> None:
        """Add a single FeatureTable to the DAG.

        Resolves upstream dependencies if the table's source
        is another FeatureTable.
        """
        import strata.core as core

        upstream: list[str] = []
        if isinstance(table.source, core.FeatureTable):
            upstream.append(table.source.name)

        self._nodes[table.name] = DAGNode(
            name=table.name,
            table=table,
            upstream=upstream,
        )

        # Update downstream references for existing nodes
        for dep_name in upstream:
            if dep_name in self._nodes:
                if table.name not in self._nodes[dep_name].downstream:
                    self._nodes[dep_name].downstream.append(table.name)

    def add_tables(self, tables: list[core.FeatureTable]) -> None:
        """Add multiple FeatureTables to the DAG.

        Performs a second pass to resolve all downstream references
        after all tables are added.
        """
        for table in tables:
            self.add_table(table)

        # Second pass: ensure all downstream references are resolved
        for node in self._nodes.values():
            for dep_name in node.upstream:
                if dep_name in self._nodes:
                    if node.name not in self._nodes[dep_name].downstream:
                        self._nodes[dep_name].downstream.append(node.name)

    def topological_sort(self) -> list[str]:
        """Return table names in execution order (dependencies first).

        Uses Kahn's algorithm for deterministic ordering.

        Raises:
            StrataError: If a cycle is detected in the dependency graph.
        """
        in_degree: dict[str, int] = {}
        for name, node in self._nodes.items():
            # Only count upstream deps that are actually in the DAG
            in_degree[name] = sum(1 for dep in node.upstream if dep in self._nodes)

        queue = sorted(name for name, degree in in_degree.items() if degree == 0)
        result: list[str] = []

        while queue:
            name = queue.pop(0)
            result.append(name)

            for downstream in sorted(self._nodes[name].downstream):
                if downstream in in_degree:
                    in_degree[downstream] -= 1
                    if in_degree[downstream] == 0:
                        queue.append(downstream)

        if len(result) != len(self._nodes):
            remaining = set(self._nodes.keys()) - set(result)
            raise errors.StrataError(
                context="Building DAG execution order",
                cause=f"Cycle detected involving tables: {', '.join(sorted(remaining))}",
                fix="Remove circular dependencies between feature tables.",
            )

        return result

    def get_upstream(self, table_name: str, *, include_self: bool = True) -> list[str]:
        """Return all upstream dependencies in topological order.

        Args:
            table_name: The table to query dependencies for.
            include_self: Whether to include the table itself (default True).

        Raises:
            StrataError: If the table is not found in the DAG.
        """
        if table_name not in self._nodes:
            raise errors.StrataError(
                context=f"Getting dependencies for '{table_name}'",
                cause="Table not found in DAG",
                fix=f"Ensure '{table_name}' is registered in the DAG.",
            )

        visited: set[str] = set()
        result: list[str] = []

        def _visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            if name in self._nodes:
                for upstream in self._nodes[name].upstream:
                    _visit(upstream)
            result.append(name)

        _visit(table_name)

        if not include_self:
            result.remove(table_name)

        return result

    def get_downstream(
        self, table_name: str, *, include_self: bool = True
    ) -> list[str]:
        """Return all downstream dependents in topological order.

        Args:
            table_name: The table to query dependents for.
            include_self: Whether to include the table itself (default True).

        Raises:
            StrataError: If the table is not found in the DAG.
        """
        if table_name not in self._nodes:
            raise errors.StrataError(
                context=f"Getting dependents for '{table_name}'",
                cause="Table not found in DAG",
                fix=f"Ensure '{table_name}' is registered in the DAG.",
            )

        visited: set[str] = set()
        result: list[str] = []

        def _visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            result.append(name)
            if name in self._nodes:
                for downstream in self._nodes[name].downstream:
                    _visit(downstream)

        _visit(table_name)

        if not include_self:
            result.remove(table_name)

        return result

    def get_table(self, name: str) -> core.FeatureTable:
        """Retrieve a FeatureTable by name.

        Raises:
            StrataError: If the table is not found in the DAG.
        """
        if name not in self._nodes:
            raise errors.StrataError(
                context=f"Getting table '{name}'",
                cause="Table not found in DAG",
                fix=f"Ensure '{name}' is registered in the DAG.",
            )
        return self._nodes[name].table

    @property
    def nodes(self) -> dict[str, DAGNode]:
        """Read-only access to the internal nodes dictionary."""
        return dict(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, name: str) -> bool:
        return name in self._nodes
