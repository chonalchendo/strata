"""Discovery of feature definitions from Python modules.

This module provides the DefinitionDiscoverer class for scanning Python files
and extracting Strata SDK objects (Entity, FeatureTable, SourceTable, Dataset).

Design note: The class-based approach enables future caching wrappers without
changing consumer code. Current implementation has NO caching - measure first.
"""

from __future__ import annotations

import fnmatch
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import strata.core as core
import strata.settings as settings


@dataclass
class DiscoveredObject:
    """A discovered feature definition."""

    kind: str  # "entity", "feature_table", "source_table", "dataset"
    name: str
    obj: Any  # The actual SDK object
    source_file: str  # Path where it was defined


class DefinitionDiscoverer:
    """Discovers feature definitions from Python modules.

    This class provides a clear interface that can later be wrapped with
    CachingDiscoverer if needed. Current implementation is synchronous
    and uncached - premature optimization is avoided.

    Usage:
        discoverer = DefinitionDiscoverer(strata_settings)
        objects = discoverer.discover_all()
    """

    def __init__(
        self,
        strata_settings: settings.StrataSettings | None = None,
        project_root: Path | None = None,
    ):
        """Initialize discoverer.

        Args:
            strata_settings: Strata configuration (loads from strata.yaml if None)
            project_root: Project root directory (derived from config if None)
        """
        self._settings = strata_settings

        if project_root is None and strata_settings is not None:
            project_root = (
                Path(strata_settings._config_path).parent
                if strata_settings._config_path
                else Path.cwd()
            )
        elif project_root is None:
            project_root = Path.cwd()

        self._project_root = project_root

    def discover_all(self) -> list[DiscoveredObject]:
        """Discover all feature definitions in the project.

        Uses smart discovery (scanning all Python files with exclusion patterns)
        or legacy discovery (scanning specific directories) depending on config.
        """
        if self._settings is None:
            return []

        paths = self._settings.paths
        if isinstance(paths, settings.SmartPathsSettings):
            return self._discover_smart(paths)
        return self._discover_legacy(paths)

    def _discover_legacy(
        self, paths: settings.LegacyPathsSettings
    ) -> list[DiscoveredObject]:
        """Legacy discovery: scan specific directories (tables/, datasets/, entities/).

        Maintained for backward compatibility with existing configurations.
        """
        discovered: list[DiscoveredObject] = []

        # Scan each configured path
        for _path_name, rel_path in [
            ("entities", paths.entities),
            ("tables", paths.tables),
            ("datasets", paths.datasets),
        ]:
            full_path = self._project_root / rel_path
            if full_path.exists():
                discovered.extend(self._scan_directory(full_path))

        return discovered

    def _discover_smart(
        self, paths: settings.SmartPathsSettings
    ) -> list[DiscoveredObject]:
        """Smart discovery: scan all Python files with intelligent exclusions.

        Scans all .py files, uses isinstance() to find SDK objects, and
        applies default + custom exclusion patterns to skip test files,
        virtual environments, etc.
        """
        discovered: list[DiscoveredObject] = []

        # Combine default exclusions with custom excludes
        exclude_patterns = list(paths.DEFAULT_EXCLUDES) + list(paths.exclude)

        # Determine scan roots
        if paths.include:
            scan_roots = [self._project_root / inc for inc in paths.include]
        else:
            scan_roots = [self._project_root]

        for root in scan_roots:
            if not root.exists():
                continue

            for py_file in root.rglob("*.py"):
                # Skip files starting with underscore
                if py_file.name.startswith("_"):
                    continue

                if not self._should_exclude(py_file, exclude_patterns):
                    discovered.extend(self._extract_from_module(py_file))

        return discovered

    def _should_exclude(
        self, py_file: Path, exclude_patterns: list[str]
    ) -> bool:
        """Check if a file should be excluded based on patterns.

        Patterns can match:
        - Just the filename (e.g., "test_*.py", "conftest.py")
        - Full path patterns (e.g., "**/tests/**", "**/venv/**")
        """
        # Get path relative to project root for pattern matching
        try:
            rel_path = py_file.relative_to(self._project_root)
        except ValueError:
            # File is outside project root, use absolute path
            rel_path = py_file

        # Use forward slashes for cross-platform compatibility
        rel_path_str = str(rel_path).replace("\\", "/")

        for pattern in exclude_patterns:
            # Check filename match (e.g., "test_*.py")
            if fnmatch.fnmatch(py_file.name, pattern):
                return True

            # Check if any path component matches a directory pattern
            # For patterns like "**/tests/**" or "**/venv/**"
            if "**" in pattern:
                # Extract the directory name from patterns like "**/tests/**"
                # This handles patterns where we want to exclude any path
                # containing a specific directory
                pattern_parts = pattern.replace("\\", "/").split("/")
                for part in pattern_parts:
                    if part and part != "**" and "*" not in part:
                        # Check if this directory name appears in the path
                        if part in rel_path.parts:
                            return True

            # Check full path match using fnmatch (for non-** patterns)
            if fnmatch.fnmatch(rel_path_str, pattern):
                return True

        return False

    def _scan_directory(self, directory: Path) -> list[DiscoveredObject]:
        """Scan a directory for Python files and extract definitions."""
        discovered: list[DiscoveredObject] = []

        for py_file in directory.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue
            discovered.extend(self._extract_from_module(py_file))

        return discovered

    def _extract_from_module(self, py_file: Path) -> list[DiscoveredObject]:
        """Import a Python file and extract SDK objects."""
        module_name = f"_strata_discovery_{py_file.stem}_{id(py_file)}"

        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            return []

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        # Map SDK types to their kind names
        sdk_types = {
            core.Entity: "entity",
            core.FeatureTable: "feature_table",
            core.SourceTable: "source_table",
            core.Dataset: "dataset",
        }

        discovered: list[DiscoveredObject] = []
        try:
            spec.loader.exec_module(module)

            for name in dir(module):
                if name.startswith("_"):
                    continue
                obj = getattr(module, name)

                for sdk_type, kind in sdk_types.items():
                    if isinstance(obj, sdk_type):
                        discovered.append(
                            DiscoveredObject(
                                kind=kind,
                                name=obj.name,
                                obj=obj,
                                source_file=str(py_file),
                            )
                        )
                        break
        finally:
            del sys.modules[module_name]

        return discovered


# Convenience function for simpler API
def discover_definitions(
    strata_settings: settings.StrataSettings | None = None,
    project_root: Path | None = None,
) -> list[DiscoveredObject]:
    """Discover all feature definitions in the project.

    Convenience wrapper around DefinitionDiscoverer.discover_all().
    """
    discoverer = DefinitionDiscoverer(strata_settings, project_root)
    return discoverer.discover_all()


# =============================================================================
# Serialization Functions
# =============================================================================


def serialize_to_spec(obj: Any, kind: str) -> dict[str, Any]:
    """Convert SDK object to canonical spec dictionary.

    The spec contains only the definition, not runtime state.
    References to other objects use names, not the objects themselves.
    """
    if kind == "entity":
        return _serialize_entity(obj)
    elif kind == "feature_table":
        return _serialize_feature_table(obj)
    elif kind == "source_table":
        return _serialize_source_table(obj)
    elif kind == "dataset":
        return _serialize_dataset(obj)
    else:
        msg = f"Unknown kind: {kind}"
        raise ValueError(msg)


def spec_to_json(spec: dict[str, Any]) -> str:
    """Convert spec dict to canonical JSON string.

    Sorted keys, no extra whitespace, deterministic output.
    """
    return json.dumps(spec, sort_keys=True, separators=(",", ":"))


def _serialize_entity(entity: core.Entity) -> dict[str, Any]:
    """Serialize Entity to spec."""
    return {
        "name": entity.name,
        "description": entity.description,
        "join_keys": entity.join_keys,
    }


def _serialize_feature_table(table: core.FeatureTable) -> dict[str, Any]:
    """Serialize FeatureTable to spec."""

    # Determine source reference
    source_ref = _get_source_reference(table.source)

    spec: dict[str, Any] = {
        "name": table.name,
        "description": table.description,
        "source": source_ref,
        "entity": table.entity.name,  # Reference by name
        "timestamp_field": table.timestamp_field,
        "schedule": table.schedule,
        "owner": table.owner,
        "tags": table.tags,
    }

    # Include aggregates if defined
    aggregates = table._aggregates
    if aggregates:
        spec["aggregates"] = [
            {
                "name": agg["name"],
                "column": agg["column"],
                "function": agg["function"],
                "window_seconds": int(agg["window"].total_seconds()),
                "field": _serialize_field(agg["field"]),
            }
            for agg in aggregates
        ]

    # Include custom features if defined (just names, functions not serializable)
    custom = table._custom_features
    if custom:
        spec["custom_features"] = [
            {
                "name": cf["name"],
                "field": _serialize_field(cf["field"]),
                "has_function": True,  # Note that a function exists
            }
            for cf in custom
        ]

    # Note if transforms exist
    if table._transforms:
        spec["has_transforms"] = True

    return spec


def _serialize_source_table(table: core.SourceTable) -> dict[str, Any]:
    """Serialize SourceTable to spec."""
    source_ref = _get_source_reference(table.source)

    spec: dict[str, Any] = {
        "name": table.name,
        "description": table.description,
        "source": source_ref,
        "entity": table.entity.name,
        "timestamp_field": table.timestamp_field,
        "owner": table.owner,
        "tags": table.tags,
    }

    # Include schema fields if defined
    if table.schema_:
        spec["schema"] = {
            name: _serialize_field(field)
            for name, field in table.schema_.fields()
        }

    return spec


def _serialize_dataset(dataset: core.Dataset) -> dict[str, Any]:
    """Serialize Dataset to spec."""
    return {
        "name": dataset.name,
        "description": dataset.description,
        "features": [
            {
                "table": f.table_name,
                "feature": f.name,
                "alias": f._alias,
            }
            for f in dataset.features
        ],
        "prefix_features": dataset.prefix_features,
        "owner": dataset.owner,
        "tags": dataset.tags,
    }


def _serialize_field(field: core.Field) -> dict[str, Any]:
    """Serialize Field to spec dict."""
    return {
        "dtype": field.dtype,
        "description": field.description,
        "gt": field.gt,
        "ge": field.ge,
        "lt": field.lt,
        "le": field.le,
        "not_null": field.not_null,
        "max_null_pct": field.max_null_pct,
        "allowed_values": field.allowed_values,
        "pattern": field.pattern,
        "min_length": field.min_length,
        "max_length": field.max_length,
        "unique": field.unique,
        "max_zscore": field.max_zscore,
        "tags": field.tags,
    }


def _get_source_reference(source: Any) -> dict[str, str]:
    """Get a reference to a source object."""
    import strata.sources as sources

    if isinstance(source, core.FeatureTable):
        return {"type": "feature_table", "name": source.name}
    elif isinstance(source, core.SourceTable):
        return {"type": "source_table", "name": source.name}
    elif isinstance(source, sources.BatchSource):
        return {"type": "batch_source", "name": source.name}
    elif isinstance(source, sources.StreamSource):
        return {"type": "stream_source", "name": source.name}
    elif isinstance(source, sources.RealTimeSource):
        return {"type": "realtime_source", "name": source.name}
    else:
        return {"type": "unknown", "name": str(source)}
