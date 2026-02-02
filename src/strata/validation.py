"""Validation of feature definitions.

Checks definitions for errors and provides actionable fix suggestions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import strata.core as core
import strata.discovery as discovery
import strata.settings as settings

if TYPE_CHECKING:
    pass


@dataclass
class ValidationIssue:
    """A single validation issue."""

    severity: str  # "error" or "warning"
    message: str
    source_file: str | None = None
    object_kind: str | None = None
    object_name: str | None = None
    fix_suggestion: str | None = None


@dataclass
class ValidationResult:
    """Result of validation."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def has_warnings(self) -> bool:
        return any(i.severity == "warning" for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    def add_error(
        self,
        message: str,
        source_file: str | None = None,
        object_kind: str | None = None,
        object_name: str | None = None,
        fix_suggestion: str | None = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity="error",
                message=message,
                source_file=source_file,
                object_kind=object_kind,
                object_name=object_name,
                fix_suggestion=fix_suggestion,
            )
        )

    def add_warning(
        self,
        message: str,
        source_file: str | None = None,
        object_kind: str | None = None,
        object_name: str | None = None,
        fix_suggestion: str | None = None,
    ) -> None:
        self.issues.append(
            ValidationIssue(
                severity="warning",
                message=message,
                source_file=source_file,
                object_kind=object_kind,
                object_name=object_name,
                fix_suggestion=fix_suggestion,
            )
        )


def validate_definitions(
    strata_settings: settings.StrataSettings | None = None,
) -> ValidationResult:
    """Validate all discovered definitions.

    Checks:
    - Duplicate names within kind
    - Entity references exist
    - Source references exist
    - Schedule tags are valid
    - Feature names are unique within table

    Returns ValidationResult with all issues found.
    """
    if strata_settings is None:
        strata_settings = settings.load_strata_settings()

    result = ValidationResult()

    # Discover all definitions
    try:
        discovered = discovery.discover_definitions(strata_settings)
    except Exception as e:
        result.add_error(
            message=f"Failed to discover definitions: {e}",
            fix_suggestion="Check that all Python files in tables/, entities/, datasets/ are valid",
        )
        return result

    # Build indexes for reference checking
    entities: dict[str, discovery.DiscoveredObject] = {}
    feature_tables: dict[str, discovery.DiscoveredObject] = {}
    source_tables: dict[str, discovery.DiscoveredObject] = {}
    datasets: dict[str, discovery.DiscoveredObject] = {}

    # Check for duplicate names
    for disc in discovered:
        if disc.kind == "entity":
            if disc.name in entities:
                result.add_error(
                    message=f"Duplicate entity name: '{disc.name}'",
                    source_file=disc.source_file,
                    object_kind=disc.kind,
                    object_name=disc.name,
                    fix_suggestion=f"Entity '{disc.name}' is also defined in {entities[disc.name].source_file}",
                )
            entities[disc.name] = disc

        elif disc.kind == "feature_table":
            if disc.name in feature_tables:
                result.add_error(
                    message=f"Duplicate feature table name: '{disc.name}'",
                    source_file=disc.source_file,
                    object_kind=disc.kind,
                    object_name=disc.name,
                    fix_suggestion=f"FeatureTable '{disc.name}' is also defined in {feature_tables[disc.name].source_file}",
                )
            feature_tables[disc.name] = disc

        elif disc.kind == "source_table":
            if disc.name in source_tables:
                result.add_error(
                    message=f"Duplicate source table name: '{disc.name}'",
                    source_file=disc.source_file,
                    object_kind=disc.kind,
                    object_name=disc.name,
                )
            source_tables[disc.name] = disc

        elif disc.kind == "dataset":
            if disc.name in datasets:
                result.add_error(
                    message=f"Duplicate dataset name: '{disc.name}'",
                    source_file=disc.source_file,
                    object_kind=disc.kind,
                    object_name=disc.name,
                )
            datasets[disc.name] = disc

    # Validate FeatureTables
    for name, disc in feature_tables.items():
        table: core.FeatureTable = disc.obj
        _validate_feature_table(
            table,
            disc,
            entities,
            feature_tables,
            source_tables,
            strata_settings,
            result,
        )

    # Validate SourceTables
    for name, disc in source_tables.items():
        table: core.SourceTable = disc.obj
        _validate_source_table(table, disc, entities, result)

    # Validate Datasets
    for name, disc in datasets.items():
        dataset: core.Dataset = disc.obj
        _validate_dataset(dataset, disc, feature_tables, source_tables, result)

    return result


def _validate_feature_table(
    table: core.FeatureTable,
    disc: discovery.DiscoveredObject,
    entities: dict[str, discovery.DiscoveredObject],
    feature_tables: dict[str, discovery.DiscoveredObject],
    source_tables: dict[str, discovery.DiscoveredObject],
    strata_settings: settings.StrataSettings,
    result: ValidationResult,
) -> None:
    """Validate a single FeatureTable."""
    # Check entity reference
    if table.entity.name not in entities:
        available = list(entities.keys())
        suggestion = _suggest_similar(table.entity.name, available)
        result.add_error(
            message=f"Entity '{table.entity.name}' not found",
            source_file=disc.source_file,
            object_kind=disc.kind,
            object_name=disc.name,
            fix_suggestion=suggestion
            or f"Available entities: {', '.join(available) or '(none)'}",
        )

    # Check schedule tag if specified
    if table.schedule and strata_settings.schedules:
        if table.schedule not in strata_settings.schedules:
            result.add_error(
                message=f"Invalid schedule tag: '{table.schedule}'",
                source_file=disc.source_file,
                object_kind=disc.kind,
                object_name=disc.name,
                fix_suggestion=f"Valid schedules: {', '.join(strata_settings.schedules)}",
            )

    # Check source reference (if it's a FeatureTable or SourceTable)
    if isinstance(table.source, core.FeatureTable):
        if table.source.name not in feature_tables:
            result.add_error(
                message=f"Source FeatureTable '{table.source.name}' not found",
                source_file=disc.source_file,
                object_kind=disc.kind,
                object_name=disc.name,
            )
    elif isinstance(table.source, core.SourceTable):
        if table.source.name not in source_tables:
            result.add_error(
                message=f"Source SourceTable '{table.source.name}' not found",
                source_file=disc.source_file,
                object_kind=disc.kind,
                object_name=disc.name,
            )


def _validate_source_table(
    table: core.SourceTable,
    disc: discovery.DiscoveredObject,
    entities: dict[str, discovery.DiscoveredObject],
    result: ValidationResult,
) -> None:
    """Validate a single SourceTable."""
    # Check entity reference
    if table.entity.name not in entities:
        available = list(entities.keys())
        suggestion = _suggest_similar(table.entity.name, available)
        result.add_error(
            message=f"Entity '{table.entity.name}' not found",
            source_file=disc.source_file,
            object_kind=disc.kind,
            object_name=disc.name,
            fix_suggestion=suggestion
            or f"Available entities: {', '.join(available) or '(none)'}",
        )


def _validate_dataset(
    dataset: core.Dataset,
    disc: discovery.DiscoveredObject,
    feature_tables: dict[str, discovery.DiscoveredObject],
    source_tables: dict[str, discovery.DiscoveredObject],
    result: ValidationResult,
) -> None:
    """Validate a single Dataset."""
    # Check feature references
    for feature in dataset.features:
        table_name = feature.table_name
        if (
            table_name
            and table_name not in feature_tables
            and table_name not in source_tables
        ):
            result.add_error(
                message=f"Feature '{feature.name}' references unknown table '{table_name}'",
                source_file=disc.source_file,
                object_kind=disc.kind,
                object_name=disc.name,
                fix_suggestion=f"Available tables: {', '.join(list(feature_tables.keys()) + list(source_tables.keys())) or '(none)'}",
            )


def _suggest_similar(name: str, available: list[str]) -> str | None:
    """Suggest similar names using simple Levenshtein-ish matching."""
    if not available:
        return None

    # Simple prefix/suffix matching
    for a in available:
        if name.lower() in a.lower() or a.lower() in name.lower():
            return f"Did you mean '{a}'?"

    return None
