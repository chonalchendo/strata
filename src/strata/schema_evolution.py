"""Schema evolution detection and migration action determination.

Detects changes between old and new PyArrow schemas and determines
the appropriate migration action based on the change type:

- Column added: Full backfill (new column needs history)
- Column removed: Continue incremental (column just absent)
- Type widened (int32->int64): Schema evolve, continue incremental
- Type narrowed (float64->int32): Full backfill (potential data loss)
- Column renamed: Full backfill (can't infer mapping)
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

import pyarrow as pa


class SchemaChangeType(enum.Enum):
    """Types of schema changes that can be detected."""

    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    TYPE_WIDENED = "type_widened"
    TYPE_NARROWED = "type_narrowed"
    NO_CHANGE = "no_change"


class MigrationAction(enum.Enum):
    """Migration action to take when schema changes are detected."""

    NONE = "none"
    CONTINUE_INCREMENTAL = "continue_incremental"
    FULL_BACKFILL = "full_backfill"


@dataclass(frozen=True)
class SchemaChange:
    """A single detected schema change."""

    change_type: SchemaChangeType
    column_name: str
    old_type: pa.DataType | None = None
    new_type: pa.DataType | None = None
    migration_action: MigrationAction = MigrationAction.NONE


@dataclass(frozen=True)
class SchemaEvolutionResult:
    """Result of comparing two schemas for evolution."""

    changes: list[SchemaChange]
    requires_backfill: bool
    migration_action: MigrationAction


# Integer types ordered by width (narrower to wider)
_INT_WIDENING_ORDER: list[type[pa.DataType]] = [
    pa.int8,
    pa.int16,
    pa.int32,
    pa.int64,
]

# Unsigned integer types ordered by width
_UINT_WIDENING_ORDER: list[type[pa.DataType]] = [
    pa.uint8,
    pa.uint16,
    pa.uint32,
    pa.uint64,
]

# Float types ordered by width
_FLOAT_WIDENING_ORDER: list[type[pa.DataType]] = [
    pa.float16,
    pa.float32,
    pa.float64,
]


def _type_index(
    dtype: pa.DataType,
    order: list[type[pa.DataType]],
) -> int | None:
    """Return the position of a type in the widening order, or None."""
    for i, factory in enumerate(order):
        if dtype == factory():
            return i
    return None


def _classify_type_change(
    old_type: pa.DataType,
    new_type: pa.DataType,
) -> SchemaChangeType:
    """Classify a type change as widening, narrowing, or no change.

    Type widening means the new type can represent all values of the old
    type without loss (e.g., int32 -> int64). Type narrowing means
    potential data loss (e.g., int64 -> int32, float64 -> int32).

    Cross-family changes (int -> string, float -> int) are treated as
    narrowing (full backfill) since they may involve data loss.
    """
    if old_type == new_type:
        return SchemaChangeType.NO_CHANGE

    # Check within same type families
    for order in (_INT_WIDENING_ORDER, _UINT_WIDENING_ORDER, _FLOAT_WIDENING_ORDER):
        old_idx = _type_index(old_type, order)
        new_idx = _type_index(new_type, order)

        if old_idx is not None and new_idx is not None:
            if new_idx > old_idx:
                return SchemaChangeType.TYPE_WIDENED
            return SchemaChangeType.TYPE_NARROWED

    # int -> float is widening (int32/int64 fit in float64 for practical purposes)
    old_int_idx = _type_index(old_type, _INT_WIDENING_ORDER)
    new_float_idx = _type_index(new_type, _FLOAT_WIDENING_ORDER)
    if old_int_idx is not None and new_float_idx is not None:
        return SchemaChangeType.TYPE_WIDENED

    # float -> int is narrowing (potential precision loss)
    old_float_idx = _type_index(old_type, _FLOAT_WIDENING_ORDER)
    new_int_idx = _type_index(new_type, _INT_WIDENING_ORDER)
    if old_float_idx is not None and new_int_idx is not None:
        return SchemaChangeType.TYPE_NARROWED

    # Cross-family changes are treated as narrowing (safe default)
    return SchemaChangeType.TYPE_NARROWED


def _action_for_change(change_type: SchemaChangeType) -> MigrationAction:
    """Determine migration action for a given change type."""
    if change_type == SchemaChangeType.COLUMN_ADDED:
        return MigrationAction.FULL_BACKFILL
    if change_type == SchemaChangeType.COLUMN_REMOVED:
        return MigrationAction.CONTINUE_INCREMENTAL
    if change_type == SchemaChangeType.TYPE_WIDENED:
        return MigrationAction.CONTINUE_INCREMENTAL
    if change_type == SchemaChangeType.TYPE_NARROWED:
        return MigrationAction.FULL_BACKFILL
    return MigrationAction.NONE


def detect_schema_changes(
    old_schema: pa.Schema | None,
    new_schema: pa.Schema,
) -> SchemaEvolutionResult:
    """Detect changes between old and new PyArrow schemas.

    Args:
        old_schema: Previous schema (None for new tables).
        new_schema: Current schema from the feature table definition.

    Returns:
        SchemaEvolutionResult with all detected changes and the
        overall migration action (the most aggressive action needed).
    """
    # New table -- no evolution needed
    if old_schema is None:
        return SchemaEvolutionResult(
            changes=[],
            requires_backfill=False,
            migration_action=MigrationAction.NONE,
        )

    old_fields = {field.name: field.type for field in old_schema}
    new_fields = {field.name: field.type for field in new_schema}

    changes: list[SchemaChange] = []

    # Detect removed columns
    for name in old_fields:
        if name not in new_fields:
            change = SchemaChange(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                column_name=name,
                old_type=old_fields[name],
                new_type=None,
                migration_action=MigrationAction.CONTINUE_INCREMENTAL,
            )
            changes.append(change)

    # Detect added columns
    for name in new_fields:
        if name not in old_fields:
            change = SchemaChange(
                change_type=SchemaChangeType.COLUMN_ADDED,
                column_name=name,
                old_type=None,
                new_type=new_fields[name],
                migration_action=MigrationAction.FULL_BACKFILL,
            )
            changes.append(change)

    # Detect type changes for columns that exist in both
    for name in old_fields:
        if name in new_fields:
            old_type = old_fields[name]
            new_type = new_fields[name]
            change_type = _classify_type_change(old_type, new_type)

            if change_type != SchemaChangeType.NO_CHANGE:
                action = _action_for_change(change_type)
                change = SchemaChange(
                    change_type=change_type,
                    column_name=name,
                    old_type=old_type,
                    new_type=new_type,
                    migration_action=action,
                )
                changes.append(change)

    # Overall action is the most aggressive across all changes
    requires_backfill = any(
        c.migration_action == MigrationAction.FULL_BACKFILL for c in changes
    )

    if requires_backfill:
        overall_action = MigrationAction.FULL_BACKFILL
    elif any(
        c.migration_action == MigrationAction.CONTINUE_INCREMENTAL for c in changes
    ):
        overall_action = MigrationAction.CONTINUE_INCREMENTAL
    else:
        overall_action = MigrationAction.NONE

    return SchemaEvolutionResult(
        changes=changes,
        requires_backfill=requires_backfill,
        migration_action=overall_action,
    )
