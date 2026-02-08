"""Validation engine with pluggable constraint checker interface.

Evaluates Field constraints (ge, le, not_null, max_null_pct, allowed_values,
pattern) against data using a pluggable BaseConstraintChecker backend.

V1 default: PyArrowConstraintChecker (in-memory via PyArrow compute).
Future: IbisConstraintChecker (pushdown to Databricks/BigQuery).
"""

from __future__ import annotations

import abc
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import strata.core as core


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConstraintResult:
    """Result of a single constraint check."""

    field_name: str
    constraint: str  # "ge", "le", "not_null", "max_null_pct", "allowed_values", "pattern", "custom"
    passed: bool
    severity: str  # "warn" or "error"
    expected: str  # Human-readable expected value
    actual: str  # Human-readable actual value
    rows_checked: int
    rows_failed: int  # Number of rows violating constraint


@dataclass(frozen=True)
class FieldResult:
    """Aggregated result for a single field."""

    field_name: str
    constraints: list[ConstraintResult]
    passed: bool  # True if all error-severity constraints passed


@dataclass(frozen=True)
class TableValidationResult:
    """Complete validation result for a table."""

    table_name: str
    field_results: list[FieldResult]
    rows_checked: int
    passed: bool  # True if no error-severity constraints failed
    has_warnings: bool  # True if any warn-severity constraints failed


# ---------------------------------------------------------------------------
# BaseConstraintChecker ABC
# ---------------------------------------------------------------------------


class BaseConstraintChecker(abc.ABC):
    """Interface for constraint checking implementations.

    Implementations receive a column of data and check a single constraint.
    The ABC defines 6 built-in constraint types that map to simple aggregate
    operations (min, max, null_count, distinct, regex match).

    V1: PyArrowConstraintChecker -- in-memory via PyArrow compute.
    Future: IbisConstraintChecker -- pushes queries to backend engine.
    """

    @abc.abstractmethod
    def check_ge(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...

    @abc.abstractmethod
    def check_le(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...

    @abc.abstractmethod
    def check_not_null(
        self,
        column: Any,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...

    @abc.abstractmethod
    def check_max_null_pct(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...

    @abc.abstractmethod
    def check_allowed_values(
        self,
        column: Any,
        values: list,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...

    @abc.abstractmethod
    def check_pattern(
        self,
        column: Any,
        pattern: str,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult: ...


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker
# ---------------------------------------------------------------------------


class PyArrowConstraintChecker(BaseConstraintChecker):
    """Constraint checker using PyArrow compute for in-memory validation."""

    def check_ge(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        import pyarrow.compute as pc

        # Drop nulls for numeric comparison
        valid = pc.drop_null(column)
        if len(valid) == 0:
            return ConstraintResult(
                field_name=field_name,
                constraint="ge",
                passed=True,
                severity=severity,
                expected=f">= {threshold}",
                actual="no non-null values",
                rows_checked=rows_checked,
                rows_failed=0,
            )

        min_val = pc.min(valid).as_py()
        failing_mask = pc.less(valid, threshold)
        rows_failed = pc.sum(failing_mask).as_py()

        return ConstraintResult(
            field_name=field_name,
            constraint="ge",
            passed=min_val >= threshold,
            severity=severity,
            expected=f">= {threshold}",
            actual=f"min={min_val}",
            rows_checked=rows_checked,
            rows_failed=rows_failed,
        )

    def check_le(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        import pyarrow.compute as pc

        valid = pc.drop_null(column)
        if len(valid) == 0:
            return ConstraintResult(
                field_name=field_name,
                constraint="le",
                passed=True,
                severity=severity,
                expected=f"<= {threshold}",
                actual="no non-null values",
                rows_checked=rows_checked,
                rows_failed=0,
            )

        max_val = pc.max(valid).as_py()
        failing_mask = pc.greater(valid, threshold)
        rows_failed = pc.sum(failing_mask).as_py()

        return ConstraintResult(
            field_name=field_name,
            constraint="le",
            passed=max_val <= threshold,
            severity=severity,
            expected=f"<= {threshold}",
            actual=f"max={max_val}",
            rows_checked=rows_checked,
            rows_failed=rows_failed,
        )

    def check_not_null(
        self,
        column: Any,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        null_count = column.null_count
        pct = (null_count / rows_checked * 100) if rows_checked > 0 else 0

        return ConstraintResult(
            field_name=field_name,
            constraint="not_null",
            passed=null_count == 0,
            severity=severity,
            expected="no nulls",
            actual=f"{null_count} nulls ({pct:.1f}%)",
            rows_checked=rows_checked,
            rows_failed=null_count,
        )

    def check_max_null_pct(
        self,
        column: Any,
        threshold: float,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        null_count = column.null_count
        actual_pct = (null_count / rows_checked) if rows_checked > 0 else 0.0

        return ConstraintResult(
            field_name=field_name,
            constraint="max_null_pct",
            passed=actual_pct <= threshold,
            severity=severity,
            expected=f"nulls <= {threshold * 100}%",
            actual=f"{actual_pct * 100:.1f}% nulls",
            rows_checked=rows_checked,
            rows_failed=null_count,
        )

    def check_allowed_values(
        self,
        column: Any,
        values: list,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        import pyarrow as pa
        import pyarrow.compute as pc

        valid = pc.drop_null(column)
        if len(valid) == 0:
            return ConstraintResult(
                field_name=field_name,
                constraint="allowed_values",
                passed=True,
                severity=severity,
                expected=f"in {values}",
                actual="no non-null values",
                rows_checked=rows_checked,
                rows_failed=0,
            )

        value_set = pa.array(values, type=valid.type)
        in_mask = pc.is_in(valid, value_set=value_set)
        not_in_mask = pc.invert(in_mask)
        rows_failed = pc.sum(not_in_mask).as_py()

        return ConstraintResult(
            field_name=field_name,
            constraint="allowed_values",
            passed=rows_failed == 0,
            severity=severity,
            expected=f"in {values}",
            actual=f"{rows_failed} invalid values",
            rows_checked=rows_checked,
            rows_failed=rows_failed,
        )

    def check_pattern(
        self,
        column: Any,
        pattern: str,
        field_name: str,
        severity: str,
        rows_checked: int,
    ) -> ConstraintResult:
        import pyarrow.compute as pc

        valid = pc.drop_null(column)
        if len(valid) == 0:
            return ConstraintResult(
                field_name=field_name,
                constraint="pattern",
                passed=True,
                severity=severity,
                expected=f"matches '{pattern}'",
                actual="no non-null values",
                rows_checked=rows_checked,
                rows_failed=0,
            )

        match_mask = pc.match_substring_regex(valid, pattern)
        not_matching = pc.invert(match_mask)
        rows_failed = pc.sum(not_matching).as_py()

        return ConstraintResult(
            field_name=field_name,
            constraint="pattern",
            passed=rows_failed == 0,
            severity=severity,
            expected=f"matches '{pattern}'",
            actual=f"{rows_failed} non-matching",
            rows_checked=rows_checked,
            rows_failed=rows_failed,
        )


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------


def _collect_fields(table: core.FeatureTable) -> list[tuple[str, core.Field]]:
    """Extract named Field definitions from a FeatureTable.

    Fields come from features registered via aggregate() or @feature decorator.
    """
    result: list[tuple[str, core.Field]] = []
    for feature in table.features_list():
        if feature.field is not None:
            result.append((feature.name, feature.field))
    return result


def _has_constraints(field: core.Field) -> bool:
    """Check if a Field has any built-in constraints defined."""
    return any([
        field.ge is not None,
        field.le is not None,
        field.not_null,
        field.max_null_pct is not None,
        field.allowed_values is not None,
        field.pattern is not None,
    ])


# ---------------------------------------------------------------------------
# Main validation function
# ---------------------------------------------------------------------------


def validate_table(
    table: core.FeatureTable,
    data: Any,
    sample_pct: float | None = None,
    checker: BaseConstraintChecker | None = None,
    custom_validators: dict[str, Callable] | None = None,
) -> TableValidationResult:
    """Validate data against a FeatureTable's Field constraints.

    Args:
        table: FeatureTable definition with Field constraints.
        data: PyArrow Table (or compatible) containing the data to validate.
        sample_pct: If provided (1-100), randomly sample that percentage
            of rows before validation. Overrides table.sample_pct.
        checker: Constraint checker implementation. Defaults to
            PyArrowConstraintChecker.
        custom_validators: Dict mapping field names to callables.
            Each callable receives a column (pa.Array) and returns bool.

    Returns:
        TableValidationResult with per-field constraint results.
    """
    if checker is None:
        checker = PyArrowConstraintChecker()

    if custom_validators is None:
        custom_validators = {}

    # Resolve sample_pct: explicit param > table.sample_pct > None
    effective_sample_pct = sample_pct if sample_pct is not None else table.sample_pct

    # Apply sampling if requested
    if effective_sample_pct is not None and 1 <= effective_sample_pct < 100:
        n_rows = len(data)
        sample_size = max(1, int(n_rows * effective_sample_pct / 100))
        # Use random indices for sampling
        import random

        indices = random.sample(range(n_rows), sample_size)
        data = data.take(indices)

    rows_checked = len(data)

    # Collect fields from FeatureTable
    fields = _collect_fields(table)

    field_results: list[FieldResult] = []
    all_error_passed = True
    any_warn_failed = False

    for field_name, field in fields:
        constraints: list[ConstraintResult] = []

        # Check if column exists in data
        if field_name not in data.column_names:
            # Skip fields not present in data
            continue

        column = data.column(field_name)
        severity = field.severity

        # Built-in constraints
        if field.ge is not None:
            cr = checker.check_ge(column, field.ge, field_name, severity, rows_checked)
            constraints.append(cr)

        if field.le is not None:
            cr = checker.check_le(column, field.le, field_name, severity, rows_checked)
            constraints.append(cr)

        if field.not_null:
            cr = checker.check_not_null(column, field_name, severity, rows_checked)
            constraints.append(cr)

        if field.max_null_pct is not None:
            cr = checker.check_max_null_pct(
                column, field.max_null_pct, field_name, severity, rows_checked
            )
            constraints.append(cr)

        if field.allowed_values is not None:
            cr = checker.check_allowed_values(
                column, field.allowed_values, field_name, severity, rows_checked
            )
            constraints.append(cr)

        if field.pattern is not None:
            cr = checker.check_pattern(column, field.pattern, field_name, severity, rows_checked)
            constraints.append(cr)

        # Custom validator for this field
        if field_name in custom_validators:
            validator_fn = custom_validators[field_name]
            try:
                custom_passed = validator_fn(column)
            except Exception:
                custom_passed = False

            constraints.append(
                ConstraintResult(
                    field_name=field_name,
                    constraint="custom",
                    passed=custom_passed,
                    severity="error",  # Custom validators always error severity
                    expected="custom check",
                    actual="passed" if custom_passed else "failed",
                    rows_checked=rows_checked,
                    rows_failed=0 if custom_passed else rows_checked,
                )
            )

        # Determine field-level pass/fail (only error-severity matter)
        error_constraints_passed = all(
            cr.passed for cr in constraints if cr.severity == "error"
        )
        field_passed = error_constraints_passed

        if not field_passed:
            all_error_passed = False

        # Track warnings
        warn_constraints_failed = any(
            not cr.passed for cr in constraints if cr.severity == "warn"
        )
        if warn_constraints_failed:
            any_warn_failed = True

        field_results.append(
            FieldResult(
                field_name=field_name,
                constraints=constraints,
                passed=field_passed,
            )
        )

    # Handle custom validators for fields not in the FeatureTable definition
    field_names_processed = {fr.field_name for fr in field_results}
    for field_name, validator_fn in custom_validators.items():
        if field_name not in field_names_processed and field_name in data.column_names:
            column = data.column(field_name)
            try:
                custom_passed = validator_fn(column)
            except Exception:
                custom_passed = False

            cr = ConstraintResult(
                field_name=field_name,
                constraint="custom",
                passed=custom_passed,
                severity="error",
                expected="custom check",
                actual="passed" if custom_passed else "failed",
                rows_checked=rows_checked,
                rows_failed=0 if custom_passed else rows_checked,
            )

            if not custom_passed:
                all_error_passed = False

            field_results.append(
                FieldResult(
                    field_name=field_name,
                    constraints=[cr],
                    passed=custom_passed,
                )
            )

    return TableValidationResult(
        table_name=table.name,
        field_results=field_results,
        rows_checked=rows_checked,
        passed=all_error_passed,
        has_warnings=any_warn_failed,
    )
