"""Tests for validation engine in strata.quality."""

from __future__ import annotations

import pyarrow as pa
import pytest

import strata.core as core
import strata.quality as quality
import strata.sources as sources
from strata.infra.backends.local import LocalSourceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture()
def batch_source():
    cfg = LocalSourceConfig(path="/tmp/test.parquet")
    return sources.BatchSource(
        name="test_source", config=cfg, timestamp_field="ts"
    )


def _make_table(
    entity, batch_source, fields: dict, **kwargs
) -> core.FeatureTable:
    """Helper: create a FeatureTable, register fields via aggregate (simplest path)."""
    from datetime import timedelta

    ft = core.FeatureTable(
        name="test_table",
        source=batch_source,
        entity=entity,
        timestamp_field="ts",
        **kwargs,
    )
    for name, field in fields.items():
        ft.aggregate(
            name=name,
            field=field,
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )
    return ft


# ---------------------------------------------------------------------------
# Result dataclass tests
# ---------------------------------------------------------------------------


class TestConstraintResult:
    def test_frozen(self):
        r = quality.ConstraintResult(
            field_name="x",
            constraint="ge",
            passed=True,
            severity="error",
            expected=">= 0",
            actual="min=1",
            rows_checked=10,
            rows_failed=0,
        )
        with pytest.raises(AttributeError):
            r.passed = False  # type: ignore[misc]

    def test_fields(self):
        r = quality.ConstraintResult(
            field_name="x",
            constraint="le",
            passed=False,
            severity="warn",
            expected="<= 100",
            actual="max=120",
            rows_checked=50,
            rows_failed=3,
        )
        assert r.field_name == "x"
        assert r.constraint == "le"
        assert r.passed is False
        assert r.severity == "warn"
        assert r.expected == "<= 100"
        assert r.actual == "max=120"
        assert r.rows_checked == 50
        assert r.rows_failed == 3


class TestFieldResult:
    def test_frozen(self):
        fr = quality.FieldResult(field_name="x", constraints=[], passed=True)
        with pytest.raises(AttributeError):
            fr.passed = False  # type: ignore[misc]


class TestTableValidationResult:
    def test_frozen(self):
        tr = quality.TableValidationResult(
            table_name="t",
            field_results=[],
            rows_checked=0,
            passed=True,
            has_warnings=False,
        )
        with pytest.raises(AttributeError):
            tr.passed = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# BaseConstraintChecker ABC
# ---------------------------------------------------------------------------


class TestBaseConstraintChecker:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            quality.BaseConstraintChecker()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: ge
# ---------------------------------------------------------------------------


class TestCheckGe:
    def test_ge_pass(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"amount": core.Field(dtype="float64", ge=0)}
        )
        data = pa.table({"amount": [1.0, 2.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True
        assert len(result.field_results) == 1
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "ge"
        assert cr.passed is True
        assert cr.rows_failed == 0

    def test_ge_fail(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"amount": core.Field(dtype="float64", ge=0)}
        )
        data = pa.table({"amount": [1.0, -5.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "ge"
        assert cr.passed is False
        assert cr.rows_failed >= 1


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: le
# ---------------------------------------------------------------------------


class TestCheckLe:
    def test_le_pass(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"score": core.Field(dtype="float64", le=100)}
        )
        data = pa.table({"score": [50.0, 99.0, 100.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True

    def test_le_fail(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"score": core.Field(dtype="float64", le=100)}
        )
        data = pa.table({"score": [50.0, 101.0, 100.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "le"
        assert cr.passed is False


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: not_null
# ---------------------------------------------------------------------------


class TestCheckNotNull:
    def test_not_null_pass(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"val": core.Field(dtype="float64", not_null=True)},
        )
        data = pa.table({"val": [1.0, 2.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True

    def test_not_null_fail(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"val": core.Field(dtype="float64", not_null=True)},
        )
        data = pa.table({"val": pa.array([1.0, None, 3.0], type=pa.float64())})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "not_null"
        assert cr.passed is False
        assert cr.rows_failed == 1


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: max_null_pct
# ---------------------------------------------------------------------------


class TestCheckMaxNullPct:
    def test_max_null_pct_pass(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"val": core.Field(dtype="float64", max_null_pct=0.05)},
        )
        # 0/100 = 0% nulls
        data = pa.table({"val": [float(i) for i in range(100)]})
        result = quality.validate_table(ft, data)
        assert result.passed is True

    def test_max_null_pct_fail(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"val": core.Field(dtype="float64", max_null_pct=0.05)},
        )
        # 10/100 = 10% nulls > 5%
        values = [float(i) if i >= 10 else None for i in range(100)]
        data = pa.table({"val": pa.array(values, type=pa.float64())})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "max_null_pct"
        assert cr.passed is False


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: allowed_values
# ---------------------------------------------------------------------------


class TestCheckAllowedValues:
    def test_allowed_values_pass(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {
                "status": core.Field(
                    dtype="string", allowed_values=["active", "inactive"]
                )
            },
        )
        data = pa.table({"status": ["active", "inactive", "active"]})
        result = quality.validate_table(ft, data)
        assert result.passed is True

    def test_allowed_values_fail(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {
                "status": core.Field(
                    dtype="string", allowed_values=["active", "inactive"]
                )
            },
        )
        data = pa.table({"status": ["active", "deleted", "active"]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "allowed_values"
        assert cr.passed is False
        assert cr.rows_failed >= 1


# ---------------------------------------------------------------------------
# PyArrowConstraintChecker: pattern
# ---------------------------------------------------------------------------


class TestCheckPattern:
    def test_pattern_pass(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {
                "email": core.Field(
                    dtype="string", pattern=r"^[^@]+@[^@]+\.[^@]+$"
                )
            },
        )
        data = pa.table({"email": ["a@b.com", "x@y.org"]})
        result = quality.validate_table(ft, data)
        assert result.passed is True

    def test_pattern_fail(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {
                "email": core.Field(
                    dtype="string", pattern=r"^[^@]+@[^@]+\.[^@]+$"
                )
            },
        )
        data = pa.table({"email": ["a@b.com", "notanemail"]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        cr = result.field_results[0].constraints[0]
        assert cr.constraint == "pattern"
        assert cr.passed is False
        assert cr.rows_failed >= 1


# ---------------------------------------------------------------------------
# Severity propagation: warn vs error
# ---------------------------------------------------------------------------


class TestSeverityPropagation:
    def test_warn_severity_does_not_fail_table(self, entity, batch_source):
        """warn-severity constraint failure: table passes, has_warnings=True."""
        ft = _make_table(
            entity,
            batch_source,
            {"amount": core.Field(dtype="float64", ge=0, severity="warn")},
        )
        data = pa.table({"amount": [-1.0, 2.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True
        assert result.has_warnings is True

    def test_error_severity_fails_table(self, entity, batch_source):
        """error-severity constraint failure: table fails."""
        ft = _make_table(
            entity,
            batch_source,
            {"amount": core.Field(dtype="float64", ge=0, severity="error")},
        )
        data = pa.table({"amount": [-1.0, 2.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
        assert result.has_warnings is False


# ---------------------------------------------------------------------------
# sample_pct
# ---------------------------------------------------------------------------


class TestSamplePct:
    def test_sample_pct_reduces_rows(self, entity, batch_source):
        """sample_pct=50 should validate roughly half the rows."""
        ft = _make_table(
            entity,
            batch_source,
            {"val": core.Field(dtype="float64", ge=0)},
            sample_pct=50,
        )
        data = pa.table({"val": [float(i) for i in range(1000)]})
        result = quality.validate_table(ft, data)
        # Should validate fewer than all 1000 rows
        assert result.rows_checked < 1000
        assert result.rows_checked > 0
        assert result.passed is True


# ---------------------------------------------------------------------------
# Custom validators
# ---------------------------------------------------------------------------


class TestCustomValidators:
    def test_custom_validator_pass(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"val": core.Field(dtype="float64")}
        )
        data = pa.table({"val": [1.0, 2.0, 3.0]})

        def all_positive(column: pa.Array) -> bool:
            import pyarrow.compute as pc

            return pc.all(pc.greater(column, 0)).as_py()

        result = quality.validate_table(
            ft, data, custom_validators={"val": all_positive}
        )
        assert result.passed is True

    def test_custom_validator_fail(self, entity, batch_source):
        ft = _make_table(
            entity, batch_source, {"val": core.Field(dtype="float64")}
        )
        data = pa.table({"val": [1.0, -2.0, 3.0]})

        def all_positive(column: pa.Array) -> bool:
            import pyarrow.compute as pc

            return pc.all(pc.greater(column, 0)).as_py()

        result = quality.validate_table(
            ft, data, custom_validators={"val": all_positive}
        )
        assert result.passed is False
        # Find the custom constraint result
        custom_crs = [
            cr
            for fr in result.field_results
            for cr in fr.constraints
            if cr.constraint == "custom"
        ]
        assert len(custom_crs) == 1
        assert custom_crs[0].passed is False


# ---------------------------------------------------------------------------
# No constraints
# ---------------------------------------------------------------------------


class TestNoConstraints:
    def test_table_with_no_constrained_fields(self, entity, batch_source):
        """A table with no constraints should pass validation."""
        ft = _make_table(
            entity, batch_source, {"val": core.Field(dtype="float64")}
        )
        data = pa.table({"val": [1.0, 2.0, 3.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True
        assert result.has_warnings is False


# ---------------------------------------------------------------------------
# Explicit checker parameter
# ---------------------------------------------------------------------------


class TestExplicitChecker:
    def test_explicit_pyarrow_checker(self, entity, batch_source):
        """Passing an explicit PyArrowConstraintChecker works."""
        ft = _make_table(
            entity, batch_source, {"amount": core.Field(dtype="float64", ge=0)}
        )
        data = pa.table({"amount": [1.0, 2.0, 3.0]})
        checker = quality.PyArrowConstraintChecker()
        result = quality.validate_table(ft, data, checker=checker)
        assert result.passed is True


# ---------------------------------------------------------------------------
# Multiple constraints on a single field
# ---------------------------------------------------------------------------


class TestMultipleConstraints:
    def test_ge_and_le_both_checked(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"score": core.Field(dtype="float64", ge=0, le=100)},
        )
        data = pa.table({"score": [50.0, 60.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is True
        # Both ge and le constraints checked
        constraints = result.field_results[0].constraints
        constraint_names = [c.constraint for c in constraints]
        assert "ge" in constraint_names
        assert "le" in constraint_names

    def test_ge_and_le_one_fails(self, entity, batch_source):
        ft = _make_table(
            entity,
            batch_source,
            {"score": core.Field(dtype="float64", ge=0, le=100)},
        )
        data = pa.table({"score": [50.0, 150.0]})
        result = quality.validate_table(ft, data)
        assert result.passed is False
