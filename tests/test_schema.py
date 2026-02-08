import pydantic as pdt
import pytest

import strata.core as core


class TestSchema:
    def test_schema_subclass_defines_fields(self):
        class UserSchema(core.Schema):
            user_id = core.Field(dtype="string", not_null=True)
            email = core.Field(dtype="string")

        fields = UserSchema.fields()
        assert len(fields) == 2
        names = [name for name, _ in fields]
        assert "user_id" in names
        assert "email" in names

    def test_field_names_returns_list(self):
        class UserSchema(core.Schema):
            user_id = core.Field(dtype="string")
            name = core.Field(dtype="string")

        names = UserSchema.field_names()
        assert "user_id" in names
        assert "name" in names

    def test_field_preserves_validation_params(self):
        class UserSchema(core.Schema):
            age = core.Field(dtype="int64", ge=0, le=150)

        fields = dict(UserSchema.fields())
        assert fields["age"].ge == 0
        assert fields["age"].le == 150


class TestField:
    def test_creates_with_dtype(self):
        field = core.Field(dtype="float64")
        assert field.dtype == "float64"

    def test_accepts_validation_params(self):
        field = core.Field(
            dtype="float64",
            ge=0,
            le=100,
            not_null=True,
        )
        assert field.ge == 0
        assert field.le == 100
        assert field.not_null is True


class TestFieldSeverity:
    def test_field_severity_default_error(self):
        field = core.Field(dtype="float64")
        assert field.severity == "error"

    def test_field_severity_warn(self):
        field = core.Field(dtype="float64", severity="warn")
        assert field.severity == "warn"

    def test_field_severity_error_explicit(self):
        field = core.Field(dtype="float64", severity="error")
        assert field.severity == "error"

    def test_field_severity_invalid(self):
        with pytest.raises(pdt.ValidationError):
            core.Field(dtype="float64", severity="bad")
