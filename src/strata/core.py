from datetime import timedelta
from typing import Callable

import pydantic as pdt

import strata.errors as errors
import strata.sources as sources


class StrataBaseModel(pdt.BaseModel):
    pass


class FeatureTable(StrataBaseModel):
    name: str
    description: str
    source: sources.SourceKind | SourceTable
    entity: Entity
    timestamp_field: str
    schedule: str | None = None
    owner: str | None = None
    tags: dict[str, str] | None = None

    def transform(self) -> Callable:
        def decorator():
            pass

        # WHAT DOES THIS RETURN? A pyarrow table?
        return decorator

    def feature(self) -> Feature:
        def decorator() -> Feature:
            return Feature()

        # WHAT DOES THIS RETURN? the Feature class?
        return decorator

    def aggregate(
        self, field: Field, dtype: str, column: str, function: str, window: timedelta
    ) -> Feature:
        # WHAT DOES THIS RETURN? the Feature class?
        pass

    def changes(self, start: str, end: str):
        # WHAT DOES THIS RETURN? A Pyarrow table?
        pass


class Schema(StrataBaseModel):
    """User provides a for SourceTable class

    class UsersDimSchema(Schema):
        user_id: Field = Field("user_id", unique=True)
        name: Field = Field("name")
    """

    pass


class SourceTable(StrataBaseModel):
    name: str
    description: str
    source: sources.SourceKind
    entity: Entity
    timestamp_field: str
    owner: str | None = None
    tags: dict[str, str] | None = None
    schema_: Schema = pdt.Field(default_factory=Schema, alias="schema")


class Column(StrataBaseModel):
    name: str
    dtype: str


class Dataset(StrataBaseModel):
    name: str
    description: str
    features: list[Feature]

    def get_offline_features():
        ## Does this return an arrow table that can be converted
        # to any format e.g. spark pandas or polars?
        pass

    def get_online_vector():
        ## I think this is return as an arrow table and then
        # output as json?
        pass


class Feature(StrataBaseModel):
    name: str


class Field(StrataBaseModel):
    # Identity
    dtype: str  # "int64", "float64", "string", "bool", "datetime"
    description: str | None = None  # Human-readable description

    # Range constraints
    gt: float | None = None  # > value
    ge: float | None = None  # >= value
    lt: float | None = None  # < value
    le: float | None = None  # <= value

    # Null handling
    not_null: bool = False  # No nulls allowed
    max_null_pct: float | None = None  # Max percentage of nulls (0.0-1.0)

    # Categorical
    allowed_values: list | None = None  # Enum-like constraint

    # String
    pattern: str | None = None  # Regex pattern
    min_length: int | None = None
    max_length: int | None = None

    # Uniqueness
    unique: bool = False

    # Statistical
    max_zscore: float | None = None  # Outlier detection

    # Metadata
    tags: list[str] | None = None  # ["pii", "financial"]


class Entity(StrataBaseModel):
    name: str
    description: str | None = None
    join_keys: list[str]

    @pdt.model_validator(mode="after")
    def validate_join_keys(self) -> "Entity":
        if not self.join_keys:
            raise errors.StrataError(
                context=f"Validating entity '{self.name}'",
                cause="join_keys cannot be empty",
                fix="Provide at least one join key for the entity.",
            )
        return self
