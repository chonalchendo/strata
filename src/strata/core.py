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


class Schema:
    """Schema definition using Field for column specifications.

    Users subclass Schema and define fields as class attributes:

    Example:
        class UserSchema(Schema):
            user_id = Field(dtype="string", not_null=True, unique=True)
            email = Field(dtype="string")
            created_at = Field(dtype="datetime", not_null=True)
    """

    @classmethod
    def fields(cls) -> list[tuple[str, Field]]:
        """Return all Field definitions as (name, field) tuples."""
        result = []
        for name in dir(cls):
            if not name.startswith("_"):
                value = getattr(cls, name)
                if isinstance(value, Field):
                    result.append((name, value))
        return result

    @classmethod
    def field_names(cls) -> list[str]:
        """Return names of all fields in schema."""
        return [name for name, _ in cls.fields()]


class SourceTable(StrataBaseModel):
    """Reference to an external table with pre-computed features.

    SourceTable registers features that already exist (e.g., built by dbt).
    Strata doesn't compute anything - just makes them available for Datasets.

    Example:
        class CustomerSchema(Schema):
            lifetime_value = Field(dtype="float64", ge=0)
            churn_risk = Field(dtype="float64", ge=0, le=1)

        customer_features = SourceTable(
            name="customer_features",
            source=BatchSource(
                name="customer_features",
                config=DuckDBSourceConfig(path="./features.parquet"),
                timestamp_field="updated_at",
            ),
            entity=user,
            timestamp_field="updated_at",
            schema=CustomerSchema,
        )

        # Access features as attributes
        customer_features.lifetime_value  # Returns Feature
    """
    name: str
    description: str | None = None
    source: sources.SourceKind
    entity: Entity
    timestamp_field: str
    owner: str | None = None
    tags: dict[str, str] | None = None
    schema_: type[Schema] | None = pdt.Field(default=None, alias="schema")

    # Internal cache for feature objects
    _features: dict[str, Feature] = pdt.PrivateAttr(default_factory=dict)

    def model_post_init(self, __context) -> None:
        """Build feature objects from schema."""
        if self.schema_ is not None:
            for name, field in self.schema_.fields():
                self._features[name] = Feature(
                    name=name,
                    table_name=self.name,
                    field=field,
                )

    def __getattr__(self, name: str) -> Feature:
        """Allow attribute-style access to features: table.feature_name"""
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")
        if name in self._features:
            return self._features[name]
        raise AttributeError(
            f"SourceTable '{self.name}' has no feature '{name}'. "
            f"Available features: {list(self._features.keys())}"
        )

    def features_list(self) -> list[Feature]:
        """Return all features defined in this table."""
        return list(self._features.values())


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
    """Reference to a feature within a table."""
    name: str
    table_name: str | None = None
    field: Field | None = None
    _alias: str | None = pdt.PrivateAttr(default=None)

    def alias(self, name: str) -> Feature:
        """Create a copy with a custom output column name."""
        new_feature = Feature(
            name=self.name,
            table_name=self.table_name,
            field=self.field,
        )
        new_feature._alias = name
        return new_feature

    @property
    def output_name(self) -> str:
        """The column name in Dataset output."""
        if self._alias:
            return self._alias
        if self.table_name:
            return f"{self.table_name}__{self.name}"
        return self.name

    @property
    def qualified_name(self) -> str:
        """Fully qualified name: table.feature"""
        if self.table_name:
            return f"{self.table_name}.{self.name}"
        return self.name


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
