from __future__ import annotations

from datetime import timedelta
from typing import Callable, Literal

import pydantic as pdt

import strata.errors as errors
import strata.sources as sources

DType = Literal[
    "int64",
    "int32",
    "float64",
    "float32",
    "string",
    "bool",
    "datetime",
    "date",
]

AggFunction = Literal["sum", "count", "avg", "min", "max", "count_distinct"]


class StrataBaseModel(pdt.BaseModel):
    model_config = pdt.ConfigDict(
        arbitrary_types_allowed=True,
    )


class FeatureTable(StrataBaseModel):
    """Table where features are defined and computed.

    FeatureTable supports:
    - Source from BatchSource/StreamSource/RealTimeSource
    - Source from another FeatureTable (DAG dependencies)
    - Optional schedule tag for materialization scheduling
    - Feature definitions via aggregate() and @feature decorator

    Example:
        user_transactions = FeatureTable(
            name="user_transactions",
            source=transactions,  # BatchSource
            entity=user,
            timestamp_field="event_timestamp",
            schedule="hourly",
        )

        # Derived table (DAG)
        user_risk = FeatureTable(
            name="user_risk",
            source=user_transactions,  # FeatureTable dependency
            entity=user,
            timestamp_field="event_timestamp",
        )
    """

    name: str
    description: str | None = None
    source: sources.SourceKind | SourceTable | "FeatureTable"
    entity: Entity
    timestamp_field: str | None = None
    schedule: str | None = None  # Optional tag, validated at preview/up
    owner: str | None = None
    tags: dict[str, str] | None = None

    # Write semantics
    write_mode: Literal["append", "merge"] = "append"
    merge_keys: list[str] | None = None  # None = use entity.join_keys
    lookback: timedelta | None = None  # Late-arriving data window

    # Online serving
    online: bool = (
        False  # Declares intent to sync to online store via `strata publish`
    )

    # Quality
    sla: "checks.SLA | None" = None
    sample_pct: float | None = None  # Percentage of rows to validate (1-100)

    # Internal storage for features
    _features: dict[str, Feature] = pdt.PrivateAttr(default_factory=dict)
    _transforms: list[Callable] = pdt.PrivateAttr(default_factory=list)
    _aggregates: list[dict] = pdt.PrivateAttr(default_factory=list)
    _custom_features: list[dict] = pdt.PrivateAttr(default_factory=list)

    @pdt.model_validator(mode="after")
    def validate_sample_pct(self) -> "FeatureTable":
        """Ensure sample_pct is between 1 and 100 if provided."""
        if self.sample_pct is not None and not (1 <= self.sample_pct <= 100):
            raise errors.StrataError(
                context=f"Validating FeatureTable '{self.name}'",
                cause=f"sample_pct must be between 1 and 100, got {self.sample_pct}",
                fix="Set sample_pct to a value between 1 and 100 (e.g., sample_pct=10).",
            )
        return self

    def model_post_init(self, __context) -> None:
        """Initialize private attributes for Pydantic v2 compatibility."""
        # PrivateAttr default_factory doesn't run until after __init__
        # but __getattr__ intercepts _features access, so we must init explicitly
        object.__setattr__(self, "_features", {})
        object.__setattr__(self, "_transforms", [])
        object.__setattr__(self, "_aggregates", [])
        object.__setattr__(self, "_custom_features", [])

    def __getattr__(self, name: str) -> Feature:
        """Allow attribute-style access to features: table.feature_name"""
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' has no attribute '{name}'"
            )
        # Use object.__getattribute__ to avoid recursion when accessing private attributes
        features = object.__getattribute__(self, "_features")
        if name in features:
            return features[name]
        available = list(features.keys())
        hint = (
            f"Available features: {available}"
            if available
            else "Define features using aggregate() or @feature decorator."
        )
        raise AttributeError(
            f"FeatureTable '{self.name}' has no feature '{name}'. {hint}"
        )

    @property
    def is_derived(self) -> bool:
        """True if this table depends on another FeatureTable."""
        return isinstance(self.source, FeatureTable)

    @property
    def source_name(self) -> str:
        """Name of the source table or source."""
        return self.source.name

    @property
    def effective_merge_keys(self) -> list[str]:
        """Return merge keys, defaulting to entity join_keys."""
        if self.merge_keys is not None:
            return self.merge_keys
        return self.entity.join_keys

    def features_list(self) -> list[Feature]:
        """Return all features defined in this table."""
        return list(object.__getattribute__(self, "_features").values())

    def feature(
        self,
        name: str,
        field: Field,
    ) -> Callable[[Callable], Feature]:
        """Decorator for custom Ibis-based feature logic.

        The decorated function receives ibis.Table and returns ibis.Column.

        Example:
            @user_transactions.feature(
                name="spend_velocity",
                field=Field(dtype="float64"),
            )
            def spend_velocity(t):
                # t is ibis.Table
                return (t.spend_90d - t.spend_90d.lag(7)) / t.spend_90d.lag(7)

        Args:
            name: Feature name
            field: Field definition with dtype and validation

        Returns:
            Decorator that returns a Feature reference
        """

        def decorator(func: Callable) -> Feature:
            # Store custom feature definition
            custom_def = {
                "name": name,
                "field": field,
                "func": func,
            }
            self._custom_features.append(custom_def)

            # Create and store feature
            feature = Feature(
                name=name,
                table_name=self.name,
                field=field,
            )
            self._features[name] = feature
            return feature

        return decorator

    def transform(self) -> Callable[[Callable], Callable]:
        """Decorator for table-level transformations.

        The decorated function receives ibis.Table and returns ibis.Table.
        Transforms are applied before feature computation.

        Example:
            @user_transactions.transform()
            def filter_valid(t):
                # t is ibis.Table
                return t.filter(t.status == "completed").filter(t.amount > 0)

        Returns:
            Decorator that returns the original function
        """

        def decorator(func: Callable) -> Callable:
            self._transforms.append(func)
            return func

        return decorator

    def aggregate(
        self,
        name: str,
        field: Field,
        column: str,
        function: AggFunction,
        window: timedelta,
    ) -> Feature:
        """Define a windowed aggregation feature.

        Example:
            spend_90d = user_transactions.aggregate(
                name="spend_90d",
                field=Field(dtype="float64", ge=0, not_null=True),
                column="amount",
                function="sum",
                window=timedelta(days=90),
            )

        Args:
            name: Feature name
            field: Field definition with dtype and validation
            column: Source column to aggregate
            function: Aggregation function (sum, count, avg, min, max, count_distinct)
            window: Time window for aggregation

        Returns:
            Feature reference that can be used in Dataset
        """
        valid = {"sum", "count", "avg", "min", "max", "count_distinct"}
        if function not in valid:
            raise errors.StrataError(
                context=f"Defining aggregate '{name}' on FeatureTable '{self.name}'",
                cause=f"Unsupported aggregation function '{function}'",
                fix=f"Use one of: {', '.join(sorted(valid))}.",
            )
        # Store aggregation definition for later compilation
        agg_def = {
            "name": name,
            "field": field,
            "column": column,
            "function": function,
            "window": window,
        }
        self._aggregates.append(agg_def)

        # Create and store feature
        feature = Feature(
            name=name,
            table_name=self.name,
            field=field,
        )
        self._features[name] = feature
        return feature


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
    timestamp_field: str | None = None
    owner: str | None = None
    tags: dict[str, str] | None = None
    schema_: type[Schema] | None = pdt.Field(default=None, alias="schema")

    # Internal cache for feature objects
    _features: dict[str, Feature] = pdt.PrivateAttr(default_factory=dict)

    def model_post_init(self, __context) -> None:
        """Build feature objects from schema."""
        # Initialize _features before accessing (PrivateAttr + __getattr__ interaction)
        object.__setattr__(self, "_features", {})
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
            raise AttributeError(
                f"'{type(self).__name__}' has no attribute '{name}'"
            )

        features = object.__getattribute__(self, "_features")
        if name in features:
            return features[name]
        raise AttributeError(
            f"SourceTable '{self.name}' has no feature '{name}'. "
            f"Available features: {list(features.keys())}"
        )

    def features_list(self) -> list[Feature]:
        """Return all features defined in this table."""
        return list(self._features.values())


class Dataset(StrataBaseModel):
    """Collection of features for ML training and inference.

    Dataset groups features from multiple FeatureTables and SourceTables.

    Feature naming (controlled by prefix_features):
    - True (default): table__feature (sklearn convention)
    - False: feature (short names, user manages collisions)
    - .alias() always wins regardless of prefix_features

    Example:
        fraud_detection = Dataset(
            name="fraud_detection",
            features=[
                user_transactions.spend_90d,
                user_transactions.txn_count,
                customer_features.lifetime_value,
            ],
            description="Features for fraud detection model",
        )

        # Get output column names
        fraud_detection.output_columns()
        # ['user_transactions__spend_90d', 'user_transactions__txn_count', ...]
    """

    name: str
    description: str | None = None
    features: list[Feature]
    label: Feature | None = None
    prefix_features: bool = True
    owner: str | None = None
    tags: dict[str, str] | None = None

    def output_columns(self) -> list[str]:
        """Return the column names that will appear in output data.

        Respects prefix_features setting and individual aliases.
        """
        columns = []
        for feature in self.features:
            if feature._alias:
                # Alias always wins
                columns.append(feature._alias)
            elif self.prefix_features:
                # Default: table__feature
                columns.append(feature.output_name)
            else:
                # Short name: just feature name
                columns.append(feature.name)
        return columns

    def tables_referenced(self) -> set[str]:
        """Return unique table names referenced by features."""
        return {f.table_name for f in self.features if f.table_name}

    @pdt.model_validator(mode="after")
    def validate_no_duplicate_columns(self) -> "Dataset":
        """Ensure no duplicate output column names."""
        columns = self.output_columns()
        seen = set()
        for col in columns:
            if col in seen:
                raise ValueError(
                    f"Duplicate output column name: '{col}'. "
                    f"Use .alias() to disambiguate or enable prefix_features."
                )
            seen.add(col)
        return self


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
    dtype: DType
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

    # Severity
    severity: Literal["warn", "error"] = (
        "error"  # Constraint violation behavior
    )

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


# Import checks after Entity definition to avoid circular dependency
import strata.checks as checks  # noqa: E402

# Rebuild models to resolve forward references
FeatureTable.model_rebuild()
SourceTable.model_rebuild()
