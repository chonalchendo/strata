"""Project handle for Strata feature store connections.

Provides strata.connect() entry point and BoundDataset for offline feature retrieval
with point-in-time joins and online feature lookup. The StrataProject holds live
backend/registry/online_store connections; BoundDataset uses them to execute
read_features() (offline) and lookup_features() (online).

Usage:
    import strata

    project = strata.connect()
    ds = project.get_dataset("fraud_detection")

    # Offline: PIT-correct training data
    features = ds.read_features(start="2024-01-01", end="2024-04-01")

    # Online: real-time feature lookup
    vector = ds.lookup_features({"user_id": "123"})
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa

import strata.errors as errors
import strata.settings as settings

if TYPE_CHECKING:
    import strata.backends as backends
    import strata.core as core
    import strata.serving as serving


class StrataProject:
    """Runtime handle for a Strata project.

    Created via strata.connect(). Holds live backend, registry, and
    optional online store connections resolved from strata.yaml.

    Not a Pydantic model -- this is a runtime handle, not configuration.
    """

    def __init__(self, strata_settings: settings.StrataSettings) -> None:
        self._settings = strata_settings
        env_config = strata_settings.active_environment
        self._backend: backends.BackendKind = env_config.backend
        self._registry: backends.RegistryKind = env_config.registry
        self._online_store: serving.OnlineStoreKind | None = env_config.online_store

    @property
    def name(self) -> str:
        """Project name from strata.yaml."""
        return self._settings.name

    @property
    def env(self) -> str:
        """Active environment name."""
        return self._settings.active_env

    def get_dataset(self, name: str) -> BoundDataset:
        """Retrieve a Dataset definition by name and return a bound handle.

        Discovers all definitions in the project, finds the Dataset with
        the given name, and returns a BoundDataset with resolved feature
        table references.

        Args:
            name: Name of the Dataset to retrieve.

        Returns:
            BoundDataset bound to this project's backend connections.

        Raises:
            StrataError: If the dataset is not found.
        """
        import strata.discovery as discovery

        discovered = discovery.discover_definitions(self._settings)

        # Collect all objects by type
        feature_tables: dict[str, core.FeatureTable] = {}
        source_tables: dict[str, core.SourceTable] = {}
        dataset_obj: core.Dataset | None = None

        for obj in discovered:
            if obj.kind == "feature_table":
                feature_tables[obj.name] = obj.obj
            elif obj.kind == "source_table":
                source_tables[obj.name] = obj.obj
            elif obj.kind == "dataset" and obj.name == name:
                dataset_obj = obj.obj

        if dataset_obj is None:
            available = [
                o.name for o in discovered if o.kind == "dataset"
            ]
            available_str = ", ".join(available) if available else "(none)"
            raise errors.StrataError(
                context=f"Retrieving dataset '{name}'",
                cause=f"Dataset '{name}' not found in project definitions",
                fix=f"Available datasets: {available_str}. Check the dataset name and ensure the definition file is in the configured paths.",
            )

        return BoundDataset(
            dataset=dataset_obj,
            project=self,
            feature_tables=feature_tables,
            source_tables=source_tables,
        )

    def write_table(
        self,
        name: str,
        data: pa.Table,
        *,
        mode: str = "append",
    ) -> None:
        """Write data to a named table via the backend.

        Clean public API that avoids exposing _backend internals.

        Args:
            name: Logical table name.
            data: PyArrow Table to write.
            mode: Write mode -- "append" or "merge".
        """
        self._backend.write_table(table_name=name, data=data, mode=mode)

    def get_feature_table(self, name: str) -> BoundFeatureTable:
        """Retrieve a FeatureTable definition by name and return a bound handle.

        Args:
            name: Name of the FeatureTable to retrieve.

        Returns:
            BoundFeatureTable bound to this project's backend connections.

        Raises:
            StrataError: If the feature table is not found.
        """
        import strata.discovery as discovery

        discovered = discovery.discover_definitions(self._settings)

        for obj in discovered:
            if obj.kind == "feature_table" and obj.name == name:
                return BoundFeatureTable(
                    feature_table=obj.obj,
                    project=self,
                )

        available = [
            o.name for o in discovered if o.kind == "feature_table"
        ]
        available_str = ", ".join(available) if available else "(none)"
        raise errors.StrataError(
            context=f"Retrieving feature table '{name}'",
            cause=f"FeatureTable '{name}' not found in project definitions",
            fix=f"Available feature tables: {available_str}. Check the name and ensure the definition file is in the configured paths.",
        )


class BoundFeatureTable:
    """A FeatureTable bound to a project's backend for reads/writes.

    Simple handle that delegates operations to the project's backend.
    """

    def __init__(
        self,
        feature_table: core.FeatureTable,
        project: StrataProject,
    ) -> None:
        self._feature_table = feature_table
        self._project = project

    @property
    def name(self) -> str:
        """Feature table name."""
        return self._feature_table.name

    def write(self, data: pa.Table, *, mode: str = "append") -> None:
        """Write data to this feature table.

        Args:
            data: PyArrow Table to write.
            mode: Write mode -- "append" or "merge".
        """
        self._project.write_table(self._feature_table.name, data, mode=mode)

    def read(self) -> pa.Table:
        """Read all data from this feature table.

        Returns:
            PyArrow Table with the table's data.
        """
        return self._project._backend.read_table(self._feature_table.name)


class BoundDataset:
    """A Dataset bound to a project's backend for feature retrieval.

    Created by StrataProject.get_dataset(). Provides read_features() for
    offline PIT-correct training data and lookup_features() for online serving.
    """

    def __init__(
        self,
        dataset: core.Dataset,
        project: StrataProject,
        feature_tables: dict[str, core.FeatureTable],
        source_tables: dict[str, core.SourceTable],
    ) -> None:
        self._dataset = dataset
        self._project = project
        self._feature_tables = feature_tables
        self._source_tables = source_tables

    @property
    def name(self) -> str:
        """Dataset name."""
        return self._dataset.name

    def read_features(
        self,
        start: datetime | str,
        end: datetime | str,
        *,
        spine: pa.Table | None = None,
    ) -> pa.Table:
        """Read features with point-in-time correctness for training.

        Uses implicit spine by default: reads the primary feature table for
        the given date range and PIT-joins other feature tables onto it. The
        Dataset knows its own spine from its feature table definitions.

        Args:
            start: Start date (inclusive). String "YYYY-MM-DD" or datetime.
            end: End date (exclusive). String "YYYY-MM-DD" or datetime.
            spine: Optional external spine table with entity keys + timestamps.
                When provided, PIT-joins features onto this instead of the
                implicit spine. Use for external label sets.

        Returns:
            pa.Table with spine columns + feature columns.
        """
        import strata.pit as pit

        start_dt = _parse_datetime(start)
        end_dt = _parse_datetime(end)

        if start_dt >= end_dt:
            raise errors.StrataError(
                context="Reading features from dataset '{}'".format(self._dataset.name),
                cause="start ({}) must be before end ({})".format(start_dt.isoformat(), end_dt.isoformat()),
                fix="Provide a start date earlier than the end date.",
            )

        # Group features by their source table
        table_features: dict[str, list[core.Feature]] = {}
        for feature in self._dataset.features:
            table_name = feature.table_name
            if table_name not in table_features:
                table_features[table_name] = []
            table_features[table_name].append(feature)

        # Include label if present
        if self._dataset.label is not None:
            label = self._dataset.label
            table_name = label.table_name
            if table_name not in table_features:
                table_features[table_name] = []
            # Add label only if not already in the list
            if not any(f.name == label.name and f.table_name == label.table_name for f in table_features[table_name]):
                table_features[table_name].append(label)

        # Determine the spine table (first table referenced by dataset features)
        referenced_tables = list(table_features.keys())
        if not referenced_tables:
            raise errors.StrataError(
                context="Reading features from dataset '{}'".format(self._dataset.name),
                cause="Dataset has no feature references",
                fix="Add features to the dataset definition.",
            )

        spine_table_name = referenced_tables[0]

        # Resolve table metadata for each referenced table
        all_tables = {**self._feature_tables, **self._source_tables}

        # Read data and build spine if not provided
        if spine is None:
            spine = self._build_implicit_spine(
                spine_table_name, all_tables, start_dt, end_dt,
            )

        # Build FeatureTableData for each referenced table
        feature_table_data_list: list[pit.FeatureTableData] = []
        for tbl_name, features in table_features.items():
            table_def = all_tables.get(tbl_name)
            if table_def is None:
                raise errors.StrataError(
                    context="Reading features from dataset '{}'".format(self._dataset.name),
                    cause="Table '{}' referenced by features is not defined".format(tbl_name),
                    fix="Ensure the table definition exists in the project.",
                )

            # Read the feature data from backend
            if not self._project._backend.table_exists(tbl_name):
                raise errors.StrataError(
                    context="Reading features from dataset '{}'".format(self._dataset.name),
                    cause="Table '{}' has no built data".format(tbl_name),
                    fix="Run `strata build` first to materialize the table.",
                )

            data = self._project._backend.read_table(tbl_name)

            # Determine metadata from the table definition
            entity_keys = table_def.entity.join_keys
            timestamp_col = table_def.timestamp_field or "event_ts"
            feature_cols = [f.name for f in features]

            # Get TTL from source if available (RealTimeSource)
            ttl = None
            if hasattr(table_def, "source") and hasattr(table_def.source, "ttl"):
                ttl = table_def.source.ttl

            feature_table_data_list.append(
                pit.FeatureTableData(
                    name=tbl_name,
                    data=data,
                    entity_keys=entity_keys,
                    timestamp_column=timestamp_col,
                    feature_columns=feature_cols,
                    ttl=ttl,
                )
            )

        # Determine spine timestamp column from the spine table
        spine_table_def = all_tables.get(spine_table_name)
        spine_timestamp = (
            spine_table_def.timestamp_field
            if spine_table_def and spine_table_def.timestamp_field
            else "event_ts"
        )

        # Execute PIT join
        result = pit.pit_join(
            spine=spine,
            feature_tables=feature_table_data_list,
            spine_timestamp=spine_timestamp,
        )

        # Build output column mapping (raw feature name -> dataset output name)
        output_columns = _build_output_column_map(self._dataset, spine_timestamp)

        # Select and rename columns for the output
        # Start with entity key columns and timestamp
        if spine_table_def is not None:
            keep_cols = list(spine_table_def.entity.join_keys) + [spine_timestamp]
        else:
            keep_cols = [spine_timestamp]

        # Add feature columns (renamed if needed)
        rename_map: dict[str, str] = {}
        for feature in self._dataset.features:
            raw_name = feature.name
            output_name = output_columns.get(raw_name, raw_name)
            if raw_name in result.column_names:
                keep_cols.append(raw_name)
                if output_name != raw_name:
                    rename_map[raw_name] = output_name

        # Add label column if present
        if self._dataset.label is not None:
            label = self._dataset.label
            if label.name in result.column_names and label.name not in keep_cols:
                keep_cols.append(label.name)

        # Select only the columns we need
        result = result.select(keep_cols)

        # Apply renames
        if rename_map:
            new_names = [rename_map.get(n, n) for n in result.column_names]
            result = result.rename_columns(new_names)

        return result

    def lookup_features(
        self,
        entity_keys: dict[str, str],
    ) -> pa.Table:
        """Look up pre-computed features for a single entity from the online store.

        Returns the latest feature vector for the given entity key. Features
        must have been published via ``strata publish`` first.

        Args:
            entity_keys: Entity key dict (e.g., {"user_id": "123"}).

        Returns:
            pa.Table with 1 row containing feature columns + _feature_timestamp.
            Returns nulls for all features if entity is not found (consistent
            with Feast/Tecton -- missing entities return nulls, not errors).

        Raises:
            StrataError: If no online store is configured.
        """
        if self._project._online_store is None:
            raise errors.StrataError(
                context="Looking up features",
                cause="No online store configured",
                fix="Add online_store section to strata.yaml",
            )

        online_store = self._project._online_store
        online_store.initialize()

        # Group features by their source table
        table_features: dict[str, list[str]] = {}
        for feature in self._dataset.features:
            table_name = feature.table_name
            if table_name not in table_features:
                table_features[table_name] = []
            table_features[table_name].append(feature.name)

        # Read features from online store for each table
        table_results: dict[str, pa.Table] = {}
        for table_name in table_features:
            result = online_store.read_features(table_name, entity_keys)
            table_results[table_name] = result

        # Build output column mapping
        output_columns = _build_output_column_map(self._dataset, "_feature_timestamp")

        # Combine feature columns from all tables into a single row
        combined: dict[str, list] = {}
        oldest_timestamp: str | None = None

        for table_name, feature_names in table_features.items():
            result = table_results[table_name]
            has_data = result.num_rows > 0

            for feat_name in feature_names:
                output_name = output_columns.get(feat_name, feat_name)
                if has_data and feat_name in result.column_names:
                    combined[output_name] = [result.column(feat_name).to_pylist()[0]]
                else:
                    combined[output_name] = [None]

            # Track oldest timestamp (conservative freshness)
            if has_data and "_feature_timestamp" in result.column_names:
                ts = result.column("_feature_timestamp").to_pylist()[0]
                if ts is not None:
                    if oldest_timestamp is None or ts < oldest_timestamp:
                        oldest_timestamp = ts

        # Add _feature_timestamp column
        combined["_feature_timestamp"] = [oldest_timestamp]

        return pa.table(combined)

    def _build_implicit_spine(
        self,
        spine_table_name: str,
        all_tables: dict[str, core.FeatureTable | core.SourceTable],
        start_dt: datetime,
        end_dt: datetime,
    ) -> pa.Table:
        """Build an implicit spine from the primary feature table.

        Reads the primary table's data, filters to the date range, and
        extracts unique (entity_key, timestamp) combinations.

        Args:
            spine_table_name: Name of the table to derive spine from.
            all_tables: All available table definitions.
            start_dt: Start date (inclusive).
            end_dt: End date (exclusive).

        Returns:
            pa.Table with entity key columns + timestamp column.
        """
        import pyarrow.compute as pc

        table_def = all_tables.get(spine_table_name)
        if table_def is None:
            raise errors.StrataError(
                context="Building implicit spine for dataset '{}'".format(self._dataset.name),
                cause="Spine table '{}' is not defined".format(spine_table_name),
                fix="Ensure the table definition exists in the project.",
            )

        if not self._project._backend.table_exists(spine_table_name):
            raise errors.StrataError(
                context="Building implicit spine for dataset '{}'".format(self._dataset.name),
                cause="Table '{}' has no built data".format(spine_table_name),
                fix="Run `strata build` first to materialize the table.",
            )

        data = self._project._backend.read_table(spine_table_name)

        ts_col = table_def.timestamp_field
        if ts_col is None:
            raise errors.StrataError(
                context="Building implicit spine for dataset '{}'".format(self._dataset.name),
                cause="Spine table '{}' has no timestamp_field".format(spine_table_name),
                fix="Set timestamp_field on the table definition, or provide an explicit spine.",
            )
        entity_keys = table_def.entity.join_keys

        # Filter to date range
        ts_array = data.column(ts_col)
        start_scalar = pa.scalar(start_dt, type=ts_array.type)
        end_scalar = pa.scalar(end_dt, type=ts_array.type)

        mask = pc.and_(
            pc.greater_equal(ts_array, start_scalar),
            pc.less(ts_array, end_scalar),
        )
        filtered = data.filter(mask)

        # Select spine columns (entity keys + timestamp)
        spine_cols = list(entity_keys) + [ts_col]
        return filtered.select(spine_cols)


def _parse_datetime(value: datetime | str) -> datetime:
    """Parse a datetime value from string or passthrough datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Support YYYY-MM-DD format
        return datetime.fromisoformat(value)
    msg = "Expected datetime or string, got {}".format(type(value).__name__)
    raise TypeError(msg)


def _build_output_column_map(
    dataset: core.Dataset,
    spine_timestamp: str,
) -> dict[str, str]:
    """Build a mapping from raw feature names to dataset output names.

    Args:
        dataset: The Dataset with output naming rules.
        spine_timestamp: Spine timestamp column name (excluded from mapping).

    Returns:
        Dict mapping raw feature name to output column name.
    """
    output_map: dict[str, str] = {}
    for feature in dataset.features:
        raw = feature.name
        if feature._alias:
            output_map[raw] = feature._alias
        elif dataset.prefix_features:
            output_map[raw] = feature.output_name
        else:
            output_map[raw] = raw
    return output_map


def connect(
    env: str | None = None,
    config_path: str | Path = "strata.yaml",
) -> StrataProject:
    """Connect to a Strata project.

    Loads strata.yaml, resolves the target environment, and returns a
    project handle with live backend/registry/online_store connections.

    Args:
        env: Environment name (uses default_env if None).
        config_path: Path to strata.yaml.

    Returns:
        StrataProject instance.
    """
    strata_settings = settings.load_strata_settings(path=config_path, env=env)
    return StrataProject(strata_settings)
