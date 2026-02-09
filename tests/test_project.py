"""Tests for the project handle, connect(), and BoundDataset.read_features()."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

import strata.core as core
import strata.errors as errors
import strata.project as project
import strata.settings as settings
import strata.sources as sources

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STRATA_YAML_TEMPLATE = """\
name: test-project
default_env: dev
environments:
  dev:
    registry:
      kind: sqlite
      path: {registry_path}
    backend:
      kind: duckdb
      path: {data_path}
      catalog: features
"""


def _ts(year: int, month: int, day: int) -> datetime:
    """Shorthand for creating datetime objects."""
    return datetime(year, month, day)


def _write_strata_yaml(tmp_path: Path) -> Path:
    """Write a minimal strata.yaml and return its path."""
    registry_path = str(tmp_path / ".strata" / "registry.db")
    data_path = str(tmp_path / ".strata" / "data")
    yaml_path = tmp_path / "strata.yaml"
    yaml_path.write_text(
        _STRATA_YAML_TEMPLATE.format(
            registry_path=registry_path,
            data_path=data_path,
        )
    )
    return yaml_path


def _make_entity() -> core.Entity:
    """Create a test entity."""
    return core.Entity(name="user", join_keys=["user_id"])


def _make_source(name: str = "events") -> sources.BatchSource:
    """Create a test batch source."""
    from strata.backends.local import LocalSourceConfig

    return sources.BatchSource(
        name=name,
        config=LocalSourceConfig(path="./data/events.parquet"),
        timestamp_field="event_ts",
    )


def _make_feature_table(
    name: str = "user_features",
    entity: core.Entity | None = None,
    timestamp_field: str = "event_ts",
) -> core.FeatureTable:
    """Create a test FeatureTable."""
    if entity is None:
        entity = _make_entity()
    ft = core.FeatureTable(
        name=name,
        source=_make_source(),
        entity=entity,
        timestamp_field=timestamp_field,
    )
    # Manually register features
    spend = core.Feature(name="spend", table_name=name, field=core.Field(dtype="float64"))
    txn_count = core.Feature(name="txn_count", table_name=name, field=core.Field(dtype="int64"))
    ft._features["spend"] = spend
    ft._features["txn_count"] = txn_count
    return ft


def _make_project_with_data(
    tmp_path: Path,
    table_data: dict[str, pa.Table],
) -> project.StrataProject:
    """Create a StrataProject with data written to the backend.

    Args:
        tmp_path: Temporary directory.
        table_data: Dict of table_name -> pa.Table to write.

    Returns:
        StrataProject with data materialized.
    """
    yaml_path = _write_strata_yaml(tmp_path)
    strata_settings = settings.load_strata_settings(path=yaml_path)
    proj = project.StrataProject(strata_settings)

    for table_name, data in table_data.items():
        proj._backend.write_table(table_name=table_name, data=data, mode="append")

    return proj


def _make_bound_dataset(
    proj: project.StrataProject,
    dataset: core.Dataset,
    feature_tables: dict[str, core.FeatureTable],
    source_tables: dict[str, core.SourceTable] | None = None,
) -> project.BoundDataset:
    """Create a BoundDataset directly (without discovery)."""
    return project.BoundDataset(
        dataset=dataset,
        project=proj,
        feature_tables=feature_tables,
        source_tables=source_tables or {},
    )


# ---------------------------------------------------------------------------
# Tests: connect()
# ---------------------------------------------------------------------------


class TestConnect:
    """Tests for strata.connect() settings loading."""

    def test_connect_loads_settings(self, tmp_path: Path) -> None:
        """connect() loads strata.yaml and returns StrataProject."""
        yaml_path = _write_strata_yaml(tmp_path)
        proj = project.connect(config_path=yaml_path)

        assert isinstance(proj, project.StrataProject)
        assert proj.name == "test-project"
        assert proj.env == "dev"

    def test_connect_with_env(self, tmp_path: Path) -> None:
        """connect() respects the env parameter."""
        yaml_path = _write_strata_yaml(tmp_path)
        proj = project.connect(env="dev", config_path=yaml_path)

        assert proj.env == "dev"

    def test_connect_missing_config_raises(self, tmp_path: Path) -> None:
        """connect() raises ConfigNotFoundError if strata.yaml missing."""
        with pytest.raises(errors.ConfigNotFoundError):
            project.connect(config_path=tmp_path / "nonexistent.yaml")


# ---------------------------------------------------------------------------
# Tests: get_dataset()
# ---------------------------------------------------------------------------


class TestGetDataset:
    """Tests for StrataProject.get_dataset()."""

    def test_get_dataset_not_found(self, tmp_path: Path) -> None:
        """get_dataset() raises StrataError when dataset doesn't exist."""
        yaml_path = _write_strata_yaml(tmp_path)
        strata_settings = settings.load_strata_settings(path=yaml_path)

        # Point paths to an empty directory so discovery finds nothing
        proj = project.StrataProject(strata_settings)

        with pytest.raises(errors.StrataError, match="not found"):
            proj.get_dataset("nonexistent")


# ---------------------------------------------------------------------------
# Tests: read_features() - basic
# ---------------------------------------------------------------------------


class TestReadFeaturesBasic:
    """Basic read_features() behavior."""

    def test_read_features_basic(self, tmp_path: Path) -> None:
        """read_features() returns correct columns with PIT-correct data."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        # Write feature data to backend
        data = pa.table({
            "user_id": ["A", "A", "B"],
            "event_ts": pa.array([
                _ts(2024, 1, 5), _ts(2024, 1, 15), _ts(2024, 1, 10),
            ], type=pa.timestamp("us")),
            "spend": [100.0, 200.0, 300.0],
            "txn_count": [1, 2, 3],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend, ft.txn_count],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(
            proj, dataset, {"user_features": ft},
        )

        result = bound_ds.read_features(
            start="2024-01-01",
            end="2024-02-01",
        )

        assert isinstance(result, pa.Table)
        assert "user_id" in result.column_names
        assert "spend" in result.column_names
        assert "txn_count" in result.column_names
        assert len(result) == 3

    def test_read_features_with_string_dates(self, tmp_path: Path) -> None:
        """read_features() accepts string dates."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        # String dates should work
        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")
        assert len(result) == 1

    def test_read_features_with_datetime_objects(self, tmp_path: Path) -> None:
        """read_features() accepts datetime objects."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 2, 1),
        )
        assert len(result) == 1

    def test_read_features_date_range_filters(self, tmp_path: Path) -> None:
        """read_features() filters data to the specified date range."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A", "A", "A"],
            "event_ts": pa.array([
                _ts(2024, 1, 5),   # Inside range
                _ts(2024, 2, 15),  # Outside range
                _ts(2024, 1, 20),  # Inside range
            ], type=pa.timestamp("us")),
            "spend": [100.0, 200.0, 300.0],
            "txn_count": [1, 2, 3],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        # Only 2 rows should be in range (Jan 5 and Jan 20)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Tests: read_features() - PIT correctness
# ---------------------------------------------------------------------------


class TestReadFeaturesPITCorrectness:
    """PIT join correctness tests via BoundDataset.read_features()."""

    def test_pit_correctness_two_tables(self, tmp_path: Path) -> None:
        """Features from two tables are PIT-joined correctly."""
        entity = _make_entity()

        # First feature table: user_spend
        ft_spend = core.FeatureTable(
            name="user_spend",
            source=_make_source(),
            entity=entity,
            timestamp_field="event_ts",
        )
        spend_feat = core.Feature(name="spend", table_name="user_spend", field=core.Field(dtype="float64"))
        ft_spend._features["spend"] = spend_feat

        # Second feature table: user_clicks
        ft_clicks = core.FeatureTable(
            name="user_clicks",
            source=_make_source("clicks"),
            entity=entity,
            timestamp_field="event_ts",
        )
        clicks_feat = core.Feature(name="clicks", table_name="user_clicks", field=core.Field(dtype="int64"))
        ft_clicks._features["clicks"] = clicks_feat

        # Write data: spend has data at Jan 5, clicks at Jan 8
        spend_data = pa.table({
            "user_id": ["A", "B"],
            "event_ts": pa.array([_ts(2024, 1, 5), _ts(2024, 1, 5)], type=pa.timestamp("us")),
            "spend": [100.0, 300.0],
        })
        clicks_data = pa.table({
            "user_id": ["A", "B"],
            "event_ts": pa.array([_ts(2024, 1, 8), _ts(2024, 1, 12)], type=pa.timestamp("us")),
            "clicks": [10, 20],
        })

        proj = _make_project_with_data(tmp_path, {
            "user_spend": spend_data,
            "user_clicks": clicks_data,
        })

        dataset = core.Dataset(
            name="test_ds",
            features=[spend_feat, clicks_feat],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(
            proj, dataset,
            {"user_spend": ft_spend, "user_clicks": ft_clicks},
        )

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        assert "spend" in result.column_names
        assert "clicks" in result.column_names
        # Spine comes from first table (user_spend) - 2 rows
        assert len(result) == 2

    def test_no_future_leakage(self, tmp_path: Path) -> None:
        """Features after the spine timestamp are not joined (no leakage)."""
        entity = _make_entity()

        # Primary table provides spine events at Jan 10
        ft_primary = core.FeatureTable(
            name="primary",
            source=_make_source(),
            entity=entity,
            timestamp_field="event_ts",
        )
        primary_feat = core.Feature(name="value", table_name="primary", field=core.Field(dtype="float64"))
        ft_primary._features["value"] = primary_feat

        # Feature table has data at Jan 5 (before) and Jan 15 (after spine)
        ft_features = core.FeatureTable(
            name="features",
            source=_make_source("features_src"),
            entity=entity,
            timestamp_field="event_ts",
        )
        feat = core.Feature(name="score", table_name="features", field=core.Field(dtype="float64"))
        ft_features._features["score"] = feat

        primary_data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "value": [1.0],
        })
        features_data = pa.table({
            "user_id": ["A", "A"],
            "event_ts": pa.array([_ts(2024, 1, 5), _ts(2024, 1, 15)], type=pa.timestamp("us")),
            "score": [50.0, 999.0],
        })

        proj = _make_project_with_data(tmp_path, {
            "primary": primary_data,
            "features": features_data,
        })

        dataset = core.Dataset(
            name="test_ds",
            features=[primary_feat, feat],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(
            proj, dataset,
            {"primary": ft_primary, "features": ft_features},
        )

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")
        df = result.to_pandas()

        # score should be 50.0 (from Jan 5), NOT 999.0 (from Jan 15 - future)
        assert df["score"].iloc[0] == 50.0


# ---------------------------------------------------------------------------
# Tests: read_features() - external spine
# ---------------------------------------------------------------------------


class TestReadFeaturesExternalSpine:
    """Tests for read_features() with an explicit spine parameter."""

    def test_external_spine(self, tmp_path: Path) -> None:
        """Features are joined onto an external spine table."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A", "B"],
            "event_ts": pa.array([
                _ts(2024, 1, 5), _ts(2024, 1, 10),
            ], type=pa.timestamp("us")),
            "spend": [100.0, 300.0],
            "txn_count": [1, 3],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        # Provide an external spine with custom timestamps
        external_spine = pa.table({
            "user_id": ["A", "B"],
            "event_ts": pa.array([
                _ts(2024, 1, 8), _ts(2024, 1, 12),
            ], type=pa.timestamp("us")),
        })

        result = bound_ds.read_features(
            start="2024-01-01",
            end="2024-02-01",
            spine=external_spine,
        )

        assert len(result) == 2
        assert "spend" in result.column_names
        df = result.to_pandas().sort_values("user_id").reset_index(drop=True)
        # A@Jan8 gets spend from Jan 5 (100.0)
        assert df.loc[df["user_id"] == "A", "spend"].iloc[0] == 100.0
        # B@Jan12 gets spend from Jan 10 (300.0)
        assert df.loc[df["user_id"] == "B", "spend"].iloc[0] == 300.0


# ---------------------------------------------------------------------------
# Tests: read_features() - TTL enforcement
# ---------------------------------------------------------------------------


class TestReadFeaturesTTL:
    """Tests for TTL enforcement via read_features()."""

    def test_ttl_enforcement(self, tmp_path: Path) -> None:
        """Features older than TTL are nulled out."""
        entity = _make_entity()

        ft = core.FeatureTable(
            name="user_features",
            source=sources.RealTimeSource(
                name="events",
                config=sources.base.BaseSourceConfig(),
                timestamp_field="event_ts",
                ttl=timedelta(days=30),
            ),
            entity=entity,
            timestamp_field="event_ts",
        )
        spend_feat = core.Feature(name="spend", table_name="user_features", field=core.Field(dtype="float64"))
        ft._features["spend"] = spend_feat

        # Feature data from Jan 5 - spine at April 10 (>30 days)
        feature_data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 5)], type=pa.timestamp("us")),
            "spend": [100.0],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": feature_data})

        dataset = core.Dataset(
            name="test_ds",
            features=[spend_feat],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        # External spine with a timestamp far beyond TTL
        external_spine = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 4, 10)], type=pa.timestamp("us")),
        })

        result = bound_ds.read_features(
            start="2024-01-01",
            end="2024-05-01",
            spine=external_spine,
        )
        df = result.to_pandas()

        # Feature is >30 days old - should be null
        assert df["spend"].isna().iloc[0]


# ---------------------------------------------------------------------------
# Tests: read_features() - error cases
# ---------------------------------------------------------------------------


class TestReadFeaturesErrors:
    """Error handling in read_features()."""

    def test_start_after_end_raises(self, tmp_path: Path) -> None:
        """start > end raises StrataError."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        with pytest.raises(errors.StrataError, match="start .* must be before end"):
            bound_ds.read_features(start="2024-02-01", end="2024-01-01")

    def test_no_built_data_raises(self, tmp_path: Path) -> None:
        """read_features() raises when table has no built data."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        # Create project but don't write any data
        yaml_path = _write_strata_yaml(tmp_path)
        strata_settings = settings.load_strata_settings(path=yaml_path)
        proj = project.StrataProject(strata_settings)

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        with pytest.raises(errors.StrataError, match="has no built data"):
            bound_ds.read_features(start="2024-01-01", end="2024-02-01")


# ---------------------------------------------------------------------------
# Tests: read_features() - prefix_features and aliases
# ---------------------------------------------------------------------------


class TestReadFeaturesNaming:
    """Column naming with prefix_features and aliases."""

    def test_prefix_features_true(self, tmp_path: Path) -> None:
        """prefix_features=True produces table__feature column names."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=True,  # Default
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        assert "user_features__spend" in result.column_names

    def test_prefix_features_false(self, tmp_path: Path) -> None:
        """prefix_features=False produces short column names."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        assert "spend" in result.column_names

    def test_alias_overrides_prefix(self, tmp_path: Path) -> None:
        """Feature alias overrides prefix_features."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        aliased_spend = ft.spend.alias("total_spend")
        dataset = core.Dataset(
            name="test_ds",
            features=[aliased_spend],
            prefix_features=True,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        assert "total_spend" in result.column_names
        assert "user_features__spend" not in result.column_names


# ---------------------------------------------------------------------------
# Tests: Dataset.label
# ---------------------------------------------------------------------------


class TestDatasetLabel:
    """Tests for Dataset.label field."""

    def test_label_included_in_output(self, tmp_path: Path) -> None:
        """When label is set, the label column appears in read_features output."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        # Add a label feature
        target = core.Feature(name="is_fraud", table_name="user_features", field=core.Field(dtype="bool"))
        ft._features["is_fraud"] = target

        data = pa.table({
            "user_id": ["A", "B"],
            "event_ts": pa.array([_ts(2024, 1, 5), _ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0, 200.0],
            "txn_count": [1, 2],
            "is_fraud": [True, False],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            label=target,
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        assert "is_fraud" in result.column_names
        assert "spend" in result.column_names

    def test_label_none_no_extra_columns(self, tmp_path: Path) -> None:
        """When label is None, no extra label column is added."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })

        proj = _make_project_with_data(tmp_path, {"user_features": data})

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            label=None,
            prefix_features=False,
        )

        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.read_features(start="2024-01-01", end="2024-02-01")

        # Only entity key, timestamp, and spend -- no label column
        assert set(result.column_names) == {"user_id", "event_ts", "spend"}


# ---------------------------------------------------------------------------
# Tests: BoundDataset.name and BoundFeatureTable
# ---------------------------------------------------------------------------


class TestBoundObjects:
    """Tests for BoundDataset and BoundFeatureTable properties."""

    def test_bound_dataset_name(self) -> None:
        """BoundDataset.name returns the dataset name."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)
        dataset = core.Dataset(
            name="my_dataset",
            features=[ft.spend],
            prefix_features=False,
        )

        # Use a minimal mock project (we don't call read_features here)
        bound_ds = project.BoundDataset(
            dataset=dataset,
            project=None,  # type: ignore[arg-type]
            feature_tables={"user_features": ft},
            source_tables={},
        )

        assert bound_ds.name == "my_dataset"

    def test_bound_feature_table_write_and_read(self, tmp_path: Path) -> None:
        """BoundFeatureTable.write() and read() delegate to backend."""
        entity = _make_entity()
        ft = _make_feature_table("user_features", entity)

        yaml_path = _write_strata_yaml(tmp_path)
        strata_settings = settings.load_strata_settings(path=yaml_path)
        proj = project.StrataProject(strata_settings)

        bound_ft = project.BoundFeatureTable(feature_table=ft, project=proj)
        assert bound_ft.name == "user_features"

        # Write data via bound feature table
        data = pa.table({
            "user_id": ["A"],
            "event_ts": pa.array([_ts(2024, 1, 10)], type=pa.timestamp("us")),
            "spend": [100.0],
            "txn_count": [5],
        })
        bound_ft.write(data)

        # Read back
        result = bound_ft.read()
        assert len(result) == 1
        assert result.column("spend").to_pylist() == [100.0]


# ---------------------------------------------------------------------------
# Tests: write_table()
# ---------------------------------------------------------------------------


class TestWriteTable:
    """Tests for StrataProject.write_table()."""

    def test_write_table_and_read_back(self, tmp_path: Path) -> None:
        """project.write_table() writes data readable by backend."""
        yaml_path = _write_strata_yaml(tmp_path)
        strata_settings = settings.load_strata_settings(path=yaml_path)
        proj = project.StrataProject(strata_settings)

        data = pa.table({
            "user_id": ["A", "B"],
            "value": [1.0, 2.0],
        })

        proj.write_table("test_table", data)

        result = proj._backend.read_table("test_table")
        assert len(result) == 2
