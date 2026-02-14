"""Tests for BoundDataset.lookup_features() -- online feature serving."""

from __future__ import annotations

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

_STRATA_YAML_ONLINE = """\
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
    online_store:
      kind: sqlite
      path: {online_path}
"""

_STRATA_YAML_NO_ONLINE = """\
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


def _write_strata_yaml(
    tmp_path: Path,
    *,
    with_online_store: bool = True,
) -> Path:
    """Write a strata.yaml and return its path."""
    strata_dir = tmp_path / ".strata"
    strata_dir.mkdir(parents=True, exist_ok=True)

    registry_path = str(strata_dir / "registry.db")
    data_path = str(strata_dir / "data")
    online_path = str(strata_dir / "online.db")
    yaml_path = tmp_path / "strata.yaml"

    template = (
        _STRATA_YAML_ONLINE if with_online_store else _STRATA_YAML_NO_ONLINE
    )
    yaml_path.write_text(
        template.format(
            registry_path=registry_path,
            data_path=data_path,
            online_path=online_path,
        )
    )
    return yaml_path


def _make_entity() -> core.Entity:
    return core.Entity(name="user", join_keys=["user_id"])


def _make_source(name: str = "events") -> sources.BatchSource:
    from strata.infra.backends.local import LocalSourceConfig

    return sources.BatchSource(
        name=name,
        config=LocalSourceConfig(path="./data/events.parquet"),
        timestamp_field="event_ts",
    )


def _make_feature_table(
    name: str = "user_features",
    entity: core.Entity | None = None,
) -> core.FeatureTable:
    if entity is None:
        entity = _make_entity()
    ft = core.FeatureTable(
        name=name,
        source=_make_source(),
        entity=entity,
        timestamp_field="event_ts",
        online=True,
    )
    spend = core.Feature(
        name="spend", table_name=name, field=core.Field(dtype="float64")
    )
    txn_count = core.Feature(
        name="txn_count", table_name=name, field=core.Field(dtype="int64")
    )
    ft._features["spend"] = spend
    ft._features["txn_count"] = txn_count
    return ft


def _make_project_with_online(
    tmp_path: Path,
    *,
    with_online_store: bool = True,
) -> project.StrataProject:
    """Create a StrataProject with optional online store."""
    yaml_path = _write_strata_yaml(
        tmp_path, with_online_store=with_online_store
    )
    strata_settings = settings.load_strata_settings(path=yaml_path)
    return project.StrataProject(strata_settings)


def _make_bound_dataset(
    proj: project.StrataProject,
    dataset: core.Dataset,
    feature_tables: dict[str, core.FeatureTable],
) -> project.BoundDataset:
    return project.BoundDataset(
        dataset=dataset,
        project=proj,
        feature_tables=feature_tables,
        source_tables={},
    )


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- basic
# ---------------------------------------------------------------------------


class TestLookupFeaturesBasic:
    def test_lookup_features_basic(self, tmp_path: Path) -> None:
        """lookup_features returns correct feature values from online store."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)

        # Write features directly to the online store
        proj._online_store.initialize()
        proj._online_store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend": 500.0, "txn_count": 10},
            timestamp="2024-06-01T00:00:00Z",
        )

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend, ft.txn_count],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "123"})

        assert isinstance(result, pa.Table)
        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend"] == [500.0]
        assert data["txn_count"] == [10]


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- missing entity
# ---------------------------------------------------------------------------


class TestLookupFeaturesMissingEntity:
    def test_lookup_features_missing_entity(self, tmp_path: Path) -> None:
        """Missing entity returns 1-row table with all nulls."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend, ft.txn_count],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "nonexistent"})

        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend"] == [None]
        assert data["txn_count"] == [None]


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- _feature_timestamp
# ---------------------------------------------------------------------------


class TestLookupFeaturesTimestamp:
    def test_lookup_features_has_timestamp(self, tmp_path: Path) -> None:
        """Result includes _feature_timestamp column with correct value."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()
        proj._online_store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend": 100.0, "txn_count": 5},
            timestamp="2024-03-15T10:00:00Z",
        )

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "123"})

        assert "_feature_timestamp" in result.column_names
        assert result.to_pydict()["_feature_timestamp"] == [
            "2024-03-15T10:00:00Z"
        ]

    def test_lookup_features_missing_entity_timestamp_is_none(
        self, tmp_path: Path
    ) -> None:
        """Missing entity has None _feature_timestamp."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "nobody"})

        assert result.to_pydict()["_feature_timestamp"] == [None]


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- no online store
# ---------------------------------------------------------------------------


class TestLookupFeaturesNoOnlineStore:
    def test_lookup_features_no_online_store(self, tmp_path: Path) -> None:
        """Project without online_store config raises StrataError."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path, with_online_store=False)

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        with pytest.raises(
            errors.StrataError, match="No online store configured"
        ):
            bound_ds.lookup_features({"user_id": "123"})


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- multiple tables
# ---------------------------------------------------------------------------


class TestLookupFeaturesMultipleTables:
    def test_lookup_features_multiple_tables(self, tmp_path: Path) -> None:
        """Features from 2 tables are retrieved and combined."""
        entity = _make_entity()

        ft_spend = core.FeatureTable(
            name="user_spend",
            source=_make_source(),
            entity=entity,
            timestamp_field="event_ts",
            online=True,
        )
        spend_feat = core.Feature(
            name="spend",
            table_name="user_spend",
            field=core.Field(dtype="float64"),
        )
        ft_spend._features["spend"] = spend_feat

        ft_clicks = core.FeatureTable(
            name="user_clicks",
            source=_make_source("clicks"),
            entity=entity,
            timestamp_field="event_ts",
            online=True,
        )
        clicks_feat = core.Feature(
            name="clicks",
            table_name="user_clicks",
            field=core.Field(dtype="int64"),
        )
        ft_clicks._features["clicks"] = clicks_feat

        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()

        # Write features to both tables
        proj._online_store.write_features(
            table_name="user_spend",
            entity_key={"user_id": "A"},
            features={"spend": 250.0},
            timestamp="2024-06-01T00:00:00Z",
        )
        proj._online_store.write_features(
            table_name="user_clicks",
            entity_key={"user_id": "A"},
            features={"clicks": 42},
            timestamp="2024-06-02T00:00:00Z",
        )

        dataset = core.Dataset(
            name="test_ds",
            features=[spend_feat, clicks_feat],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(
            proj,
            dataset,
            {"user_spend": ft_spend, "user_clicks": ft_clicks},
        )

        result = bound_ds.lookup_features({"user_id": "A"})

        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend"] == [250.0]
        assert data["clicks"] == [42]
        # Oldest timestamp is used (conservative)
        assert data["_feature_timestamp"] == ["2024-06-01T00:00:00Z"]

    def test_lookup_features_partial_missing(self, tmp_path: Path) -> None:
        """When one table has the entity and another doesn't, partial nulls."""
        entity = _make_entity()

        ft_spend = core.FeatureTable(
            name="user_spend",
            source=_make_source(),
            entity=entity,
            timestamp_field="event_ts",
            online=True,
        )
        spend_feat = core.Feature(
            name="spend",
            table_name="user_spend",
            field=core.Field(dtype="float64"),
        )
        ft_spend._features["spend"] = spend_feat

        ft_clicks = core.FeatureTable(
            name="user_clicks",
            source=_make_source("clicks"),
            entity=entity,
            timestamp_field="event_ts",
            online=True,
        )
        clicks_feat = core.Feature(
            name="clicks",
            table_name="user_clicks",
            field=core.Field(dtype="int64"),
        )
        ft_clicks._features["clicks"] = clicks_feat

        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()

        # Only write spend, not clicks
        proj._online_store.write_features(
            table_name="user_spend",
            entity_key={"user_id": "A"},
            features={"spend": 100.0},
            timestamp="2024-06-01T00:00:00Z",
        )

        dataset = core.Dataset(
            name="test_ds",
            features=[spend_feat, clicks_feat],
            prefix_features=False,
        )
        bound_ds = _make_bound_dataset(
            proj,
            dataset,
            {"user_spend": ft_spend, "user_clicks": ft_clicks},
        )

        result = bound_ds.lookup_features({"user_id": "A"})

        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend"] == [100.0]
        assert data["clicks"] == [None]


# ---------------------------------------------------------------------------
# Tests: lookup_features() -- column naming
# ---------------------------------------------------------------------------


class TestLookupFeaturesColumnNaming:
    def test_prefix_features_true(self, tmp_path: Path) -> None:
        """prefix_features=True produces table__feature column names."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()
        proj._online_store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend": 100.0, "txn_count": 5},
            timestamp="2024-01-01T00:00:00Z",
        )

        dataset = core.Dataset(
            name="test_ds",
            features=[ft.spend, ft.txn_count],
            prefix_features=True,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "123"})

        assert "user_features__spend" in result.column_names
        assert "user_features__txn_count" in result.column_names

    def test_alias_overrides_prefix(self, tmp_path: Path) -> None:
        """Feature alias overrides prefix_features."""
        ft = _make_feature_table()
        proj = _make_project_with_online(tmp_path)
        proj._online_store.initialize()
        proj._online_store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend": 200.0},
            timestamp="2024-01-01T00:00:00Z",
        )

        aliased = ft.spend.alias("total_spend")
        dataset = core.Dataset(
            name="test_ds",
            features=[aliased],
            prefix_features=True,
        )
        bound_ds = _make_bound_dataset(proj, dataset, {"user_features": ft})

        result = bound_ds.lookup_features({"user_id": "123"})

        assert "total_spend" in result.column_names
        assert "user_features__spend" not in result.column_names
        assert result.to_pydict()["total_spend"] == [200.0]
