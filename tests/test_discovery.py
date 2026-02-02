"""Tests for feature definition discovery and serialization."""

from datetime import timedelta

import pytest

import strata.core as core
import strata.discovery as discovery
import strata.sources as sources
from strata.plugins.local.storage import LocalSourceConfig


class TestSerializeEntity:
    """Test Entity serialization."""

    def test_serialize_entity_basic(self):
        entity = core.Entity(name="user", join_keys=["user_id"])
        spec = discovery.serialize_to_spec(entity, "entity")

        assert spec["name"] == "user"
        assert spec["join_keys"] == ["user_id"]
        assert spec["description"] is None

    def test_serialize_entity_with_description(self):
        entity = core.Entity(
            name="merchant",
            join_keys=["merchant_id"],
            description="A merchant entity",
        )
        spec = discovery.serialize_to_spec(entity, "entity")

        assert spec["description"] == "A merchant entity"


class TestSerializeFeatureTable:
    """Test FeatureTable serialization."""

    @pytest.fixture
    def user_entity(self):
        return core.Entity(name="user", join_keys=["user_id"])

    @pytest.fixture
    def batch_source(self):
        return sources.BatchSource(
            name="transactions",
            config=LocalSourceConfig(path="./data.parquet"),
            timestamp_field="event_ts",
        )

    def test_serialize_feature_table_basic(self, user_entity, batch_source):
        table = core.FeatureTable(
            name="user_features",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_ts",
        )
        spec = discovery.serialize_to_spec(table, "feature_table")

        assert spec["name"] == "user_features"
        assert spec["entity"] == "user"
        assert spec["source"]["type"] == "batch_source"
        assert spec["source"]["name"] == "transactions"

    def test_serialize_feature_table_with_aggregate(self, user_entity, batch_source):
        table = core.FeatureTable(
            name="user_features",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_ts",
        )
        table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64", ge=0),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        spec = discovery.serialize_to_spec(table, "feature_table")

        assert "aggregates" in spec
        assert len(spec["aggregates"]) == 1
        assert spec["aggregates"][0]["name"] == "spend_90d"
        assert spec["aggregates"][0]["function"] == "sum"
        assert spec["aggregates"][0]["window_seconds"] == 90 * 24 * 3600


class TestSerializeDataset:
    """Test Dataset serialization."""

    def test_serialize_dataset_basic(self):
        features = [
            core.Feature(name="spend_90d", table_name="user_features"),
            core.Feature(name="txn_count", table_name="user_features"),
        ]
        dataset = core.Dataset(
            name="fraud_detection",
            features=features,
        )

        spec = discovery.serialize_to_spec(dataset, "dataset")

        assert spec["name"] == "fraud_detection"
        assert len(spec["features"]) == 2
        assert spec["features"][0]["table"] == "user_features"
        assert spec["features"][0]["feature"] == "spend_90d"


class TestCanonicalJson:
    """Test canonical JSON serialization."""

    def test_spec_to_json_deterministic(self):
        spec1 = {"b": 2, "a": 1}
        spec2 = {"a": 1, "b": 2}

        json1 = discovery.spec_to_json(spec1)
        json2 = discovery.spec_to_json(spec2)

        # Should produce identical JSON regardless of input order
        assert json1 == json2
        assert json1 == '{"a":1,"b":2}'

    def test_spec_to_json_no_whitespace(self):
        spec = {"name": "test", "value": [1, 2, 3]}
        json_str = discovery.spec_to_json(spec)

        # No extra whitespace
        assert " " not in json_str
        assert "\n" not in json_str


class TestDefinitionDiscoverer:
    """Test DefinitionDiscoverer class."""

    def test_discoverer_class_exists(self):
        """DefinitionDiscoverer class should be importable."""
        assert hasattr(discovery, "DefinitionDiscoverer")

    def test_discover_all_returns_list(self, tmp_path, monkeypatch):
        """discover_all should return a list of DiscoveredObject."""
        # Create minimal config
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test
default_env: dev
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    storage:
      kind: local
      path: .strata/data
      catalog: features
    compute:
      kind: duckdb
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        assert isinstance(result, list)


class TestDiscovery:
    """Test module discovery."""

    def test_extract_from_module_finds_entity(self, tmp_path):
        # Create a Python file with an Entity
        module_file = tmp_path / "entities" / "user.py"
        module_file.parent.mkdir(parents=True)
        module_file.write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )

        discoverer = discovery.DefinitionDiscoverer(project_root=tmp_path)
        discovered = discoverer._extract_from_module(module_file)

        assert len(discovered) == 1
        assert discovered[0].kind == "entity"
        assert discovered[0].name == "user"

    def test_extract_ignores_private(self, tmp_path):
        module_file = tmp_path / "test.py"
        module_file.write_text(
            """
import strata.core as core
_internal = core.Entity(name="internal", join_keys=["id"])
public = core.Entity(name="public", join_keys=["id"])
"""
        )

        discoverer = discovery.DefinitionDiscoverer(project_root=tmp_path)
        discovered = discoverer._extract_from_module(module_file)

        # Should only find public entity
        assert len(discovered) == 1
        assert discovered[0].name == "public"
