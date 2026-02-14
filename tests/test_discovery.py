"""Tests for feature definition discovery and serialization."""

from datetime import timedelta

import pytest

import strata.core as core
import strata.discovery as discovery
import strata.sources as sources
from strata.infra.backends.local.storage import LocalSourceConfig


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

    def test_serialize_feature_table_with_aggregate(
        self, user_entity, batch_source
    ):
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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
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


class TestSmartDiscovery:
    """Test smart discovery with include/exclude patterns."""

    def test_discovers_from_any_location(self, tmp_path, monkeypatch):
        """Smart discovery finds entities anywhere in the project."""
        # Create entities in non-standard locations
        (tmp_path / "src" / "models").mkdir(parents=True)
        (tmp_path / "src" / "models" / "user.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        (tmp_path / "lib" / "features").mkdir(parents=True)
        (tmp_path / "lib" / "features" / "merchant.py").write_text(
            """
import strata.core as core
merchant = core.Entity(name="merchant", join_keys=["merchant_id"])
"""
        )

        # Create config without paths (uses smart discovery)
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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "merchant" in names

    def test_excludes_test_files(self, tmp_path, monkeypatch):
        """Smart discovery excludes test_*.py and *_test.py files."""
        # Create regular file
        (tmp_path / "models.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        # Create test files that should be excluded
        (tmp_path / "test_models.py").write_text(
            """
import strata.core as core
test_entity = core.Entity(name="test_entity", join_keys=["id"])
"""
        )
        (tmp_path / "models_test.py").write_text(
            """
import strata.core as core
another_test = core.Entity(name="another_test", join_keys=["id"])
"""
        )

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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "test_entity" not in names
        assert "another_test" not in names

    def test_excludes_conftest(self, tmp_path, monkeypatch):
        """Smart discovery excludes conftest.py files."""
        # Create regular file
        (tmp_path / "models.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        # Create conftest.py that should be excluded
        (tmp_path / "conftest.py").write_text(
            """
import strata.core as core
fixture_entity = core.Entity(name="fixture_entity", join_keys=["id"])
"""
        )

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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "fixture_entity" not in names

    def test_excludes_tests_directory(self, tmp_path, monkeypatch):
        """Smart discovery excludes **/tests/** directories."""
        # Create regular file
        (tmp_path / "src" / "models.py").parent.mkdir(parents=True)
        (tmp_path / "src" / "models.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        # Create file in tests directory that should be excluded
        (tmp_path / "tests").mkdir()
        (tmp_path / "tests" / "fixtures.py").write_text(
            """
import strata.core as core
test_fixture = core.Entity(name="test_fixture", join_keys=["id"])
"""
        )

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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "test_fixture" not in names

    def test_excludes_venv_directory(self, tmp_path, monkeypatch):
        """Smart discovery excludes venv directories."""
        # Create regular file
        (tmp_path / "models.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        # Create file in venv that should be excluded
        (tmp_path / "venv" / "lib").mkdir(parents=True)
        (tmp_path / "venv" / "lib" / "something.py").write_text(
            """
import strata.core as core
venv_entity = core.Entity(name="venv_entity", join_keys=["id"])
"""
        )

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
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "venv_entity" not in names

    def test_custom_exclude_patterns(self, tmp_path, monkeypatch):
        """Custom exclude patterns work."""
        # Create files
        (tmp_path / "models.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        (tmp_path / "scratch").mkdir()
        (tmp_path / "scratch" / "experiment.py").write_text(
            """
import strata.core as core
scratch_entity = core.Entity(name="scratch_entity", join_keys=["id"])
"""
        )

        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test
default_env: dev
paths:
  exclude:
    - "**/scratch/**"
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "scratch_entity" not in names

    def test_include_restricts_search(self, tmp_path, monkeypatch):
        """Include patterns restrict search to specific directories."""
        # Create files in different locations
        (tmp_path / "src" / "features").mkdir(parents=True)
        (tmp_path / "src" / "features" / "user.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        (tmp_path / "other" / "models.py").parent.mkdir(parents=True)
        (tmp_path / "other" / "models.py").write_text(
            """
import strata.core as core
other_entity = core.Entity(name="other_entity", join_keys=["id"])
"""
        )

        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test
default_env: dev
paths:
  include:
    - src/features/
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        assert "user" in names
        assert "other_entity" not in names

    def test_legacy_mode_still_works(self, tmp_path, monkeypatch):
        """Legacy paths configuration still works."""
        # Create files in legacy structure
        (tmp_path / "entities").mkdir()
        (tmp_path / "entities" / "user.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )
        (tmp_path / "tables").mkdir()
        (tmp_path / "tables" / "features.py").write_text(
            """
import strata.core as core
import strata.sources as sources
from strata.infra.backends.local.storage import LocalSourceConfig

user = core.Entity(name="user", join_keys=["user_id"])
batch = sources.BatchSource(
    name="data",
    config=LocalSourceConfig(path="./data.parquet"),
    timestamp_field="ts",
)
table = core.FeatureTable(
    name="user_features",
    source=batch,
    entity=user,
    timestamp_field="ts",
)
"""
        )
        # Create file outside legacy paths (should not be discovered)
        (tmp_path / "other.py").write_text(
            """
import strata.core as core
other = core.Entity(name="other", join_keys=["id"])
"""
        )

        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test
default_env: dev
paths:
  tables: tables/
  datasets: datasets/
  entities: entities/
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)
        import strata.settings as settings_mod

        strata_settings = settings_mod.load_strata_settings()

        # Verify legacy settings type
        assert isinstance(
            strata_settings.paths, settings_mod.LegacyPathsSettings
        )

        discoverer = discovery.DefinitionDiscoverer(strata_settings)
        result = discoverer.discover_all()

        names = {obj.name for obj in result}
        # User from entities/ and from tables/
        assert "user" in names
        assert "user_features" in names
        # Other should NOT be found (outside legacy paths)
        assert "other" not in names
