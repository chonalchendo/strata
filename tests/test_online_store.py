"""Tests for the online store abstraction and SQLite implementation."""

from __future__ import annotations

import pyarrow as pa
import pytest

import strata.core as core
import strata.serving.sqlite as sqlite_store
import strata.sources as sources
from strata.backends.local import LocalSourceConfig


# ---------------------------------------------------------------------------
# SqliteOnlineStore CRUD tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def store(tmp_path):
    """Create a SqliteOnlineStore with a temp database path."""
    db_path = str(tmp_path / "online.db")
    s = sqlite_store.SqliteOnlineStore(path=db_path)
    s.initialize()
    return s


class TestSqliteOnlineStoreInitialize:
    def test_initialize_creates_db(self, tmp_path):
        """Initialize creates the database file and is idempotent."""
        db_path = tmp_path / "online.db"
        s = sqlite_store.SqliteOnlineStore(path=str(db_path))
        assert not db_path.exists()

        s.initialize()
        assert db_path.exists()

        # Idempotent -- second call does not error
        s.initialize()
        assert db_path.exists()


class TestSqliteOnlineStoreWriteRead:
    def test_write_and_read_features(self, store):
        """Write a single entity and read it back."""
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend_90d": 500.0, "txn_count": 10},
            timestamp="2024-01-01T00:00:00Z",
        )
        result = store.read_features("user_features", {"user_id": "123"})

        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend_90d"] == [500.0]
        assert data["txn_count"] == [10]
        assert data["_feature_timestamp"] == ["2024-01-01T00:00:00Z"]

    def test_write_features_upsert(self, store):
        """Writing the same entity twice updates the data."""
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend_90d": 500.0},
            timestamp="2024-01-01T00:00:00Z",
        )
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend_90d": 750.0},
            timestamp="2024-02-01T00:00:00Z",
        )
        result = store.read_features("user_features", {"user_id": "123"})

        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend_90d"] == [750.0]
        assert data["_feature_timestamp"] == ["2024-02-01T00:00:00Z"]

    def test_read_missing_entity(self, store):
        """Reading a missing entity returns an empty table, not an error."""
        result = store.read_features("user_features", {"user_id": "999"})

        assert result.num_rows == 0
        assert "_feature_timestamp" in result.column_names

    def test_feature_timestamp_column(self, store):
        """read_features result always includes _feature_timestamp column."""
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "abc"},
            features={"score": 0.95},
            timestamp="2024-06-15T12:00:00Z",
        )
        result = store.read_features("user_features", {"user_id": "abc"})

        assert "_feature_timestamp" in result.column_names
        assert result.to_pydict()["_feature_timestamp"] == ["2024-06-15T12:00:00Z"]

    def test_multiple_tables_isolation(self, store):
        """Features from different tables are isolated."""
        store.write_features(
            table_name="table_a",
            entity_key={"id": "1"},
            features={"val": 10},
            timestamp="2024-01-01T00:00:00Z",
        )
        store.write_features(
            table_name="table_b",
            entity_key={"id": "1"},
            features={"val": 20},
            timestamp="2024-01-01T00:00:00Z",
        )

        result_a = store.read_features("table_a", {"id": "1"})
        result_b = store.read_features("table_b", {"id": "1"})

        assert result_a.to_pydict()["val"] == [10]
        assert result_b.to_pydict()["val"] == [20]


class TestSqliteOnlineStoreWriteBatch:
    def test_write_batch(self, store):
        """Write a batch of entities and read each back."""
        data = pa.table({
            "user_id": ["u1", "u2", "u3"],
            "spend_90d": [100.0, 200.0, 300.0],
            "txn_count": [5, 10, 15],
            "event_ts": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
            ],
        })
        store.write_batch(
            table_name="user_features",
            data=data,
            entity_columns=["user_id"],
            timestamp_column="event_ts",
        )

        for uid, expected_spend in [("u1", 100.0), ("u2", 200.0), ("u3", 300.0)]:
            result = store.read_features("user_features", {"user_id": uid})
            assert result.num_rows == 1
            assert result.to_pydict()["spend_90d"] == [expected_spend]

    def test_write_batch_latest_per_entity(self, store):
        """When batch has multiple rows per entity, only the latest is stored."""
        data = pa.table({
            "user_id": ["u1", "u1", "u1"],
            "spend_90d": [100.0, 200.0, 300.0],
            "event_ts": [
                "2024-01-01T00:00:00Z",
                "2024-03-01T00:00:00Z",  # latest
                "2024-02-01T00:00:00Z",
            ],
        })
        store.write_batch(
            table_name="user_features",
            data=data,
            entity_columns=["user_id"],
            timestamp_column="event_ts",
        )

        result = store.read_features("user_features", {"user_id": "u1"})
        assert result.num_rows == 1
        data = result.to_pydict()
        assert data["spend_90d"] == [200.0]
        assert data["_feature_timestamp"] == ["2024-03-01T00:00:00Z"]

    def test_write_batch_empty_table(self, store):
        """Writing an empty batch is a no-op."""
        data = pa.table({
            "user_id": pa.array([], type=pa.string()),
            "spend_90d": pa.array([], type=pa.float64()),
            "event_ts": pa.array([], type=pa.string()),
        })
        store.write_batch(
            table_name="user_features",
            data=data,
            entity_columns=["user_id"],
            timestamp_column="event_ts",
        )
        result = store.read_features("user_features", {"user_id": "u1"})
        assert result.num_rows == 0

    def test_write_batch_composite_entity_key(self, store):
        """Batch write with composite (multi-column) entity key."""
        data = pa.table({
            "user_id": ["u1", "u1"],
            "device_id": ["d1", "d2"],
            "score": [0.8, 0.9],
            "event_ts": ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
        })
        store.write_batch(
            table_name="device_features",
            data=data,
            entity_columns=["user_id", "device_id"],
            timestamp_column="event_ts",
        )

        r1 = store.read_features("device_features", {"user_id": "u1", "device_id": "d1"})
        r2 = store.read_features("device_features", {"user_id": "u1", "device_id": "d2"})

        assert r1.num_rows == 1
        assert r1.to_pydict()["score"] == [0.8]
        assert r2.num_rows == 1
        assert r2.to_pydict()["score"] == [0.9]


class TestSqliteOnlineStoreTeardown:
    def test_teardown(self, store):
        """After teardown, reads return empty."""
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend_90d": 500.0},
            timestamp="2024-01-01T00:00:00Z",
        )
        store.teardown()

        result = store.read_features("user_features", {"user_id": "123"})
        assert result.num_rows == 0


class TestSqliteOnlineStoreCanonicalKey:
    def test_key_order_irrelevant(self, store):
        """Entity key order doesn't matter -- canonical JSON sorts keys."""
        store.write_features(
            table_name="t",
            entity_key={"b": "2", "a": "1"},
            features={"val": 42},
            timestamp="2024-01-01T00:00:00Z",
        )
        # Read with different key order
        result = store.read_features("t", {"a": "1", "b": "2"})
        assert result.num_rows == 1
        assert result.to_pydict()["val"] == [42]


# ---------------------------------------------------------------------------
# FeatureTable.online field tests
# ---------------------------------------------------------------------------


class TestFeatureTableOnline:
    @pytest.fixture()
    def _entity(self):
        return core.Entity(name="user", join_keys=["user_id"])

    @pytest.fixture()
    def _source(self):
        cfg = LocalSourceConfig(path="/tmp/events.parquet")
        return sources.BatchSource(
            name="events",
            config=cfg,
            timestamp_field="event_ts",
        )

    def test_feature_table_online_default_false(self, _entity, _source):
        """FeatureTable.online defaults to False."""
        ft = core.FeatureTable(
            name="test",
            source=_source,
            entity=_entity,
            timestamp_field="event_ts",
        )
        assert ft.online is False

    def test_feature_table_online_true(self, _entity, _source):
        """FeatureTable.online can be set to True."""
        ft = core.FeatureTable(
            name="test",
            source=_source,
            entity=_entity,
            timestamp_field="event_ts",
            online=True,
        )
        assert ft.online is True


# ---------------------------------------------------------------------------
# EnvironmentSettings.online_store field tests
# ---------------------------------------------------------------------------


class TestEnvironmentSettingsOnlineStore:
    def test_settings_online_store_optional(self):
        """EnvironmentSettings works without online_store."""
        import strata.settings as settings

        env = settings.EnvironmentSettings(
            registry={"kind": "sqlite", "path": ":memory:"},
            backend={"kind": "duckdb", "path": "/tmp/test_data", "catalog": "test"},
        )
        assert env.online_store is None

    def test_settings_online_store_sqlite(self, tmp_path):
        """EnvironmentSettings accepts sqlite online_store config."""
        import strata.settings as settings

        db_path = str(tmp_path / "online.db")
        env = settings.EnvironmentSettings(
            registry={"kind": "sqlite", "path": ":memory:"},
            backend={"kind": "duckdb", "path": "/tmp/test_data", "catalog": "test"},
            online_store={"kind": "sqlite", "path": db_path},
        )
        assert env.online_store is not None
        assert env.online_store.kind == "sqlite"
        assert env.online_store.path == db_path
