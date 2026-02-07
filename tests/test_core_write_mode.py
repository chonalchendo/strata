from datetime import timedelta

import pytest

import strata.core as core
import strata.sources as sources
from strata.plugins.local.storage import LocalSourceConfig


@pytest.fixture
def entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def source():
    return sources.BatchSource(
        name="txns",
        config=LocalSourceConfig(path="./data.parquet"),
        timestamp_field="ts",
    )


class TestWriteMode:
    def test_default_is_append(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
        )
        assert table.write_mode == "append"

    def test_merge_mode(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            write_mode="merge",
        )
        assert table.write_mode == "merge"

    def test_invalid_mode_rejected(self, entity, source):
        with pytest.raises(Exception):
            core.FeatureTable(
                name="test",
                source=source,
                entity=entity,
                timestamp_field="ts",
                write_mode="invalid",
            )


class TestMergeKeys:
    def test_default_none(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
        )
        assert table.merge_keys is None

    def test_explicit_merge_keys(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            write_mode="merge",
            merge_keys=["user_id", "event_date"],
        )
        assert table.merge_keys == ["user_id", "event_date"]


class TestEffectiveMergeKeys:
    def test_defaults_to_entity_join_keys(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            write_mode="merge",
        )
        assert table.effective_merge_keys == ["user_id"]

    def test_explicit_keys_override_entity(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            write_mode="merge",
            merge_keys=["user_id", "event_date"],
        )
        assert table.effective_merge_keys == ["user_id", "event_date"]

    def test_multi_key_entity(self, source):
        entity = core.Entity(
            name="user_device", join_keys=["user_id", "device_id"]
        )
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            write_mode="merge",
        )
        assert table.effective_merge_keys == ["user_id", "device_id"]


class TestLookback:
    def test_default_none(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
        )
        assert table.lookback is None

    def test_with_lookback(self, entity, source):
        table = core.FeatureTable(
            name="test",
            source=source,
            entity=entity,
            timestamp_field="ts",
            lookback=timedelta(hours=6),
        )
        assert table.lookback == timedelta(hours=6)
