from datetime import timedelta

import pytest

import strata.sources as sources
from strata.plugins.duckdb import DuckDBSourceConfig
from strata.plugins.local.storage import LocalSourceConfig


class TestBatchSource:
    def test_creates_with_config(self):
        config = DuckDBSourceConfig(path="./data/transactions.parquet")
        source = sources.BatchSource(
            name="transactions",
            config=config,
            timestamp_field="event_timestamp",
        )
        assert source.name == "transactions"
        assert source.config.path == "./data/transactions.parquet"
        assert source.timestamp_field == "event_timestamp"

    def test_description_is_optional(self):
        config = DuckDBSourceConfig(path="./data.parquet")
        source = sources.BatchSource(
            name="test",
            config=config,
            timestamp_field="ts",
        )
        assert source.description is None

    def test_accepts_local_config(self):
        config = LocalSourceConfig(path="./data/events.parquet")
        source = sources.BatchSource(
            name="events",
            config=config,
            timestamp_field="created_at",
        )
        assert source.config.format == "parquet"


class TestStreamSource:
    def test_creates_with_config(self):
        config = LocalSourceConfig(path="./stream/")
        source = sources.StreamSource(
            name="events_stream",
            config=config,
            timestamp_field="event_time",
        )
        assert source.name == "events_stream"

    def test_accepts_batch_fallback(self):
        config = LocalSourceConfig(path="./stream/")
        fallback = DuckDBSourceConfig(path="./backfill.parquet")
        source = sources.StreamSource(
            name="events_stream",
            config=config,
            timestamp_field="event_time",
            batch_fallback=fallback,
        )
        assert source.batch_fallback is not None
        assert source.batch_fallback.path == "./backfill.parquet"


class TestRealTimeSource:
    def test_creates_with_ttl(self):
        config = LocalSourceConfig(path="./realtime/")
        source = sources.RealTimeSource(
            name="user_session",
            config=config,
            timestamp_field="last_active",
            ttl=timedelta(hours=24),
        )
        assert source.ttl == timedelta(hours=24)

    def test_ttl_is_optional(self):
        config = LocalSourceConfig(path="./realtime/")
        source = sources.RealTimeSource(
            name="user_session",
            config=config,
            timestamp_field="last_active",
        )
        assert source.ttl is None


class TestSourceKind:
    def test_union_accepts_batch_source(self):
        config = DuckDBSourceConfig(path="./test.parquet")
        source: sources.SourceKind = sources.BatchSource(
            name="test",
            config=config,
            timestamp_field="ts",
        )
        assert isinstance(source, sources.BatchSource)
