"""Source definitions for Strata feature stores."""

from __future__ import annotations

from datetime import timedelta

import pydantic as pdt

import strata.infra.backends.base as base


class BatchSource(pdt.BaseModel):
    """Batch data source for scheduled data pulls.

    Example:
        from strata.infra.backends.duckdb import DuckDBSourceConfig

        transactions = BatchSource(
            name="transactions",
            config=DuckDBSourceConfig(path="./data/transactions.parquet"),
            timestamp_field="event_timestamp",
        )
    """

    name: str
    description: str | None = None
    config: base.BaseSourceConfig
    timestamp_field: str | None = None


class StreamSource(pdt.BaseModel):
    """Streaming data source for continuous data flow.

    Provides batch_fallback for backfill operations when streaming
    data is not available for historical periods.

    Example:
        transactions_stream = StreamSource(
            name="transactions_stream",
            config=KafkaSourceConfig(...),
            timestamp_field="event_timestamp",
            batch_fallback=DuckDBSourceConfig(path="./backfill.parquet"),
        )
    """

    name: str
    description: str | None = None
    config: base.BaseSourceConfig
    timestamp_field: str | None = None
    batch_fallback: base.BaseSourceConfig | None = None


class RealTimeSource(pdt.BaseModel):
    """Real-time source for on-demand feature serving.

    Data has a TTL after which it expires.

    Example:
        user_session = RealTimeSource(
            name="user_session",
            config=...,
            timestamp_field="last_active",
            ttl=timedelta(hours=24),
        )
    """

    name: str
    description: str | None = None
    config: base.BaseSourceConfig
    timestamp_field: str | None = None
    ttl: timedelta | None = None


SourceKind = BatchSource | StreamSource | RealTimeSource
