"""Base online store abstraction for low-latency feature serving.

The online store is a key-value cache storing the latest feature vector per entity key,
separate from the offline backend (per ADR-001). BaseOnlineStore defines the interface
that all online store implementations must follow.
"""

from __future__ import annotations

import abc

import pydantic as pdt
import pyarrow as pa


class BaseOnlineStore(abc.ABC, pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Abstract base class for online feature stores.

    Online stores provide low-latency read access to the latest feature values
    per entity key. They are populated by `strata publish` and queried by
    `Dataset.lookup_features()`.

    Implementations must be frozen Pydantic models (config-as-code).
    """

    kind: str

    @abc.abstractmethod
    def initialize(self) -> None:
        """Create tables/schema if needed. Idempotent."""
        ...

    @abc.abstractmethod
    def write_features(
        self,
        table_name: str,
        entity_key: dict[str, str],
        features: dict[str, object],
        timestamp: str,
    ) -> None:
        """Write a single entity's feature vector (upsert by entity key).

        Args:
            table_name: Name of the feature table.
            entity_key: Entity key as dict (e.g., {"user_id": "123"}).
            features: Feature values as dict (e.g., {"spend_90d": 500.0}).
            timestamp: ISO 8601 timestamp string for the feature vector.
        """
        ...

    @abc.abstractmethod
    def write_batch(
        self,
        table_name: str,
        data: pa.Table,
        entity_columns: list[str],
        timestamp_column: str,
    ) -> None:
        """Write latest-per-entity from a batch (bulk publish).

        For each unique entity key in the data, finds the row with the latest
        timestamp and upserts it into the online store.

        Args:
            table_name: Name of the feature table.
            data: PyArrow table with entity columns, feature columns, and timestamp.
            entity_columns: Column names that form the entity key.
            timestamp_column: Column name containing the timestamp.
        """
        ...

    @abc.abstractmethod
    def read_features(
        self,
        table_name: str,
        entity_key: dict[str, str],
    ) -> pa.Table:
        """Read features for a single entity key.

        Returns a 1-row pa.Table with feature columns and a _feature_timestamp column.
        If the entity key is not found, returns an empty pa.Table (not an error).

        Args:
            table_name: Name of the feature table.
            entity_key: Entity key as dict (e.g., {"user_id": "123"}).

        Returns:
            pa.Table with 1 row (found) or 0 rows (not found).
        """
        ...

    @abc.abstractmethod
    def teardown(self) -> None:
        """Remove all data. Used for cleanup."""
        ...
