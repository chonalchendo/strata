"""SQLite online store implementation for low-latency feature serving.

Stores the latest feature vector per entity key in a SQLite database.
Entity keys are stored as canonical JSON strings (sorted keys).
Feature data is stored as JSON strings.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Literal

import pyarrow as pa

import strata.serving.base as base


class SqliteOnlineStore(base.BaseOnlineStore):
    """SQLite-backed online feature store.

    Stores feature vectors in a single `features` table with composite
    primary key (table_name, entity_key). Suitable for local development
    and single-machine deployments.

    Example:
        store = SqliteOnlineStore(path="/tmp/online.db")
        store.initialize()
        store.write_features(
            table_name="user_features",
            entity_key={"user_id": "123"},
            features={"spend_90d": 500.0},
            timestamp="2024-01-01T00:00:00Z",
        )
    """

    kind: Literal["sqlite"] = "sqlite"
    path: str

    def initialize(self) -> None:
        """Create the features table if it doesn't exist. Idempotent."""
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS features (
                    table_name TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    feature_data TEXT NOT NULL,
                    feature_timestamp TEXT NOT NULL,
                    PRIMARY KEY (table_name, entity_key)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def write_features(
        self,
        table_name: str,
        entity_key: dict[str, str],
        features: dict[str, object],
        timestamp: str,
    ) -> None:
        """Write a single entity's feature vector (upsert)."""
        entity_key_json = _canonical_key(entity_key)
        feature_data_json = json.dumps(features)

        conn = sqlite3.connect(self.path)
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO features
                    (table_name, entity_key, feature_data, feature_timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (table_name, entity_key_json, feature_data_json, timestamp),
            )
            conn.commit()
        finally:
            conn.close()

    def write_batch(
        self,
        table_name: str,
        data: pa.Table,
        entity_columns: list[str],
        timestamp_column: str,
    ) -> None:
        """Write latest-per-entity from a batch into the online store.

        For each unique entity key, finds the row with the latest timestamp
        and upserts it.
        """
        if data.num_rows == 0:
            return

        # Convert to Python for processing
        rows = data.to_pydict()
        num_rows = data.num_rows

        # Feature columns are everything except entity columns and timestamp
        feature_columns = [
            c for c in data.column_names
            if c not in entity_columns and c != timestamp_column
        ]

        # Group rows by entity key and find latest per entity
        latest_per_entity: dict[str, dict] = {}
        for i in range(num_rows):
            entity_key = {col: str(rows[col][i]) for col in entity_columns}
            key_json = _canonical_key(entity_key)
            timestamp = str(rows[timestamp_column][i])

            if key_json not in latest_per_entity or timestamp > latest_per_entity[key_json]["timestamp"]:
                features = {col: rows[col][i] for col in feature_columns}
                latest_per_entity[key_json] = {
                    "entity_key_json": key_json,
                    "features": features,
                    "timestamp": timestamp,
                }

        # Bulk upsert
        conn = sqlite3.connect(self.path)
        try:
            for entry in latest_per_entity.values():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO features
                        (table_name, entity_key, feature_data, feature_timestamp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        table_name,
                        entry["entity_key_json"],
                        json.dumps(entry["features"]),
                        entry["timestamp"],
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def read_features(
        self,
        table_name: str,
        entity_key: dict[str, str],
    ) -> pa.Table:
        """Read features for a single entity key.

        Returns a 1-row pa.Table with feature columns + _feature_timestamp.
        Returns an empty table if entity not found (nulls for missing entities).
        """
        entity_key_json = _canonical_key(entity_key)

        conn = sqlite3.connect(self.path)
        try:
            cursor = conn.execute(
                """
                SELECT feature_data, feature_timestamp
                FROM features
                WHERE table_name = ? AND entity_key = ?
                """,
                (table_name, entity_key_json),
            )
            row = cursor.fetchone()
        except sqlite3.OperationalError:
            # Table doesn't exist (e.g., after teardown or before initialize)
            row = None
        finally:
            conn.close()

        if row is None:
            # Return empty table -- missing entities return no rows
            return pa.table({"_feature_timestamp": pa.array([], type=pa.string())})

        feature_data = json.loads(row[0])
        feature_timestamp = row[1]

        # Build 1-row table with feature columns + _feature_timestamp
        result_dict: dict[str, list] = {}
        for key, value in feature_data.items():
            result_dict[key] = [value]
        result_dict["_feature_timestamp"] = [feature_timestamp]

        return pa.table(result_dict)

    def teardown(self) -> None:
        """Drop the features table."""
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("DROP TABLE IF EXISTS features")
            conn.commit()
        finally:
            conn.close()


def _canonical_key(entity_key: dict[str, str]) -> str:
    """Convert entity key dict to canonical JSON string (sorted keys)."""
    return json.dumps(entity_key, sort_keys=True)
