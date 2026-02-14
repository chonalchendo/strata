"""SQLite registry implementation.

Provides persistent storage of feature definitions with content-based
versioning and change tracking using SQLite.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import strata.infra.backends.base as base
import strata.registry as registry

# Strata version for meta table
_STRATA_VERSION = "0.1.0"


class SqliteRegistry(base.BaseRegistry):
    """SQLite-backed registry for feature definitions.

    Stores objects in a hybrid schema with:
    - objects: kind, name, spec_hash, spec_json, version
    - changelog: tracks all mutations with timestamps
    - meta: key-value metadata (lineage, serial, strata_version)
    """

    kind: Literal["sqlite"] = "sqlite"
    path: str

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection."""
        return sqlite3.connect(self.path)

    def _ensure_build_tables(self) -> None:
        """Create build-related tables if they don't exist.

        Called automatically by put_quality_result/put_build_record when
        the registry hasn't been fully initialized via ``up``. This lets
        ``build`` persist its own metadata without requiring ``up`` first.
        """
        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)

        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quality_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    has_warnings INTEGER NOT NULL,
                    rows_checked INTEGER NOT NULL,
                    results_json TEXT NOT NULL,
                    build_id INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS build_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    row_count INTEGER,
                    duration_ms REAL,
                    data_timestamp_max TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        """Create tables if they don't exist.

        Creates:
        - objects table for storing feature definitions
        - changelog table for tracking mutations
        - meta table for registry metadata

        Sets initial metadata:
        - lineage: UUID for this registry instance
        - serial: starts at 0
        - strata_version: current Strata version
        """
        # Ensure parent directory exists
        path = Path(self.path)
        path.parent.mkdir(parents=True, exist_ok=True)

        conn = self._connect()
        try:
            cursor = conn.cursor()

            # Create objects table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    PRIMARY KEY (kind, name)
                )
            """)

            # Create changelog table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS changelog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    old_hash TEXT,
                    new_hash TEXT,
                    applied_by TEXT NOT NULL
                )
            """)

            # Create meta table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            # Create quality_results table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quality_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    has_warnings INTEGER NOT NULL,
                    rows_checked INTEGER NOT NULL,
                    results_json TEXT NOT NULL,
                    build_id INTEGER
                )
            """)

            # Create build_records table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS build_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    row_count INTEGER,
                    duration_ms REAL,
                    data_timestamp_max TEXT
                )
            """)

            # Set initial metadata if not exists
            cursor.execute("SELECT value FROM meta WHERE key = 'lineage'")
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO meta (key, value) VALUES ('lineage', ?)",
                    (str(uuid.uuid4()),),
                )
                cursor.execute(
                    "INSERT INTO meta (key, value) VALUES ('serial', '0')"
                )
                cursor.execute(
                    "INSERT INTO meta (key, value) VALUES ('strata_version', ?)",
                    (_STRATA_VERSION,),
                )

            conn.commit()
        finally:
            conn.close()

    def get_object(self, kind: str, name: str) -> registry.ObjectRecord | None:
        """Fetch a single object by kind and name."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT kind, name, spec_hash, spec_json, version FROM objects WHERE kind = ? AND name = ?",
                (kind, name),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return registry.ObjectRecord(
                kind=row[0],
                name=row[1],
                spec_hash=row[2],
                spec_json=row[3],
                version=row[4],
            )
        finally:
            conn.close()

    def list_objects(
        self, kind: str | None = None
    ) -> list[registry.ObjectRecord]:
        """List all objects, optionally filtered by kind."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if kind is not None:
                cursor.execute(
                    "SELECT kind, name, spec_hash, spec_json, version FROM objects WHERE kind = ?",
                    (kind,),
                )
            else:
                cursor.execute(
                    "SELECT kind, name, spec_hash, spec_json, version FROM objects"
                )
            rows = cursor.fetchall()
            return [
                registry.ObjectRecord(
                    kind=row[0],
                    name=row[1],
                    spec_hash=row[2],
                    spec_json=row[3],
                    version=row[4],
                )
                for row in rows
            ]
        finally:
            conn.close()

    def put_object(self, obj: registry.ObjectRecord, applied_by: str) -> None:
        """Upsert an object and log the change."""
        conn = self._connect()
        try:
            cursor = conn.cursor()

            # Check if exists
            existing = self.get_object(obj.kind, obj.name)
            timestamp = datetime.now(timezone.utc).isoformat()

            if existing is not None:
                # Update existing object
                new_version = existing.version + 1
                cursor.execute(
                    "UPDATE objects SET spec_hash = ?, spec_json = ?, version = ? WHERE kind = ? AND name = ?",
                    (
                        obj.spec_hash,
                        obj.spec_json,
                        new_version,
                        obj.kind,
                        obj.name,
                    ),
                )
                # Log update
                cursor.execute(
                    "INSERT INTO changelog (timestamp, operation, kind, name, old_hash, new_hash, applied_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        timestamp,
                        "update",
                        obj.kind,
                        obj.name,
                        existing.spec_hash,
                        obj.spec_hash,
                        applied_by,
                    ),
                )
            else:
                # Insert new object
                cursor.execute(
                    "INSERT INTO objects (kind, name, spec_hash, spec_json, version) VALUES (?, ?, ?, ?, ?)",
                    (obj.kind, obj.name, obj.spec_hash, obj.spec_json, 1),
                )
                # Log create
                cursor.execute(
                    "INSERT INTO changelog (timestamp, operation, kind, name, old_hash, new_hash, applied_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        timestamp,
                        "create",
                        obj.kind,
                        obj.name,
                        None,
                        obj.spec_hash,
                        applied_by,
                    ),
                )

            # Increment serial
            cursor.execute("SELECT value FROM meta WHERE key = 'serial'")
            row = cursor.fetchone()
            serial = int(row[0]) if row else 0
            cursor.execute(
                "UPDATE meta SET value = ? WHERE key = 'serial'",
                (str(serial + 1),),
            )

            conn.commit()
        finally:
            conn.close()

    def delete_object(self, kind: str, name: str, applied_by: str) -> None:
        """Delete an object and log the change."""
        conn = self._connect()
        try:
            cursor = conn.cursor()

            # Get current object for old_hash
            existing = self.get_object(kind, name)
            if existing is None:
                return  # Nothing to delete

            timestamp = datetime.now(timezone.utc).isoformat()

            # Delete object
            cursor.execute(
                "DELETE FROM objects WHERE kind = ? AND name = ?",
                (kind, name),
            )

            # Log delete
            cursor.execute(
                "INSERT INTO changelog (timestamp, operation, kind, name, old_hash, new_hash, applied_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    "delete",
                    kind,
                    name,
                    existing.spec_hash,
                    None,
                    applied_by,
                ),
            )

            # Increment serial
            cursor.execute("SELECT value FROM meta WHERE key = 'serial'")
            row = cursor.fetchone()
            serial = int(row[0]) if row else 0
            cursor.execute(
                "UPDATE meta SET value = ? WHERE key = 'serial'",
                (str(serial + 1),),
            )

            conn.commit()
        finally:
            conn.close()

    def get_meta(self, key: str) -> str | None:
        """Get a metadata value."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def set_meta(self, key: str, value: str) -> None:
        """Set a metadata value."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

    def get_changelog(self, limit: int = 100) -> list[registry.ChangelogEntry]:
        """Get recent changelog entries."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, timestamp, operation, kind, name, old_hash, new_hash, applied_by FROM changelog ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = cursor.fetchall()
            return [
                registry.ChangelogEntry(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    operation=row[2],
                    kind=row[3],
                    name=row[4],
                    old_hash=row[5],
                    new_hash=row[6],
                    applied_by=row[7],
                )
                for row in rows
            ]
        finally:
            conn.close()

    def put_quality_result(self, result: registry.QualityResultRecord) -> None:
        """Store a quality validation result.

        Auto-creates the quality_results table if it doesn't exist,
        so ``build`` can persist metadata without requiring ``up`` first.
        """
        try:
            self._insert_quality_result(result)
        except sqlite3.OperationalError:
            self._ensure_build_tables()
            self._insert_quality_result(result)

    def _insert_quality_result(
        self, result: registry.QualityResultRecord
    ) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            timestamp = result.timestamp.isoformat()
            cursor.execute(
                "INSERT INTO quality_results (timestamp, table_name, passed, has_warnings, rows_checked, results_json, build_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    result.table_name,
                    int(result.passed),
                    int(result.has_warnings),
                    result.rows_checked,
                    result.results_json,
                    result.build_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_quality_results(
        self, table_name: str, limit: int = 10
    ) -> list[registry.QualityResultRecord]:
        """Get recent quality results for a table."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, timestamp, table_name, passed, has_warnings, rows_checked, results_json, build_id FROM quality_results WHERE table_name = ? ORDER BY timestamp DESC LIMIT ?",
                (table_name, limit),
            )
            rows = cursor.fetchall()
            return [
                registry.QualityResultRecord(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    table_name=row[2],
                    passed=bool(row[3]),
                    has_warnings=bool(row[4]),
                    rows_checked=row[5],
                    results_json=row[6],
                    build_id=row[7],
                )
                for row in rows
            ]
        finally:
            conn.close()

    def put_build_record(self, record: registry.BuildRecord) -> None:
        """Store a build execution record.

        Auto-creates the build_records table if it doesn't exist,
        so ``build`` can persist metadata without requiring ``up`` first.
        """
        try:
            self._insert_build_record(record)
        except sqlite3.OperationalError:
            self._ensure_build_tables()
            self._insert_build_record(record)

    def _insert_build_record(self, record: registry.BuildRecord) -> None:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            timestamp = record.timestamp.isoformat()
            cursor.execute(
                "INSERT INTO build_records (timestamp, table_name, status, row_count, duration_ms, data_timestamp_max) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    timestamp,
                    record.table_name,
                    record.status,
                    record.row_count,
                    record.duration_ms,
                    record.data_timestamp_max,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_latest_build(self, table_name: str) -> registry.BuildRecord | None:
        """Get the most recent build record for a table."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, timestamp, table_name, status, row_count, duration_ms, data_timestamp_max FROM build_records WHERE table_name = ? ORDER BY timestamp DESC LIMIT 1",
                (table_name,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return registry.BuildRecord(
                id=row[0],
                timestamp=datetime.fromisoformat(row[1]),
                table_name=row[2],
                status=row[3],
                row_count=row[4],
                duration_ms=row[5],
                data_timestamp_max=row[6],
            )
        finally:
            conn.close()

    def get_build_records(
        self, table_name: str | None = None, limit: int = 10
    ) -> list[registry.BuildRecord]:
        """Get recent build records, optionally filtered by table."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if table_name is not None:
                cursor.execute(
                    "SELECT id, timestamp, table_name, status, row_count, duration_ms, data_timestamp_max FROM build_records WHERE table_name = ? ORDER BY timestamp DESC LIMIT ?",
                    (table_name, limit),
                )
            else:
                cursor.execute(
                    "SELECT id, timestamp, table_name, status, row_count, duration_ms, data_timestamp_max FROM build_records ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                )
            rows = cursor.fetchall()
            return [
                registry.BuildRecord(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    table_name=row[2],
                    status=row[3],
                    row_count=row[4],
                    duration_ms=row[5],
                    data_timestamp_max=row[6],
                )
                for row in rows
            ]
        finally:
            conn.close()
