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

import strata.plugins.base as base
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

            # Set initial metadata if not exists
            cursor.execute("SELECT value FROM meta WHERE key = 'lineage'")
            if cursor.fetchone() is None:
                cursor.execute(
                    "INSERT INTO meta (key, value) VALUES ('lineage', ?)",
                    (str(uuid.uuid4()),),
                )
                cursor.execute("INSERT INTO meta (key, value) VALUES ('serial', '0')")
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

    def list_objects(self, kind: str | None = None) -> list[registry.ObjectRecord]:
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
                    (obj.spec_hash, obj.spec_json, new_version, obj.kind, obj.name),
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
