"""Registry data types for object storage and change tracking.

This module provides the data structures used by registry implementations
to store and track feature definitions (entities, feature tables, datasets).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ObjectRecord:
    """A registered object (entity, feature table, dataset, etc.).

    Objects are stored with content-based versioning via spec_hash.
    The version field is monotonically incremented on each update.
    """

    kind: str  # "entity", "feature_table", "dataset", "source_table"
    name: str  # unique within kind
    spec_hash: str  # SHA256 of canonical JSON spec
    spec_json: str  # JSON serialization of spec
    version: int  # monotonic, incremented on each update


@dataclass(frozen=True)
class ChangelogEntry:
    """Record of a registry mutation.

    All create, update, and delete operations are logged with timestamps
    and the identity of who applied the change.
    """

    id: int
    timestamp: datetime
    operation: str  # "create", "update", "delete"
    kind: str
    name: str
    old_hash: str | None  # None for create
    new_hash: str | None  # None for delete
    applied_by: str  # user@hostname


@dataclass(frozen=True)
class QualityResultRecord:
    """Persisted quality validation result for a table build.

    Stores the outcome of running field-level constraints on materialized data.
    Results are serialized as JSON for flexible constraint detail storage.
    """

    id: int | None  # Auto-assigned by DB
    timestamp: datetime
    table_name: str
    passed: bool
    has_warnings: bool
    rows_checked: int
    results_json: str  # JSON serialization of field results
    build_id: int | None = None  # Reference to build record


@dataclass(frozen=True)
class BuildRecord:
    """Record of a table build execution.

    Tracks build metadata including timing, row counts, and the maximum
    data timestamp for freshness calculations.
    """

    id: int | None  # Auto-assigned by DB
    timestamp: datetime
    table_name: str
    status: str  # "success", "failed", "skipped"
    row_count: int | None = None
    duration_ms: float | None = None
    data_timestamp_max: str | None = (
        None  # Max value of timestamp_field in built data
    )


def compute_spec_hash(spec_json: str) -> str:
    """Compute SHA256 hash of canonical JSON spec.

    Args:
        spec_json: JSON string representation of the object spec.

    Returns:
        Hexadecimal SHA256 hash of the spec.
    """
    return hashlib.sha256(spec_json.encode()).hexdigest()
