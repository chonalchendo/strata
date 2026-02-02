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


def compute_spec_hash(spec_json: str) -> str:
    """Compute SHA256 hash of canonical JSON spec.

    Args:
        spec_json: JSON string representation of the object spec.

    Returns:
        Hexadecimal SHA256 hash of the spec.
    """
    return hashlib.sha256(spec_json.encode()).hexdigest()
