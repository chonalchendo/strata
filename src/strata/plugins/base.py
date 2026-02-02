"""Base classes for Strata plugins.

Plugins provide backend implementations for registry, storage, and compute.
Each environment in strata.yaml specifies which plugin to use for each role.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pydantic as pdt


@dataclass
class RegistryState:
    """In-memory state representation."""

    version: int  # Monotonic version number
    created_by: str  # user@hostname
    created_at: datetime


@dataclass
class SnapshotMeta:
    """State versioning metadata."""

    lineage: str  # UUID linking related states
    serial: int  # Monotonic version number
    created_by: str  # user@hostname
    created_at: datetime


@dataclass
class LockInfo:
    """Lock metadata for diagnostics."""

    id: str  # UUID for this lock
    operation: str  # plan, apply, destroy
    who: str  # user@hostname
    created: datetime


class BaseRegistry(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Registry backend interface.

    Registries store feature metadata and handle state management.
    Concrete implementations (SQLite, DuckDB, etc.) provide the actual storage.

    Methods are defined here with NotImplementedError to allow configuration
    loading without requiring full implementation. This enables incremental
    development across phases.
    """

    def state(self) -> RegistryState:
        """Return current in-memory state (deep copy)."""
        raise NotImplementedError("Registry.state() not implemented")

    def write_state(self, state: RegistryState) -> None:
        """Update in-memory state (no persistence)."""
        raise NotImplementedError("Registry.write_state() not implemented")

    def refresh_state(self) -> None:
        """Load state from persistent storage."""
        raise NotImplementedError("Registry.refresh_state() not implemented")

    def persist_state(self) -> SnapshotMeta:
        """Write state to persistent storage, return new version."""
        raise NotImplementedError("Registry.persist_state() not implemented")

    def lock(self, info: LockInfo) -> str:
        """Acquire lock, return lock ID."""
        raise NotImplementedError("Registry.lock() not implemented")

    def unlock(self, lock_id: str) -> None:
        """Release lock."""
        raise NotImplementedError("Registry.unlock() not implemented")


class BaseStorage(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Storage backend interface.

    Storage backends handle reading and writing feature data (Delta tables).
    """

    pass


class BaseCompute(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Compute backend interface.

    Compute backends execute feature transformations and queries.
    """

    pass


class BaseSourceConfig(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Base class for source connection configurations.

    Source configs define how to connect to a data source.
    Each backend (DuckDB, S3, Unity Catalog) provides its own config class.
    """
    pass
