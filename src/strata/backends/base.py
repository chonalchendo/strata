"""Base classes for Strata plugins.

Plugins provide backend implementations for registry, storage, and compute.
Each environment in strata.yaml specifies which plugin to use for each role.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pydantic as pdt

if TYPE_CHECKING:
    import strata.registry as registry


class BaseRegistry(pdt.BaseModel, strict=True, frozen=True, extra="forbid"):
    """Registry backend interface.

    Registries store feature metadata and handle state management.
    Concrete implementations (SQLite, DuckDB, etc.) provide the actual storage.

    Methods are defined here with NotImplementedError to allow configuration
    loading without requiring full implementation. This enables incremental
    development across phases.
    """

    def initialize(self) -> None:
        """Create tables if they don't exist.

        Must be called before any other operations. Idempotent.
        """
        raise NotImplementedError("Registry.initialize() not implemented")

    def get_object(self, kind: str, name: str) -> "registry.ObjectRecord | None":
        """Fetch a single object by kind and name.

        Args:
            kind: Object type ("entity", "feature_table", "dataset", "source_table").
            name: Unique name within the kind.

        Returns:
            ObjectRecord if found, None otherwise.
        """
        raise NotImplementedError("Registry.get_object() not implemented")

    def list_objects(self, kind: str | None = None) -> "list[registry.ObjectRecord]":
        """List all objects, optionally filtered by kind.

        Args:
            kind: If provided, filter to only this object type.

        Returns:
            List of ObjectRecord instances.
        """
        raise NotImplementedError("Registry.list_objects() not implemented")

    def put_object(self, obj: "registry.ObjectRecord", applied_by: str) -> None:
        """Upsert an object and log the change.

        If object exists (same kind/name), increments version and logs "update".
        If object is new, sets version=1 and logs "create".

        Args:
            obj: The object record to store.
            applied_by: Identity string (user@hostname) for changelog.
        """
        raise NotImplementedError("Registry.put_object() not implemented")

    def delete_object(self, kind: str, name: str, applied_by: str) -> None:
        """Delete an object and log the change.

        Args:
            kind: Object type.
            name: Object name.
            applied_by: Identity string (user@hostname) for changelog.
        """
        raise NotImplementedError("Registry.delete_object() not implemented")

    def get_meta(self, key: str) -> str | None:
        """Get a metadata value.

        Args:
            key: Metadata key.

        Returns:
            Value if found, None otherwise.
        """
        raise NotImplementedError("Registry.get_meta() not implemented")

    def set_meta(self, key: str, value: str) -> None:
        """Set a metadata value.

        Args:
            key: Metadata key.
            value: Metadata value.
        """
        raise NotImplementedError("Registry.set_meta() not implemented")

    def get_changelog(self, limit: int = 100) -> "list[registry.ChangelogEntry]":
        """Get recent changelog entries.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ChangelogEntry instances, newest first.
        """
        raise NotImplementedError("Registry.get_changelog() not implemented")


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
