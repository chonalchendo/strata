"""Diff engine for comparing desired vs current state.

Computes changes needed to sync Python definitions to registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

import strata.discovery as discovery
import strata.registry as registry

if TYPE_CHECKING:
    import strata.infra.backends.base as base


class ChangeOperation(str, Enum):
    """Type of change operation."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    UNCHANGED = "unchanged"


@dataclass
class Change:
    """Represents a single change in the diff.

    Attributes:
        operation: Type of change (create, update, delete, unchanged)
        kind: Object kind (entity, feature_table, etc.)
        name: Object name
        old_hash: Previous spec hash (None for create)
        new_hash: New spec hash (None for delete)
        spec_json: New spec JSON (None for delete)
        source_file: Where the definition came from (None for delete)
    """

    operation: ChangeOperation
    kind: str
    name: str
    old_hash: str | None = None
    new_hash: str | None = None
    spec_json: str | None = None
    source_file: str | None = None


@dataclass
class DiffResult:
    """Result of computing a diff.

    Contains all changes and summary statistics.
    """

    changes: list[Change]

    @property
    def creates(self) -> list[Change]:
        return [
            c for c in self.changes if c.operation == ChangeOperation.CREATE
        ]

    @property
    def updates(self) -> list[Change]:
        return [
            c for c in self.changes if c.operation == ChangeOperation.UPDATE
        ]

    @property
    def deletes(self) -> list[Change]:
        return [
            c for c in self.changes if c.operation == ChangeOperation.DELETE
        ]

    @property
    def unchanged(self) -> list[Change]:
        return [
            c for c in self.changes if c.operation == ChangeOperation.UNCHANGED
        ]

    @property
    def has_changes(self) -> bool:
        """True if there are any creates, updates, or deletes."""
        return bool(self.creates or self.updates or self.deletes)

    def summary(self) -> str:
        """Return summary string like '3 created, 1 updated, 0 deleted'."""
        parts = []
        if self.creates:
            parts.append(f"{len(self.creates)} created")
        if self.updates:
            parts.append(f"{len(self.updates)} updated")
        if self.deletes:
            parts.append(f"{len(self.deletes)} deleted")
        if self.unchanged:
            parts.append(f"{len(self.unchanged)} unchanged")
        return ", ".join(parts) if parts else "No changes"


def compute_diff(
    discovered: list[discovery.DiscoveredObject],
    reg: "base.BaseRegistry",
) -> DiffResult:
    """Compute diff between discovered definitions and registry state.

    Args:
        discovered: List of discovered definitions from Python files
        reg: Registry backend to compare against

    Returns:
        DiffResult with all changes
    """
    changes: list[Change] = []

    # Build map of current state: (kind, name) -> ObjectRecord
    current_objects = reg.list_objects()
    current_map: dict[tuple[str, str], registry.ObjectRecord] = {
        (obj.kind, obj.name): obj for obj in current_objects
    }

    # Track which objects we've seen (to detect deletes)
    seen_keys: set[tuple[str, str]] = set()

    # Process each discovered object
    for disc in discovered:
        key = (disc.kind, disc.name)
        seen_keys.add(key)

        # Serialize to canonical JSON
        spec = discovery.serialize_to_spec(disc.obj, disc.kind)
        spec_json = discovery.spec_to_json(spec)
        new_hash = registry.compute_spec_hash(spec_json)

        current = current_map.get(key)

        if current is None:
            # New object - CREATE
            changes.append(
                Change(
                    operation=ChangeOperation.CREATE,
                    kind=disc.kind,
                    name=disc.name,
                    old_hash=None,
                    new_hash=new_hash,
                    spec_json=spec_json,
                    source_file=disc.source_file,
                )
            )
        elif current.spec_hash != new_hash:
            # Existing object with different hash - UPDATE
            changes.append(
                Change(
                    operation=ChangeOperation.UPDATE,
                    kind=disc.kind,
                    name=disc.name,
                    old_hash=current.spec_hash,
                    new_hash=new_hash,
                    spec_json=spec_json,
                    source_file=disc.source_file,
                )
            )
        else:
            # Same hash - UNCHANGED
            changes.append(
                Change(
                    operation=ChangeOperation.UNCHANGED,
                    kind=disc.kind,
                    name=disc.name,
                    old_hash=current.spec_hash,
                    new_hash=new_hash,
                    spec_json=spec_json,
                    source_file=disc.source_file,
                )
            )

    # Find deletes: objects in registry but not in discovered
    for key, obj in current_map.items():
        if key not in seen_keys:
            changes.append(
                Change(
                    operation=ChangeOperation.DELETE,
                    kind=obj.kind,
                    name=obj.name,
                    old_hash=obj.spec_hash,
                    new_hash=None,
                    spec_json=None,
                    source_file=None,
                )
            )

    # Sort changes for deterministic output: by kind, then name
    changes.sort(key=lambda c: (c.kind, c.name))

    return DiffResult(changes=changes)
