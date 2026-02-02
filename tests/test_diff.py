"""Tests for diff engine."""

import strata.core as core
import strata.diff as diff
import strata.discovery as discovery
import strata.registry as registry


class FakeRegistry:
    """In-memory fake registry for testing."""

    def __init__(self, objects: list[registry.ObjectRecord] | None = None):
        self._objects = {(o.kind, o.name): o for o in (objects or [])}

    def list_objects(self, kind: str | None = None) -> list[registry.ObjectRecord]:
        if kind is None:
            return list(self._objects.values())
        return [o for o in self._objects.values() if o.kind == kind]

    def get_object(self, kind: str, name: str) -> registry.ObjectRecord | None:
        return self._objects.get((kind, name))


class TestComputeDiff:
    """Test compute_diff function."""

    def test_empty_registry_all_creates(self):
        """When registry is empty, all discovered objects are creates."""
        entity = core.Entity(name="user", join_keys=["user_id"])
        discovered = [
            discovery.DiscoveredObject(
                kind="entity",
                name="user",
                obj=entity,
                source_file="entities/user.py",
            )
        ]

        reg = FakeRegistry()
        result = diff.compute_diff(discovered, reg)

        assert len(result.creates) == 1
        assert len(result.updates) == 0
        assert len(result.deletes) == 0
        assert result.creates[0].kind == "entity"
        assert result.creates[0].name == "user"

    def test_same_hash_unchanged(self):
        """When spec hash matches, object is unchanged."""
        entity = core.Entity(name="user", join_keys=["user_id"])
        spec = discovery.serialize_to_spec(entity, "entity")
        spec_json = discovery.spec_to_json(spec)
        spec_hash = registry.compute_spec_hash(spec_json)

        discovered = [
            discovery.DiscoveredObject(
                kind="entity",
                name="user",
                obj=entity,
                source_file="entities/user.py",
            )
        ]

        current = registry.ObjectRecord(
            kind="entity",
            name="user",
            spec_hash=spec_hash,
            spec_json=spec_json,
            version=1,
        )
        reg = FakeRegistry([current])
        result = diff.compute_diff(discovered, reg)

        assert len(result.creates) == 0
        assert len(result.updates) == 0
        assert len(result.deletes) == 0
        assert len(result.unchanged) == 1

    def test_different_hash_update(self):
        """When spec hash differs, object is updated."""
        # Original entity
        entity_v1 = core.Entity(name="user", join_keys=["user_id"])
        spec_v1 = discovery.serialize_to_spec(entity_v1, "entity")
        spec_json_v1 = discovery.spec_to_json(spec_v1)
        hash_v1 = registry.compute_spec_hash(spec_json_v1)

        # Updated entity (added description)
        entity_v2 = core.Entity(
            name="user",
            join_keys=["user_id"],
            description="Updated description",
        )

        discovered = [
            discovery.DiscoveredObject(
                kind="entity",
                name="user",
                obj=entity_v2,
                source_file="entities/user.py",
            )
        ]

        current = registry.ObjectRecord(
            kind="entity",
            name="user",
            spec_hash=hash_v1,
            spec_json=spec_json_v1,
            version=1,
        )
        reg = FakeRegistry([current])
        result = diff.compute_diff(discovered, reg)

        assert len(result.creates) == 0
        assert len(result.updates) == 1
        assert len(result.deletes) == 0
        assert result.updates[0].old_hash == hash_v1
        assert result.updates[0].new_hash != hash_v1

    def test_missing_in_discovered_delete(self):
        """Objects in registry but not discovered are deletes."""
        current = registry.ObjectRecord(
            kind="entity",
            name="old_entity",
            spec_hash="abc123",
            spec_json="{}",
            version=1,
        )
        reg = FakeRegistry([current])

        # No discovered objects
        result = diff.compute_diff([], reg)

        assert len(result.creates) == 0
        assert len(result.updates) == 0
        assert len(result.deletes) == 1
        assert result.deletes[0].name == "old_entity"

    def test_mixed_operations(self):
        """Test with creates, updates, deletes, and unchanged."""
        # Current state: entity_a (v1), entity_b (same)
        entity_a = core.Entity(name="entity_a", join_keys=["id"])
        _spec_a = discovery.serialize_to_spec(entity_a, "entity")

        entity_b = core.Entity(name="entity_b", join_keys=["id"])
        spec_b = discovery.serialize_to_spec(entity_b, "entity")
        json_b = discovery.spec_to_json(spec_b)
        hash_b = registry.compute_spec_hash(json_b)

        current = [
            registry.ObjectRecord(
                kind="entity",
                name="entity_a",
                spec_hash="old_hash",  # Different hash
                spec_json="{}",
                version=1,
            ),
            registry.ObjectRecord(
                kind="entity",
                name="entity_b",
                spec_hash=hash_b,  # Same hash
                spec_json=json_b,
                version=1,
            ),
            registry.ObjectRecord(
                kind="entity",
                name="entity_deleted",
                spec_hash="xyz",
                spec_json="{}",
                version=1,
            ),
        ]
        reg = FakeRegistry(current)

        # Discovered: entity_a (updated), entity_b (same), entity_c (new)
        entity_c = core.Entity(name="entity_c", join_keys=["id"])
        discovered = [
            discovery.DiscoveredObject(
                kind="entity", name="entity_a", obj=entity_a, source_file="a.py"
            ),
            discovery.DiscoveredObject(
                kind="entity", name="entity_b", obj=entity_b, source_file="b.py"
            ),
            discovery.DiscoveredObject(
                kind="entity", name="entity_c", obj=entity_c, source_file="c.py"
            ),
        ]

        result = diff.compute_diff(discovered, reg)

        assert len(result.creates) == 1  # entity_c
        assert len(result.updates) == 1  # entity_a
        assert len(result.deletes) == 1  # entity_deleted
        assert len(result.unchanged) == 1  # entity_b
        assert result.has_changes is True


class TestDiffResult:
    """Test DiffResult helper methods."""

    def test_summary_no_changes(self):
        result = diff.DiffResult(changes=[])
        assert result.summary() == "No changes"

    def test_summary_with_changes(self):
        changes = [
            diff.Change(
                operation=diff.ChangeOperation.CREATE,
                kind="entity",
                name="e1",
            ),
            diff.Change(
                operation=diff.ChangeOperation.CREATE,
                kind="entity",
                name="e2",
            ),
            diff.Change(
                operation=diff.ChangeOperation.UPDATE,
                kind="entity",
                name="e3",
            ),
        ]
        result = diff.DiffResult(changes=changes)

        summary = result.summary()
        assert "2 created" in summary
        assert "1 updated" in summary

    def test_has_changes_false_when_only_unchanged(self):
        changes = [
            diff.Change(
                operation=diff.ChangeOperation.UNCHANGED,
                kind="entity",
                name="e1",
            ),
        ]
        result = diff.DiffResult(changes=changes)
        assert result.has_changes is False
