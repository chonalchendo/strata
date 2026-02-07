"""Performance benchmark tests for strata up workflow.

These tests measure end-to-end performance of the discovery -> diff -> apply
pipeline. They provide baseline data for optimization decisions.

Design note: Per performance decisions, no caching is implemented initially.
This benchmark validates that the uncached approach is fast enough (~500ms
for 100 definitions). If benchmarks show otherwise, caching can be added
with the extensible DefinitionDiscoverer design.
"""

import time
from pathlib import Path

import pytest

import strata.diff as diff
import strata.discovery as discovery
import strata.registry as reg_types
import strata.settings as settings


def create_mock_project(
    tmp_path: Path, num_entities: int = 10, num_tables: int = 40
) -> Path:
    """Create a mock project with many definition files.

    Args:
        tmp_path: Temporary directory for project
        num_entities: Number of entity files to create
        num_tables: Number of feature table files to create

    Returns:
        Project root path

    Note: To avoid duplicate entity definitions with different specs,
    tables import entities from a shared module rather than redefining them.
    """
    # Create strata.yaml
    config = tmp_path / "strata.yaml"
    config.write_text(
        """
name: benchmark-project
default_env: dev
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    storage:
      kind: local
      path: .strata/data
      catalog: features
    compute:
      kind: duckdb
"""
    )

    # Create entities directory with many entities
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()

    for i in range(num_entities):
        entity_file = entities_dir / f"entity_{i:03d}.py"
        entity_file.write_text(
            f"""
import strata.core as core

entity_{i:03d} = core.Entity(
    name="entity_{i:03d}",
    join_keys=["id_{i:03d}"],
    description="Benchmark entity {i}",
)
"""
        )

    # Create tables directory with many feature tables
    # Tables reference entities by name but DON'T redefine them inline
    # to avoid duplicate entity definitions with different specs
    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()

    for i in range(num_tables):
        # Each table references an entity (cycling through available entities)
        entity_idx = i % num_entities
        table_file = tables_dir / f"table_{i:03d}.py"
        # Use a stub entity reference that matches the entity definition
        # In real projects, entities would be imported from a shared module
        table_file.write_text(
            f"""
import strata.core as core
import strata.sources as sources
from strata.backends.local.storage import LocalSourceConfig

# Reference entity with matching spec (same as entities/entity_{entity_idx:03d}.py)
entity_{entity_idx:03d} = core.Entity(
    name="entity_{entity_idx:03d}",
    join_keys=["id_{entity_idx:03d}"],
    description="Benchmark entity {entity_idx}",
)

source_{i:03d} = sources.BatchSource(
    name="source_{i:03d}",
    config=LocalSourceConfig(path="./data_{i:03d}.parquet"),
    timestamp_field="event_ts",
)

table_{i:03d} = core.FeatureTable(
    name="table_{i:03d}",
    source=source_{i:03d},
    entity=entity_{entity_idx:03d},
    timestamp_field="event_ts",
    description="Benchmark feature table {i}",
)
"""
        )

    return tmp_path


class FakeRegistry:
    """Minimal fake registry for benchmark tests."""

    def __init__(self):
        self._objects: dict[tuple[str, str], reg_types.ObjectRecord] = {}

    def initialize(self) -> None:
        pass

    def list_objects(self, kind: str | None = None) -> list[reg_types.ObjectRecord]:
        if kind is None:
            return list(self._objects.values())
        return [o for o in self._objects.values() if o.kind == kind]

    def get_object(self, kind: str, name: str) -> reg_types.ObjectRecord | None:
        return self._objects.get((kind, name))

    def put_object(self, obj: reg_types.ObjectRecord, applied_by: str) -> None:
        self._objects[(obj.kind, obj.name)] = obj

    def delete_object(self, kind: str, name: str, applied_by: str) -> None:
        self._objects.pop((kind, name), None)


class TestDiscoveryBenchmark:
    """Benchmark discovery phase performance."""

    @pytest.mark.benchmark
    def test_discovery_50_files(self, tmp_path, monkeypatch):
        """Discover definitions from ~50 mock files."""
        project_root = create_mock_project(tmp_path, num_entities=10, num_tables=40)
        monkeypatch.chdir(project_root)

        strata_settings = settings.load_strata_settings()

        # Time discovery
        t0 = time.perf_counter()
        discoverer = discovery.DefinitionDiscoverer(strata_settings, project_root)
        discovered = discoverer.discover_all()
        t_discovery = time.perf_counter() - t0

        # Verify we found all definitions
        # Note: entities are duplicated (once in entities/, once in tables/)
        entities = [d for d in discovered if d.kind == "entity"]
        tables = [d for d in discovered if d.kind == "feature_table"]

        assert len(tables) == 40, f"Expected 40 tables, got {len(tables)}"
        assert len(entities) >= 10, (
            f"Expected at least 10 entities, got {len(entities)}"
        )

        # Print timing for visibility
        print(f"\nDiscovery: {t_discovery * 1000:.1f}ms for {len(discovered)} objects")

        # Soft assertion: discovery should be fast
        # This is a sanity check, not a hard requirement
        assert t_discovery < 5.0, f"Discovery took {t_discovery:.2f}s, expected < 5s"


class TestDiffBenchmark:
    """Benchmark diff phase performance."""

    @pytest.mark.benchmark
    def test_diff_50_creates(self, tmp_path, monkeypatch):
        """Compute diff for ~50 new definitions."""
        project_root = create_mock_project(tmp_path, num_entities=10, num_tables=40)
        monkeypatch.chdir(project_root)

        strata_settings = settings.load_strata_settings()

        # Discover first
        discovered = discovery.discover_definitions(strata_settings)

        # Empty registry (all creates)
        reg = FakeRegistry()

        # Time diff
        t0 = time.perf_counter()
        result = diff.compute_diff(discovered, reg)
        t_diff = time.perf_counter() - t0

        # Diff produces a change for EACH discovered object, including duplicates.
        # This is intentional - duplicates with different specs would be detected
        # as conflicts. The apply phase handles deduplication.
        assert len(result.creates) == len(discovered)
        assert len(result.updates) == 0
        assert len(result.deletes) == 0

        print(
            f"\nDiff (all creates): {t_diff * 1000:.1f}ms for {len(result.changes)} changes"
        )

        assert t_diff < 1.0, f"Diff took {t_diff:.2f}s, expected < 1s"

    @pytest.mark.benchmark
    def test_diff_no_changes(self, tmp_path, monkeypatch):
        """Compute diff when registry matches definitions (no changes)."""
        project_root = create_mock_project(tmp_path, num_entities=10, num_tables=40)
        monkeypatch.chdir(project_root)

        strata_settings = settings.load_strata_settings()

        # Discover
        discovered = discovery.discover_definitions(strata_settings)

        # Pre-populate registry with matching objects (deduplicated by kind+name)
        reg = FakeRegistry()
        seen = set()
        for disc in discovered:
            key = (disc.kind, disc.name)
            if key in seen:
                continue  # Skip duplicates
            seen.add(key)

            spec = discovery.serialize_to_spec(disc.obj, disc.kind)
            spec_json = discovery.spec_to_json(spec)
            spec_hash = reg_types.compute_spec_hash(spec_json)
            obj = reg_types.ObjectRecord(
                kind=disc.kind,
                name=disc.name,
                spec_hash=spec_hash,
                spec_json=spec_json,
                version=1,
            )
            reg.put_object(obj, applied_by="test")

        # Time diff
        t0 = time.perf_counter()
        result = diff.compute_diff(discovered, reg)
        t_diff = time.perf_counter() - t0

        # All should be unchanged
        assert len(result.creates) == 0
        assert len(result.updates) == 0
        assert len(result.deletes) == 0
        assert not result.has_changes

        print(
            f"\nDiff (no changes): {t_diff * 1000:.1f}ms for {len(result.changes)} comparisons"
        )

        assert t_diff < 1.0, f"Diff took {t_diff:.2f}s, expected < 1s"


class TestEndToEndBenchmark:
    """Benchmark complete strata up workflow."""

    @pytest.mark.benchmark
    def test_full_workflow_50_definitions(self, tmp_path, monkeypatch):
        """Measure end-to-end discovery -> diff -> apply time."""
        project_root = create_mock_project(tmp_path, num_entities=10, num_tables=40)
        monkeypatch.chdir(project_root)

        strata_settings = settings.load_strata_settings()
        reg = FakeRegistry()

        # Total workflow timing
        t_total_start = time.perf_counter()

        # Discovery
        t0 = time.perf_counter()
        discovered = discovery.discover_definitions(strata_settings)
        t_discovery = time.perf_counter() - t0

        # Diff
        t0 = time.perf_counter()
        result = diff.compute_diff(discovered, reg)
        t_diff = time.perf_counter() - t0

        # Apply (simulate)
        t0 = time.perf_counter()
        for change in result.changes:
            if change.operation == diff.ChangeOperation.CREATE:
                obj = reg_types.ObjectRecord(
                    kind=change.kind,
                    name=change.name,
                    spec_hash=change.new_hash,
                    spec_json=change.spec_json,
                    version=1,
                )
                reg.put_object(obj, applied_by="benchmark")
        t_apply = time.perf_counter() - t0

        t_total = time.perf_counter() - t_total_start

        # Print detailed timing
        print("\n=== Benchmark Results ===")
        print(f"Discovery: {t_discovery * 1000:.1f}ms ({len(discovered)} objects)")
        print(f"Diff:      {t_diff * 1000:.1f}ms ({len(result.changes)} changes)")
        print(f"Apply:     {t_apply * 1000:.1f}ms ({len(result.creates)} creates)")
        print(f"Total:     {t_total * 1000:.1f}ms")
        print("=========================")

        # Sanity check: total should be reasonable
        # Note: This is intentionally generous. Real optimization targets
        # should be set based on actual user feedback, not arbitrary limits.
        assert t_total < 10.0, f"Total workflow took {t_total:.2f}s, expected < 10s"

        # Verify correctness: registry should have unique objects
        # 10 unique entities + 40 unique tables = 50 objects
        assert len(reg.list_objects()) == 50
