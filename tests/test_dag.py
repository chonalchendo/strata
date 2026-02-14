"""Tests for DAG resolution and topological sort."""

import pytest

import strata.core as core
import strata.dag as dag
import strata.errors as errors
import strata.sources as sources
import strata.infra.backends.local.storage as local_storage


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def source_config():
    return local_storage.LocalSourceConfig(path="./data/events.parquet")


@pytest.fixture
def batch_source(source_config):
    return sources.BatchSource(
        name="events",
        config=source_config,
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def base_table(user_entity, batch_source):
    return core.FeatureTable(
        name="base_features",
        source=batch_source,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def derived_table(user_entity, base_table):
    return core.FeatureTable(
        name="derived_features",
        source=base_table,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def third_table(user_entity, derived_table):
    return core.FeatureTable(
        name="third_features",
        source=derived_table,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )


class TestDAGEmpty:
    def test_empty_dag_has_zero_length(self):
        d = dag.DAG()
        assert len(d) == 0

    def test_empty_dag_topological_sort_returns_empty(self):
        d = dag.DAG()
        assert d.topological_sort() == []

    def test_empty_dag_contains_returns_false(self):
        d = dag.DAG()
        assert "anything" not in d


class TestDAGSingleTable:
    def test_add_single_table(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        assert len(d) == 1
        assert "base_features" in d

    def test_single_table_topological_sort(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        assert d.topological_sort() == ["base_features"]

    def test_single_table_get_upstream_with_self(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        result = d.get_upstream("base_features")
        assert result == ["base_features"]

    def test_single_table_get_upstream_without_self(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        result = d.get_upstream("base_features", include_self=False)
        assert result == []

    def test_single_table_get_downstream_with_self(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        result = d.get_downstream("base_features")
        assert result == ["base_features"]

    def test_single_table_get_downstream_without_self(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        result = d.get_downstream("base_features", include_self=False)
        assert result == []


class TestDAGLinearChain:
    def test_linear_chain_topological_sort(
        self, base_table, derived_table, third_table
    ):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.topological_sort()
        # base_features must come before derived_features, which must come before third_features
        assert result.index("base_features") < result.index("derived_features")
        assert result.index("derived_features") < result.index("third_features")

    def test_linear_chain_add_order_independent(
        self, base_table, derived_table, third_table
    ):
        """Topological sort is correct regardless of insertion order."""
        d = dag.DAG()
        d.add_tables([third_table, base_table, derived_table])
        result = d.topological_sort()
        assert result.index("base_features") < result.index("derived_features")
        assert result.index("derived_features") < result.index("third_features")

    def test_get_upstream_of_leaf(self, base_table, derived_table, third_table):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.get_upstream("third_features")
        # Should include all ancestors in order
        assert result == ["base_features", "derived_features", "third_features"]

    def test_get_upstream_of_leaf_without_self(
        self, base_table, derived_table, third_table
    ):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.get_upstream("third_features", include_self=False)
        assert result == ["base_features", "derived_features"]

    def test_get_upstream_of_middle(
        self, base_table, derived_table, third_table
    ):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.get_upstream("derived_features")
        assert result == ["base_features", "derived_features"]

    def test_get_downstream_of_root(
        self, base_table, derived_table, third_table
    ):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.get_downstream("base_features")
        assert "base_features" in result
        assert "derived_features" in result
        assert "third_features" in result

    def test_get_downstream_of_root_without_self(
        self, base_table, derived_table, third_table
    ):
        d = dag.DAG()
        d.add_tables([base_table, derived_table, third_table])
        result = d.get_downstream("base_features", include_self=False)
        assert "base_features" not in result
        assert "derived_features" in result
        assert "third_features" in result


class TestDAGDiamond:
    def test_diamond_dependency(self, user_entity, batch_source, base_table):
        """Diamond: A -> B, A -> C, B -> D, C -> D"""
        table_b = core.FeatureTable(
            name="branch_b",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_c = core.FeatureTable(
            name="branch_c",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_d = core.FeatureTable(
            name="diamond_end",
            source=table_b,  # Only one source allowed per table
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([base_table, table_b, table_c, table_d])
        result = d.topological_sort()

        # base_features must be first
        assert result[0] == "base_features"
        # branch_b must come before diamond_end
        assert result.index("branch_b") < result.index("diamond_end")

    def test_diamond_upstream_of_end(
        self, user_entity, batch_source, base_table
    ):
        """Upstream of diamond end includes transitive dependencies."""
        table_b = core.FeatureTable(
            name="branch_b",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_d = core.FeatureTable(
            name="diamond_end",
            source=table_b,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([base_table, table_b, table_d])
        result = d.get_upstream("diamond_end")

        assert result == ["base_features", "branch_b", "diamond_end"]


class TestDAGCycleDetection:
    def test_cycle_raises_error(self, user_entity, batch_source):
        """Create tables that reference each other in a cycle via manual node manipulation."""
        # Build a cycle by creating two tables and manually wiring them
        table_a = core.FeatureTable(
            name="table_a",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_b = core.FeatureTable(
            name="table_b",
            source=table_a,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([table_a, table_b])

        # Manually create a cycle by adding table_a as dependent on table_b
        d._nodes["table_a"].upstream.append("table_b")
        d._nodes["table_b"].downstream.append("table_a")

        with pytest.raises(errors.StrataError, match="Cycle detected"):
            d.topological_sort()

    def test_cycle_error_lists_involved_tables(self, user_entity, batch_source):
        """Cycle error message includes the tables involved."""
        table_a = core.FeatureTable(
            name="alpha",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_b = core.FeatureTable(
            name="beta",
            source=table_a,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([table_a, table_b])

        # Create cycle
        d._nodes["alpha"].upstream.append("beta")
        d._nodes["beta"].downstream.append("alpha")

        with pytest.raises(errors.StrataError, match="alpha") as exc_info:
            d.topological_sort()

        assert "beta" in str(exc_info.value)
        assert "Remove circular dependencies" in str(exc_info.value)


class TestDAGTableNotFound:
    def test_get_upstream_unknown_table_raises_error(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)

        with pytest.raises(errors.StrataError, match="Table not found in DAG"):
            d.get_upstream("nonexistent")

    def test_get_downstream_unknown_table_raises_error(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)

        with pytest.raises(errors.StrataError, match="Table not found in DAG"):
            d.get_downstream("nonexistent")

    def test_get_table_unknown_raises_error(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)

        with pytest.raises(errors.StrataError, match="Table not found in DAG"):
            d.get_table("nonexistent")

    def test_error_includes_table_name_in_context(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)

        with pytest.raises(errors.StrataError) as exc_info:
            d.get_upstream("missing_table")

        assert "missing_table" in str(exc_info.value)


class TestDAGGetTable:
    def test_get_table_returns_feature_table(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        result = d.get_table("base_features")
        assert result is base_table

    def test_get_table_returns_correct_table(self, base_table, derived_table):
        d = dag.DAG()
        d.add_tables([base_table, derived_table])

        assert d.get_table("base_features") is base_table
        assert d.get_table("derived_features") is derived_table


class TestDAGContainsAndLen:
    def test_contains_after_add(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        assert "base_features" in d
        assert "other" not in d

    def test_len_increases_with_additions(self, base_table, derived_table):
        d = dag.DAG()
        assert len(d) == 0
        d.add_table(base_table)
        assert len(d) == 1
        d.add_table(derived_table)
        assert len(d) == 2


class TestDAGNodeProperties:
    def test_node_tracks_upstream(self, base_table, derived_table):
        d = dag.DAG()
        d.add_tables([base_table, derived_table])
        node = d.nodes["derived_features"]
        assert node.upstream == ["base_features"]

    def test_node_tracks_downstream(self, base_table, derived_table):
        d = dag.DAG()
        d.add_tables([base_table, derived_table])
        node = d.nodes["base_features"]
        assert "derived_features" in node.downstream

    def test_root_node_has_no_upstream(self, base_table):
        d = dag.DAG()
        d.add_table(base_table)
        node = d.nodes["base_features"]
        assert node.upstream == []

    def test_leaf_node_has_no_downstream(self, derived_table, base_table):
        d = dag.DAG()
        d.add_tables([base_table, derived_table])
        node = d.nodes["derived_features"]
        assert node.downstream == []


class TestDAGMultipleRoots:
    def test_multiple_independent_roots(self, user_entity, batch_source):
        """DAG with multiple independent root tables."""
        table_a = core.FeatureTable(
            name="table_a",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_b = core.FeatureTable(
            name="table_b",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([table_a, table_b])
        result = d.topological_sort()
        assert len(result) == 2
        assert set(result) == {"table_a", "table_b"}

    def test_multiple_roots_converging(self, user_entity, batch_source):
        """Two independent roots with a derived table depending on one."""
        table_a = core.FeatureTable(
            name="table_a",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_b = core.FeatureTable(
            name="table_b",
            source=batch_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table_c = core.FeatureTable(
            name="table_c",
            source=table_a,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        d = dag.DAG()
        d.add_tables([table_a, table_b, table_c])
        result = d.topological_sort()
        assert result.index("table_a") < result.index("table_c")
        assert len(result) == 3
