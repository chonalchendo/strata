"""Tests for the build execution engine."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pydantic as pdt
import pytest

import strata.build as build_mod
import strata.core as core
import strata.sources as sources
from strata.infra.backends.local.storage import LocalSourceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_mock_compiler():
    """Create a mock IbisCompiler that returns a mock compiled result.

    Avoids the decimal.ConversionSyntax issue in ibis.interval() on
    Python 3.14+ by bypassing the real compiler entirely.
    """
    mock_compiled = MagicMock()
    mock_compiled.ibis_expr = MagicMock()
    mock_compiler = MagicMock()
    mock_compiler.compile_table.return_value = mock_compiled
    return mock_compiler


@pytest.fixture(autouse=True)
def _patch_compiler():
    """Auto-patch IbisCompiler for all build tests.

    The build engine tests exercise orchestration logic (DAG ordering,
    write modes, failure cascading) -- not the compiler itself. Mocking
    the compiler avoids the decimal.ConversionSyntax crash that
    ibis.interval() triggers on Python 3.14+.
    """
    with patch(
        "strata.compiler.IbisCompiler",
        return_value=_make_mock_compiler(),
    ):
        yield


@pytest.fixture
def user_entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture
def transaction_source():
    return sources.BatchSource(
        name="transactions",
        config=LocalSourceConfig(path="./data/transactions.parquet"),
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def single_table(user_entity, transaction_source):
    """A single FeatureTable with an aggregate."""
    table = core.FeatureTable(
        name="user_transactions",
        source=transaction_source,
        entity=user_entity,
        timestamp_field="event_timestamp",
    )
    table.aggregate(
        name="spend_90d",
        field=core.Field(dtype="float64"),
        column="amount",
        function="sum",
        window=timedelta(days=90),
    )
    return table


@pytest.fixture
def mock_backend():
    """A mock backend that satisfies BackendKind contract."""
    backend = MagicMock()
    backend.kind = "duckdb"

    # connect() returns a mock connection
    mock_conn = MagicMock()
    backend.connect.return_value = mock_conn

    # execute() returns a PyArrow table
    mock_data = pa.table({"user_id": ["u1", "u2"], "spend_90d": [100.0, 200.0]})
    backend.execute.return_value = mock_data

    return backend


@pytest.fixture
def build_engine(mock_backend):
    """BuildEngine with a mocked backend.

    Uses patch to bypass BackendKind discriminator validation since
    our mock is not a real DuckDBBackend instance.
    """
    engine = build_mod.BuildEngine.model_construct(backend=mock_backend)
    return engine


# ---------------------------------------------------------------------------
# BuildEngine pydantic model
# ---------------------------------------------------------------------------


class TestBuildEngineModel:
    def test_is_pydantic_model(self):
        """BuildEngine must be a pydantic BaseModel."""
        assert issubclass(build_mod.BuildEngine, pdt.BaseModel)

    def test_model_config_strict(self):
        """BuildEngine must use strict=True."""
        config = build_mod.BuildEngine.model_config
        assert config.get("strict") is True

    def test_model_config_frozen(self):
        """BuildEngine must use frozen=True."""
        config = build_mod.BuildEngine.model_config
        assert config.get("frozen") is True

    def test_model_config_extra_forbid(self):
        """BuildEngine must use extra='forbid'."""
        config = build_mod.BuildEngine.model_config
        assert config.get("extra") == "forbid"

    def test_backend_field_required(self):
        """BuildEngine requires a backend field."""
        fields = build_mod.BuildEngine.model_fields
        assert "backend" in fields


# ---------------------------------------------------------------------------
# BuildStatus enum
# ---------------------------------------------------------------------------


class TestBuildStatus:
    def test_status_values(self):
        assert build_mod.BuildStatus.SUCCESS.value == "success"
        assert build_mod.BuildStatus.FAILED.value == "failed"
        assert build_mod.BuildStatus.SKIPPED.value == "skipped"


# ---------------------------------------------------------------------------
# BuildResult
# ---------------------------------------------------------------------------


class TestBuildResult:
    def test_empty_result_is_success(self):
        result = build_mod.BuildResult()
        assert result.is_success is True
        assert result.success_count == 0
        assert result.failed_count == 0
        assert result.skipped_count == 0

    def test_counters(self):
        result = build_mod.BuildResult(
            table_results=[
                build_mod.TableBuildResult(
                    table_name="a", status=build_mod.BuildStatus.SUCCESS
                ),
                build_mod.TableBuildResult(
                    table_name="b",
                    status=build_mod.BuildStatus.FAILED,
                    error="boom",
                ),
                build_mod.TableBuildResult(
                    table_name="c", status=build_mod.BuildStatus.SKIPPED
                ),
            ]
        )
        assert result.success_count == 1
        assert result.failed_count == 1
        assert result.skipped_count == 1
        assert result.is_success is False

    def test_all_success_is_true(self):
        result = build_mod.BuildResult(
            table_results=[
                build_mod.TableBuildResult(
                    table_name="a", status=build_mod.BuildStatus.SUCCESS
                ),
                build_mod.TableBuildResult(
                    table_name="b", status=build_mod.BuildStatus.SUCCESS
                ),
            ]
        )
        assert result.is_success is True


# ---------------------------------------------------------------------------
# Single table build
# ---------------------------------------------------------------------------


class TestSingleTableBuild:
    def test_success(self, build_engine, mock_backend, single_table):
        """A single table should compile, execute, and write successfully."""
        result = build_engine.build(tables=[single_table])

        assert result.success_count == 1
        assert result.failed_count == 0
        assert result.is_success is True

        # Backend methods should be called
        mock_backend.connect.assert_called_once()
        mock_backend.register_source.assert_called_once()
        mock_backend.execute.assert_called_once()
        mock_backend.write_table.assert_called_once()

    def test_write_table_called_with_table_name(
        self, build_engine, mock_backend, single_table
    ):
        """write_table should be called with the table's name."""
        build_engine.build(tables=[single_table])

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["table_name"] == "user_transactions"

    def test_row_count_tracked(self, build_engine, single_table):
        """TableBuildResult should track row count."""
        result = build_engine.build(tables=[single_table])

        table_result = result.table_results[0]
        assert table_result.row_count == 2  # 2 rows in mock data
        assert table_result.duration_ms is not None
        assert table_result.duration_ms >= 0


# ---------------------------------------------------------------------------
# Default write_mode (append)
# ---------------------------------------------------------------------------


class TestDefaultWriteMode:
    def test_append_is_default(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Default write_mode 'append' should be passed to write_table."""
        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        build_engine.build(tables=[table])

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "append"
        assert write_call.kwargs["merge_keys"] is None


# ---------------------------------------------------------------------------
# Merge write_mode
# ---------------------------------------------------------------------------


class TestMergeWriteMode:
    def test_merge_uses_effective_merge_keys(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Merge write_mode should use effective_merge_keys (entity join keys)."""
        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
            write_mode="merge",
        )
        table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        build_engine.build(tables=[table])

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "merge"
        assert write_call.kwargs["merge_keys"] == ["user_id"]

    def test_merge_with_custom_keys(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Custom merge_keys should override entity join keys."""
        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
            write_mode="merge",
            merge_keys=["user_id", "event_date"],
        )
        table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        build_engine.build(tables=[table])

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["merge_keys"] == ["user_id", "event_date"]


# ---------------------------------------------------------------------------
# DAG order execution
# ---------------------------------------------------------------------------


class TestDAGExecution:
    def test_topological_order(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Tables should be built in DAG order (dependencies first)."""
        base_table = core.FeatureTable(
            name="base_features",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        base_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        derived_table = core.FeatureTable(
            name="derived_features",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived_table.aggregate(
            name="risk_score",
            field=core.Field(dtype="float64"),
            column="spend_90d",
            function="avg",
            window=timedelta(days=30),
        )

        result = build_engine.build(tables=[derived_table, base_table])

        assert result.success_count == 2
        # Verify execution order: base first, derived second
        assert result.table_results[0].table_name == "base_features"
        assert result.table_results[1].table_name == "derived_features"

    def test_target_builds_with_deps(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Targeting a table should also build its upstream dependencies."""
        base_table = core.FeatureTable(
            name="base_features",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        base_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        derived_table = core.FeatureTable(
            name="derived_features",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived_table.aggregate(
            name="risk_score",
            field=core.Field(dtype="float64"),
            column="spend_90d",
            function="avg",
            window=timedelta(days=30),
        )

        # Target only the derived table -- base should still build
        result = build_engine.build(
            tables=[derived_table, base_table],
            targets=["derived_features"],
        )

        assert result.success_count == 2
        table_names = [r.table_name for r in result.table_results]
        assert "base_features" in table_names
        assert "derived_features" in table_names


# ---------------------------------------------------------------------------
# Upstream failure cascading
# ---------------------------------------------------------------------------


class TestUpstreamFailureCascade:
    def test_downstream_skipped_on_upstream_failure(
        self, mock_backend, user_entity, transaction_source
    ):
        """When upstream fails, downstream tables should be SKIPPED."""
        base_table = core.FeatureTable(
            name="base_features",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        base_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        derived_table = core.FeatureTable(
            name="derived_features",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived_table.aggregate(
            name="risk_score",
            field=core.Field(dtype="float64"),
            column="spend_90d",
            function="avg",
            window=timedelta(days=30),
        )

        # Make execute fail for the base table
        mock_backend.execute.side_effect = RuntimeError("Database error")
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        result = engine.build(tables=[base_table, derived_table])

        assert result.failed_count == 1
        assert result.skipped_count == 1
        assert result.success_count == 0

        base_result = result.table_results[0]
        derived_result = result.table_results[1]

        assert base_result.table_name == "base_features"
        assert base_result.status == build_mod.BuildStatus.FAILED

        assert derived_result.table_name == "derived_features"
        assert derived_result.status == build_mod.BuildStatus.SKIPPED
        assert derived_result.error is not None
        assert "base_features" in derived_result.error

    def test_independent_tables_not_affected(
        self, mock_backend, user_entity, transaction_source
    ):
        """Independent tables should not be affected by sibling failures."""
        failing_table = core.FeatureTable(
            name="failing_table",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        independent_table = core.FeatureTable(
            name="independent_table",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        # Make execute fail only for the first table
        call_count = 0
        mock_data = pa.table({"user_id": ["u1"], "value": [1.0]})

        def side_effect_execute(conn, expr):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First table fails")
            return mock_data

        mock_backend.execute.side_effect = side_effect_execute
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        result = engine.build(tables=[failing_table, independent_table])

        assert result.failed_count == 1
        assert result.success_count == 1


# ---------------------------------------------------------------------------
# full_refresh
# ---------------------------------------------------------------------------


class TestFullRefresh:
    def test_drops_table_before_rebuild(
        self, build_engine, mock_backend, single_table
    ):
        """full_refresh should drop the table and write with overwrite mode."""
        build_engine.build(tables=[single_table], full_refresh=True)

        # drop_table should be called
        mock_backend.drop_table.assert_called_once_with("user_transactions")

        # write_table should use overwrite mode
        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "overwrite"

    def test_overrides_write_mode(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """full_refresh should override merge write_mode to overwrite."""
        table = core.FeatureTable(
            name="merge_table",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
            write_mode="merge",
        )
        table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        build_engine.build(tables=[table], full_refresh=True)

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "overwrite"
        mock_backend.drop_table.assert_called_once_with("merge_table")


# ---------------------------------------------------------------------------
# Start/end date range (backfill)
# ---------------------------------------------------------------------------


class TestDateRangeBackfill:
    def test_deletes_range_before_write(
        self, build_engine, mock_backend, single_table
    ):
        """start/end should delete existing data in range then append."""
        start_dt = datetime(2024, 1, 1)
        end_dt = datetime(2024, 1, 31)

        build_engine.build(
            tables=[single_table],
            start=start_dt,
            end=end_dt,
        )

        # delete_range should be called
        mock_backend.delete_range.assert_called_once_with(
            table_name="user_transactions",
            partition_col="event_timestamp",
            start=start_dt.isoformat(),
            end=end_dt.isoformat(),
        )

        # write_table should use append mode (not overwrite)
        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "append"

    def test_full_refresh_overrides_date_range(
        self, build_engine, mock_backend, single_table
    ):
        """full_refresh should take priority over start/end."""
        start_dt = datetime(2024, 1, 1)
        end_dt = datetime(2024, 1, 31)

        build_engine.build(
            tables=[single_table],
            full_refresh=True,
            start=start_dt,
            end=end_dt,
        )

        # full_refresh wins: drop_table called, delete_range NOT called
        mock_backend.drop_table.assert_called_once()
        mock_backend.delete_range.assert_not_called()

        write_call = mock_backend.write_table.call_args
        assert write_call.kwargs["mode"] == "overwrite"


# ---------------------------------------------------------------------------
# Source registration
# ---------------------------------------------------------------------------


class TestSourceRegistration:
    def test_external_source_registered(
        self, build_engine, mock_backend, single_table
    ):
        """External BatchSource should be registered with the backend."""
        build_engine.build(tables=[single_table])

        mock_backend.register_source.assert_called_once()
        reg_call = mock_backend.register_source.call_args
        assert reg_call.kwargs["name"] == "transactions"

    def test_derived_table_skips_registration(
        self, build_engine, mock_backend, user_entity, transaction_source
    ):
        """Derived tables (source=FeatureTable) should not call register_source."""
        base_table = core.FeatureTable(
            name="base_features",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived_table = core.FeatureTable(
            name="derived_features",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        build_engine.build(tables=[base_table, derived_table])

        # register_source should be called once (for base), not for derived
        register_calls = mock_backend.register_source.call_args_list
        registered_names = [c.kwargs["name"] for c in register_calls]
        assert "transactions" in registered_names
        assert len(registered_names) == 1


# ---------------------------------------------------------------------------
# Build failure handling
# ---------------------------------------------------------------------------


class TestBuildFailureHandling:
    def test_execute_failure_returns_failed_status(
        self, mock_backend, user_entity, transaction_source
    ):
        """When execute raises, the table should be marked as FAILED."""
        mock_backend.execute.side_effect = RuntimeError("Query timeout")
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="failing_table",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        result = engine.build(tables=[table])

        assert result.failed_count == 1
        assert result.table_results[0].status == build_mod.BuildStatus.FAILED
        assert result.table_results[0].error is not None
        assert "Query timeout" in result.table_results[0].error

    def test_write_failure_returns_failed_status(
        self, mock_backend, user_entity, transaction_source
    ):
        """When write_table raises, the table should be marked as FAILED."""
        mock_backend.write_table.side_effect = OSError("Disk full")
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="failing_table",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        result = engine.build(tables=[table])

        assert result.failed_count == 1
        assert result.table_results[0].error is not None
        assert "Disk full" in result.table_results[0].error


# ---------------------------------------------------------------------------
# Quality validation helpers
# ---------------------------------------------------------------------------


def _make_validation_result(
    *,
    table_name="test_table",
    passed=True,
    has_warnings=False,
    field_results=None,
):
    """Create a mock TableValidationResult."""
    import strata.quality as quality

    if field_results is None:
        field_results = []
    return quality.TableValidationResult(
        table_name=table_name,
        field_results=field_results,
        rows_checked=10,
        passed=passed,
        has_warnings=has_warnings,
    )


def _make_failing_validation(table_name="test_table"):
    """Create a validation result with an error-severity failure."""
    import strata.quality as quality

    cr = quality.ConstraintResult(
        field_name="amount",
        constraint="ge",
        passed=False,
        severity="error",
        expected=">= 0",
        actual="min=-5.0",
        rows_checked=10,
        rows_failed=2,
    )
    fr = quality.FieldResult(
        field_name="amount",
        constraints=[cr],
        passed=False,
    )
    return quality.TableValidationResult(
        table_name=table_name,
        field_results=[fr],
        rows_checked=10,
        passed=False,
        has_warnings=False,
    )


def _make_warning_validation(table_name="test_table"):
    """Create a validation result that passes but has warnings."""
    import strata.quality as quality

    cr = quality.ConstraintResult(
        field_name="score",
        constraint="le",
        passed=False,
        severity="warn",
        expected="<= 100",
        actual="max=150",
        rows_checked=10,
        rows_failed=1,
    )
    fr = quality.FieldResult(
        field_name="score",
        constraints=[cr],
        passed=True,  # warn-severity doesn't fail the field
    )
    return quality.TableValidationResult(
        table_name=table_name,
        field_results=[fr],
        rows_checked=10,
        passed=True,
        has_warnings=True,
    )


# ---------------------------------------------------------------------------
# Build + validation integration
# ---------------------------------------------------------------------------


class TestBuildWithValidation:
    """Tests for validate-before-write integration in BuildEngine.

    The autouse _patch_compiler fixture handles the IbisCompiler mock.
    These tests additionally mock quality.validate_table to control
    validation outcomes.
    """

    def test_build_with_validation_passing(
        self, mock_backend, user_entity, transaction_source
    ):
        """Data passes validation, table is written successfully."""
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        passing_result = _make_validation_result(
            table_name="user_transactions", passed=True
        )

        with patch(
            "strata.quality.validate_table", return_value=passing_result
        ):
            result = engine.build(tables=[table])

        assert result.success_count == 1
        assert result.failed_count == 0
        assert result.is_success is True

        table_result = result.table_results[0]
        assert table_result.validation_passed is True
        assert table_result.validation_warnings == 0

        # Data should have been written
        mock_backend.write_table.assert_called_once()

    def test_build_with_validation_failing(
        self, mock_backend, user_entity, transaction_source
    ):
        """Data fails validation, table is NOT written, status is FAILED."""
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        failing_result = _make_failing_validation(
            table_name="user_transactions"
        )

        with patch(
            "strata.quality.validate_table", return_value=failing_result
        ):
            result = engine.build(tables=[table])

        assert result.failed_count == 1
        assert result.success_count == 0
        assert result.is_success is False

        table_result = result.table_results[0]
        assert table_result.status == build_mod.BuildStatus.FAILED
        assert table_result.validation_passed is False
        assert "quality check failed" in table_result.error
        assert "--skip-quality" in table_result.error

        # Data should NOT have been written
        mock_backend.write_table.assert_not_called()

    def test_build_skip_quality(
        self, mock_backend, user_entity, transaction_source
    ):
        """skip_quality=True bypasses validation entirely."""
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        with patch("strata.quality.validate_table") as mock_validate:
            result = engine.build(tables=[table], skip_quality=True)

        # validate_table should NOT have been called
        mock_validate.assert_not_called()

        assert result.success_count == 1
        assert result.is_success is True

        table_result = result.table_results[0]
        assert table_result.validation_passed is None  # Skipped
        assert table_result.validation_warnings == 0

        # Data should have been written
        mock_backend.write_table.assert_called_once()

    def test_build_validation_failure_skips_downstream(
        self, mock_backend, user_entity, transaction_source
    ):
        """When upstream validation fails, downstream tables are SKIPPED."""
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        base_table = core.FeatureTable(
            name="base_features",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        base_table.aggregate(
            name="spend_90d",
            field=core.Field(dtype="float64"),
            column="amount",
            function="sum",
            window=timedelta(days=90),
        )

        derived_table = core.FeatureTable(
            name="derived_features",
            source=base_table,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )
        derived_table.aggregate(
            name="risk_score",
            field=core.Field(dtype="float64"),
            column="spend_90d",
            function="avg",
            window=timedelta(days=30),
        )

        failing_result = _make_failing_validation(table_name="base_features")

        with patch(
            "strata.quality.validate_table", return_value=failing_result
        ):
            result = engine.build(tables=[base_table, derived_table])

        assert result.failed_count == 1
        assert result.skipped_count == 1
        assert result.success_count == 0

        base_result = result.table_results[0]
        assert base_result.table_name == "base_features"
        assert base_result.status == build_mod.BuildStatus.FAILED
        assert base_result.validation_passed is False

        derived_result = result.table_results[1]
        assert derived_result.table_name == "derived_features"
        assert derived_result.status == build_mod.BuildStatus.SKIPPED
        assert "base_features" in derived_result.error

    def test_build_validation_warnings(
        self, mock_backend, user_entity, transaction_source
    ):
        """Warn-severity failures don't block write, but are counted."""
        engine = build_mod.BuildEngine.model_construct(backend=mock_backend)

        table = core.FeatureTable(
            name="user_transactions",
            source=transaction_source,
            entity=user_entity,
            timestamp_field="event_timestamp",
        )

        warning_result = _make_warning_validation(
            table_name="user_transactions"
        )

        with patch(
            "strata.quality.validate_table", return_value=warning_result
        ):
            result = engine.build(tables=[table])

        assert result.success_count == 1
        assert result.is_success is True

        table_result = result.table_results[0]
        assert table_result.validation_passed is True
        assert table_result.validation_warnings == 1

        # Data should have been written (warnings don't block)
        mock_backend.write_table.assert_called_once()

        # BuildResult-level aggregation
        assert result.validation_count == 1
        assert result.validation_warning_count == 1


# ---------------------------------------------------------------------------
# BuildResult validation properties
# ---------------------------------------------------------------------------


class TestBuildResultValidation:
    def test_validation_count_with_mix(self):
        """validation_count counts only validated tables."""
        result = build_mod.BuildResult(
            table_results=[
                build_mod.TableBuildResult(
                    table_name="a",
                    status=build_mod.BuildStatus.SUCCESS,
                    validation_passed=True,
                ),
                build_mod.TableBuildResult(
                    table_name="b",
                    status=build_mod.BuildStatus.SUCCESS,
                    validation_passed=None,  # Skipped
                ),
                build_mod.TableBuildResult(
                    table_name="c",
                    status=build_mod.BuildStatus.FAILED,
                    validation_passed=False,
                ),
            ]
        )
        assert result.validation_count == 2  # a and c were validated

    def test_validation_warning_count(self):
        """validation_warning_count sums warnings across all tables."""
        result = build_mod.BuildResult(
            table_results=[
                build_mod.TableBuildResult(
                    table_name="a",
                    status=build_mod.BuildStatus.SUCCESS,
                    validation_warnings=2,
                ),
                build_mod.TableBuildResult(
                    table_name="b",
                    status=build_mod.BuildStatus.SUCCESS,
                    validation_warnings=1,
                ),
            ]
        )
        assert result.validation_warning_count == 3
