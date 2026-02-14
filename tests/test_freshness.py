"""Tests for freshness calculation logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import strata.checks as checks
import strata.freshness as freshness_mod
import strata.registry as registry


def _make_table(name: str, sla: checks.SLA | None = None) -> MagicMock:
    """Create a mock FeatureTable with optional SLA."""
    table = MagicMock()
    table.name = name
    table.sla = sla
    return table


def _make_build_record(
    table_name: str,
    timestamp: datetime,
    row_count: int | None = None,
    data_timestamp_max: str | None = None,
) -> registry.BuildRecord:
    """Create a BuildRecord for testing."""
    return registry.BuildRecord(
        id=1,
        timestamp=timestamp,
        table_name=table_name,
        status="success",
        row_count=row_count,
        duration_ms=100.0,
        data_timestamp_max=data_timestamp_max,
    )


class TestFreshnessWithinSLA:
    def test_fresh_when_build_recent(self):
        """Build 1h ago, SLA max_staleness=6h -> status='fresh'."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(max_staleness=timedelta(hours=6)),
        )
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert len(result.tables) == 1
        assert result.tables[0].status == "fresh"
        assert result.tables[0].build_staleness == timedelta(hours=1)
        assert not result.has_stale
        assert not result.has_unknown


class TestFreshnessExceedsSLAWarn:
    def test_warn_when_stale_with_warn_severity(self):
        """Build 8h ago, SLA max_staleness=6h, severity='warn' -> status='warn'."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(max_staleness=timedelta(hours=6), severity="warn"),
        )
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "warn"
        assert result.tables[0].build_staleness == timedelta(hours=8)
        assert result.has_stale


class TestFreshnessExceedsSLAError:
    def test_error_when_stale_with_error_severity(self):
        """Build 8h ago, SLA max_staleness=6h, severity='error' -> status='error'."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(max_staleness=timedelta(hours=6), severity="error"),
        )
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "error"
        assert result.has_stale


class TestFreshnessNoBuildRecord:
    def test_unknown_when_never_built(self):
        """No build record -> status='unknown'."""
        table = _make_table(
            "users",
            sla=checks.SLA(max_staleness=timedelta(hours=6)),
        )

        result = freshness_mod.check_freshness([table], {"users": None})

        assert result.tables[0].status == "unknown"
        assert result.tables[0].last_build_at is None
        assert result.tables[0].build_staleness is None
        assert result.has_unknown


class TestFreshnessNoSLA:
    def test_fresh_when_no_sla_defined(self):
        """Table without SLA -> status='fresh' always."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table("users", sla=None)
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "fresh"
        assert result.tables[0].max_staleness is None
        assert not result.has_stale


class TestFreshnessDataStaleness:
    def test_data_staleness_triggers_when_build_is_recent(self):
        """Build is recent but data_timestamp_max is stale -> status reflects data staleness."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(max_staleness=timedelta(hours=6), severity="warn"),
        )
        # Build ran 1h ago, but data is 10h stale
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            data_timestamp_max="2025-01-01T02:00:00+00:00",
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "warn"
        assert result.tables[0].build_staleness == timedelta(hours=1)
        assert result.tables[0].data_staleness == timedelta(hours=10)
        assert result.has_stale


class TestFreshnessMinRowCount:
    def test_row_count_below_threshold(self):
        """Row count below SLA min_row_count -> status reflects severity."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(min_row_count=1000, severity="warn"),
        )
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            row_count=500,
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "warn"
        assert result.tables[0].row_count == 500
        assert result.tables[0].min_row_count == 1000
        assert result.has_stale

    def test_row_count_above_threshold(self):
        """Row count above SLA min_row_count -> status='fresh'."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        table = _make_table(
            "users",
            sla=checks.SLA(min_row_count=1000, severity="warn"),
        )
        build = _make_build_record(
            "users",
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            row_count=2000,
        )

        result = freshness_mod.check_freshness(
            [table], {"users": build}, now=now
        )

        assert result.tables[0].status == "fresh"
        assert not result.has_stale


class TestFreshnessAggregate:
    def test_multiple_tables_aggregate_flags(self):
        """Aggregate flags reflect worst status across all tables."""
        now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        fresh_table = _make_table(
            "fresh_table",
            sla=checks.SLA(max_staleness=timedelta(hours=6)),
        )
        stale_table = _make_table(
            "stale_table",
            sla=checks.SLA(max_staleness=timedelta(hours=2), severity="error"),
        )
        unknown_table = _make_table("unknown_table", sla=None)

        builds = {
            "fresh_table": _make_build_record(
                "fresh_table",
                timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            ),
            "stale_table": _make_build_record(
                "stale_table",
                timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
            ),
            "unknown_table": None,
        }

        result = freshness_mod.check_freshness(
            [fresh_table, stale_table, unknown_table], builds, now=now
        )

        assert len(result.tables) == 3
        assert result.has_stale
        assert result.has_unknown
        assert result.tables[0].status == "fresh"
        assert result.tables[1].status == "error"
        assert result.tables[2].status == "unknown"
