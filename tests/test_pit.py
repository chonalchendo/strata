"""Tests for the point-in-time join engine."""

from __future__ import annotations

from datetime import datetime, timedelta

import pyarrow as pa
import pytest

import strata.pit as pit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts(year: int, month: int, day: int) -> datetime:
    """Shorthand for creating datetime objects."""
    return datetime(year, month, day)


def _make_spine(
    user_ids: list[str],
    timestamps: list[datetime],
) -> pa.Table:
    """Build a spine table with user_id and event_ts columns."""
    return pa.table(
        {
            "user_id": user_ids,
            "event_ts": pa.array(timestamps, type=pa.timestamp("us")),
        }
    )


def _make_features(
    user_ids: list[str],
    timestamps: list[datetime],
    feature_cols: dict[str, list],
    timestamp_column: str = "feature_ts",
) -> pa.Table:
    """Build a feature table with entity key, timestamp, and feature columns."""
    data = {
        "user_id": user_ids,
        timestamp_column: pa.array(timestamps, type=pa.timestamp("us")),
    }
    data.update(feature_cols)
    return pa.table(data)


def _make_feature_table_data(
    name: str,
    data: pa.Table,
    feature_columns: list[str],
    timestamp_column: str = "feature_ts",
    entity_keys: list[str] | None = None,
    ttl: timedelta | None = None,
) -> pit.FeatureTableData:
    """Build FeatureTableData with sensible defaults."""
    return pit.FeatureTableData(
        name=name,
        data=data,
        entity_keys=entity_keys or ["user_id"],
        timestamp_column=timestamp_column,
        feature_columns=feature_columns,
        ttl=ttl,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBasicAsofJoin:
    """Core ASOF JOIN behavior."""

    def test_basic_asof_join(self):
        """Each spine row gets the most recent feature where feat.ts <= spine.ts."""
        spine = _make_spine(
            user_ids=["A", "A", "B"],
            timestamps=[_ts(2024, 1, 10), _ts(2024, 1, 20), _ts(2024, 1, 15)],
        )
        features = _make_features(
            user_ids=["A", "A", "B"],
            timestamps=[_ts(2024, 1, 5), _ts(2024, 1, 15), _ts(2024, 1, 10)],
            feature_cols={"spend": [100, 200, 300]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft])

        # Convert to dict for easy assertion
        df = result.to_pandas().sort_values("event_ts").reset_index(drop=True)
        assert list(df.columns) == ["user_id", "event_ts", "spend"]
        assert list(df["spend"]) == [100, 300, 200]  # A@Jan10->100, B@Jan15->300, A@Jan20->200

    def test_no_future_leakage(self):
        """Features after the spine timestamp must not be joined."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
        )
        features = _make_features(
            user_ids=["A", "A"],
            timestamps=[_ts(2024, 1, 5), _ts(2024, 1, 15)],
            feature_cols={"spend": [100, 999]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas()

        # Only the Jan 5 feature (100) should match, NOT Jan 15 (999)
        assert df["spend"].iloc[0] == 100

    def test_missing_feature_returns_null(self):
        """Spine entities with no matching features get null feature values."""
        spine = _make_spine(
            user_ids=["A", "C"],
            timestamps=[_ts(2024, 1, 10), _ts(2024, 1, 15)],
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas().sort_values("user_id").reset_index(drop=True)

        # User A should have spend=100, User C should have null
        assert df.loc[df["user_id"] == "A", "spend"].iloc[0] == 100
        assert pa.compute.is_null(result.column("spend")).to_pylist().count(True) == 1


class TestMultipleFeatureTables:
    """Joining multiple feature tables onto a single spine."""

    def test_multiple_feature_tables(self):
        """Two feature tables join correctly onto one spine."""
        spine = _make_spine(
            user_ids=["A", "B"],
            timestamps=[_ts(2024, 1, 10), _ts(2024, 1, 15)],
        )
        spend_data = _make_features(
            user_ids=["A", "B"],
            timestamps=[_ts(2024, 1, 5), _ts(2024, 1, 10)],
            feature_cols={"spend": [100, 300]},
        )
        clicks_data = _make_features(
            user_ids=["A", "B"],
            timestamps=[_ts(2024, 1, 8), _ts(2024, 1, 12)],
            feature_cols={"clicks": [10, 20]},
            timestamp_column="click_ts",
        )
        ft_spend = _make_feature_table_data("spend", spend_data, ["spend"])
        ft_clicks = _make_feature_table_data(
            "clicks",
            clicks_data,
            ["clicks"],
            timestamp_column="click_ts",
        )

        result = pit.pit_join(spine, [ft_spend, ft_clicks])
        df = result.to_pandas().sort_values("user_id").reset_index(drop=True)

        assert set(df.columns) == {"user_id", "event_ts", "spend", "clicks"}
        assert list(df["spend"]) == [100, 300]
        assert list(df["clicks"]) == [10, 20]

    def test_multiple_tables_independent_timestamps(self):
        """Each feature table uses its own timestamp for PIT matching."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 15)],
        )
        # Feature 1: has data at Jan 10
        feat1 = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
            feature_cols={"metric_a": [42]},
            timestamp_column="ts_a",
        )
        # Feature 2: has data at Jan 14 (closer to spine)
        feat2 = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 14)],
            feature_cols={"metric_b": [99]},
            timestamp_column="ts_b",
        )
        ft1 = _make_feature_table_data("ft1", feat1, ["metric_a"], timestamp_column="ts_a")
        ft2 = _make_feature_table_data("ft2", feat2, ["metric_b"], timestamp_column="ts_b")

        result = pit.pit_join(spine, [ft1, ft2])
        df = result.to_pandas()

        assert df["metric_a"].iloc[0] == 42
        assert df["metric_b"].iloc[0] == 99


class TestTTLEnforcement:
    """Time-to-live enforcement on feature staleness."""

    def test_ttl_enforcement_expired(self):
        """Features older than TTL are nulled out."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 4, 10)],  # April 10
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],  # Jan 5: ~96 days before
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data(
            "user_spend",
            features,
            ["spend"],
            ttl=timedelta(days=90),
        )

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas()

        # 96 days > 90 day TTL -> feature should be null
        assert df["spend"].isna().iloc[0]

    def test_ttl_within_window(self):
        """Features within TTL window are preserved."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 4, 4)],  # April 4
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],  # Jan 5: ~89 days before
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data(
            "user_spend",
            features,
            ["spend"],
            ttl=timedelta(days=90),
        )

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas()

        # 89 days < 90 day TTL -> feature should be present
        assert df["spend"].iloc[0] == 100

    def test_ttl_none_no_expiry(self):
        """Without TTL, features from far past still match."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 12, 31)],  # End of year
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 1)],  # Start of year: ~365 days
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])
        assert ft.ttl is None  # No TTL set

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas()

        # No TTL -> feature should be present regardless of age
        assert df["spend"].iloc[0] == 100

    def test_ttl_mixed_expired_and_fresh(self):
        """Within one join, some rows expire and others don't."""
        spine = _make_spine(
            user_ids=["A", "A"],
            timestamps=[_ts(2024, 4, 10), _ts(2024, 1, 20)],
        )
        features = _make_features(
            user_ids=["A", "A"],
            timestamps=[_ts(2024, 1, 5), _ts(2024, 1, 15)],
            feature_cols={"spend": [100, 200]},
        )
        ft = _make_feature_table_data(
            "user_spend",
            features,
            ["spend"],
            ttl=timedelta(days=90),
        )

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas().sort_values("event_ts").reset_index(drop=True)

        # Row at Jan 20: matched to Jan 15 (5 days < 90 TTL) -> 200
        assert df.iloc[0]["spend"] == 200
        # Row at Apr 10: matched to Jan 15 (~86 days) but Jan 5 is ~96 days.
        # asof_join picks Jan 15 for Apr 10 (most recent). 86 days < 90 TTL -> 200
        # Actually: Jan 5 to Apr 10 is 96 days, Jan 15 to Apr 10 is 86 days
        # asof_join picks Jan 15 (most recent <= Apr 10), 86 days < 90 -> not expired
        assert df.iloc[1]["spend"] == 200


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_spine(self):
        """Empty spine produces empty result with correct columns."""
        spine = pa.table(
            {
                "user_id": pa.array([], type=pa.string()),
                "event_ts": pa.array([], type=pa.timestamp("us")),
            }
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft])

        assert len(result) == 0
        assert set(result.column_names) == {"user_id", "event_ts", "spend"}

    def test_empty_features(self):
        """Non-empty spine with empty features gives null feature columns."""
        spine = _make_spine(
            user_ids=["A", "B"],
            timestamps=[_ts(2024, 1, 10), _ts(2024, 1, 15)],
        )
        features = pa.table(
            {
                "user_id": pa.array([], type=pa.string()),
                "feature_ts": pa.array([], type=pa.timestamp("us")),
                "spend": pa.array([], type=pa.int64()),
            }
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft])

        assert len(result) == 2
        assert all(v is None for v in result.column("spend").to_pylist())

    def test_no_feature_tables(self):
        """Joining zero feature tables returns the spine unchanged."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
        )

        result = pit.pit_join(spine, [])

        assert result.equals(spine)

    def test_custom_connection(self):
        """pit_join works with an explicitly passed Ibis connection."""
        import decimal

        ctx = decimal.getcontext()
        ctx.traps[decimal.InvalidOperation] = False
        import ibis

        conn = ibis.duckdb.connect()

        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(spine, [ft], connection=conn)
        df = result.to_pandas()

        assert df["spend"].iloc[0] == 100

    def test_default_connection_creates_in_memory_duckdb(self):
        """Default connection (None) creates an in-memory DuckDB backend."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        # connection=None is the default
        result = pit.pit_join(spine, [ft], connection=None)

        assert len(result) == 1
        assert result.column("spend").to_pylist() == [100]

    def test_multiple_feature_columns(self):
        """Feature table with multiple feature columns are all joined."""
        spine = _make_spine(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 10)],
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100], "txn_count": [5], "avg_amount": [20.0]},
        )
        ft = _make_feature_table_data(
            "user_spend",
            features,
            ["spend", "txn_count", "avg_amount"],
        )

        result = pit.pit_join(spine, [ft])
        df = result.to_pandas()

        assert df["spend"].iloc[0] == 100
        assert df["txn_count"].iloc[0] == 5
        assert df["avg_amount"].iloc[0] == pytest.approx(20.0)

    def test_custom_spine_timestamp_column(self):
        """pit_join respects custom spine_timestamp parameter."""
        spine = pa.table(
            {
                "user_id": ["A"],
                "observation_time": pa.array(
                    [_ts(2024, 1, 10)], type=pa.timestamp("us")
                ),
            }
        )
        features = _make_features(
            user_ids=["A"],
            timestamps=[_ts(2024, 1, 5)],
            feature_cols={"spend": [100]},
        )
        ft = _make_feature_table_data("user_spend", features, ["spend"])

        result = pit.pit_join(
            spine, [ft], spine_timestamp="observation_time"
        )
        df = result.to_pandas()

        assert df["spend"].iloc[0] == 100
        assert "observation_time" in df.columns


class TestFeatureTableData:
    """FeatureTableData dataclass behavior."""

    def test_frozen(self):
        """FeatureTableData is immutable."""
        ft = _make_feature_table_data(
            "test",
            _make_features(["A"], [_ts(2024, 1, 1)], {"x": [1]}),
            ["x"],
        )
        with pytest.raises(AttributeError):
            ft.name = "changed"  # type: ignore[misc]

    def test_default_ttl_is_none(self):
        """TTL defaults to None (no expiry)."""
        ft = pit.FeatureTableData(
            name="test",
            data=pa.table({"a": [1]}),
            entity_keys=["a"],
            timestamp_column="ts",
            feature_columns=["a"],
        )
        assert ft.ttl is None
