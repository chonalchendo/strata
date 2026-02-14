"""Tests for SLA model in strata.checks."""

from datetime import timedelta

import pydantic as pdt
import pytest

import strata.checks as checks
import strata.core as core
import strata.sources as sources
from strata.infra.backends.local import LocalSourceConfig


@pytest.fixture()
def entity():
    return core.Entity(name="user", join_keys=["user_id"])


@pytest.fixture()
def batch_source():
    cfg = LocalSourceConfig(path="/tmp/test.parquet")
    return sources.BatchSource(
        name="test_source", config=cfg, timestamp_field="ts"
    )


class TestSLADefaults:
    def test_sla_defaults(self):
        sla = checks.SLA()
        assert sla.max_staleness is None
        assert sla.min_row_count is None
        assert sla.severity == "warn"

    def test_sla_max_staleness(self):
        sla = checks.SLA(max_staleness=timedelta(hours=6))
        assert sla.max_staleness == timedelta(hours=6)
        assert sla.min_row_count is None

    def test_sla_min_row_count(self):
        sla = checks.SLA(min_row_count=1000)
        assert sla.min_row_count == 1000
        assert sla.max_staleness is None

    def test_sla_both_parameters(self):
        sla = checks.SLA(
            max_staleness=timedelta(hours=12),
            min_row_count=500,
        )
        assert sla.max_staleness == timedelta(hours=12)
        assert sla.min_row_count == 500


class TestSLASeverity:
    def test_sla_severity_default_warn(self):
        sla = checks.SLA()
        assert sla.severity == "warn"

    def test_sla_severity_error(self):
        sla = checks.SLA(severity="error")
        assert sla.severity == "error"

    def test_sla_severity_invalid(self):
        with pytest.raises(pdt.ValidationError):
            checks.SLA(severity="critical")


class TestSLAOnFeatureTable:
    def test_feature_table_with_sla(self, entity, batch_source):
        sla = checks.SLA(
            max_staleness=timedelta(hours=6),
            min_row_count=1000,
        )
        ft = core.FeatureTable(
            name="user_features",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
            sla=sla,
        )
        assert ft.sla is not None
        assert ft.sla.max_staleness == timedelta(hours=6)
        assert ft.sla.min_row_count == 1000
        assert ft.sla.severity == "warn"

    def test_feature_table_sla_none_by_default(self, entity, batch_source):
        ft = core.FeatureTable(
            name="user_features",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
        )
        assert ft.sla is None

    def test_feature_table_sla_error_severity(self, entity, batch_source):
        sla = checks.SLA(
            max_staleness=timedelta(hours=1),
            severity="error",
        )
        ft = core.FeatureTable(
            name="critical_features",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
            sla=sla,
        )
        assert ft.sla.severity == "error"


class TestSamplePct:
    def test_sample_pct_default_none(self, entity, batch_source):
        ft = core.FeatureTable(
            name="test",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
        )
        assert ft.sample_pct is None

    def test_sample_pct_valid(self, entity, batch_source):
        ft = core.FeatureTable(
            name="test",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
            sample_pct=10,
        )
        assert ft.sample_pct == 10

    def test_sample_pct_boundary_low(self, entity, batch_source):
        ft = core.FeatureTable(
            name="test",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
            sample_pct=1,
        )
        assert ft.sample_pct == 1

    def test_sample_pct_boundary_high(self, entity, batch_source):
        ft = core.FeatureTable(
            name="test",
            source=batch_source,
            entity=entity,
            timestamp_field="ts",
            sample_pct=100,
        )
        assert ft.sample_pct == 100

    def test_sample_pct_below_range(self, entity, batch_source):
        with pytest.raises(
            Exception, match="sample_pct must be between 1 and 100"
        ):
            core.FeatureTable(
                name="test",
                source=batch_source,
                entity=entity,
                timestamp_field="ts",
                sample_pct=0,
            )

    def test_sample_pct_above_range(self, entity, batch_source):
        with pytest.raises(
            Exception, match="sample_pct must be between 1 and 100"
        ):
            core.FeatureTable(
                name="test",
                source=batch_source,
                entity=entity,
                timestamp_field="ts",
                sample_pct=101,
            )
