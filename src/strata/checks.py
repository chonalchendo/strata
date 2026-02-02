"""Data quality and SLA definitions for feature tables."""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

import strata.core as core


class SLA(core.StrataBaseModel):
    """Table-level operational guarantees.

    SLAs define freshness expectations, row count bounds, and late data handling
    for feature tables.
    """

    # Freshness
    freshness_expected: timedelta  # Normal latency
    freshness_max: timedelta  # Alert threshold

    # Row count
    min_row_count: int | None = None
    max_row_count: int | None = None

    # Late data
    late_data_max: timedelta | None = None  # Max lateness before action
    late_data_action: Literal["accept", "quarantine", "discard"] = "accept"

    # Alerting
    owner: str | None = None
    slack_channel: str | None = None
    oncall: str | None = None

    # Enforcement
    on_violation: Literal["warn", "fail"] = "warn"
