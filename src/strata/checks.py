"""Data quality and SLA definitions for feature tables."""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

import strata.core as core


class SLA(core.StrataBaseModel):
    """Table-level operational guarantees for freshness and row counts.

    SLAs monitor operational health. Default severity is 'warn' (informational).
    Override to 'error' to make SLA violations block builds.

    Example:
        FeatureTable(
            ...,
            sla=SLA(max_staleness=timedelta(hours=6), min_row_count=1000),
        )
    """

    max_staleness: timedelta | None = None
    min_row_count: int | None = None
    severity: Literal["warn", "error"] = "warn"
