"""Local storage plugin for file-based sources."""

from __future__ import annotations

from typing import Literal

import strata.infra.backends.base as base


class LocalSourceConfig(base.BaseSourceConfig):
    """Local file source configuration.

    Example:
        from strata.infra.backends.local.storage import LocalSourceConfig

        config = LocalSourceConfig(
            path="./data/events.parquet",
            format="parquet",
        )
    """

    path: str
    format: Literal["parquet", "csv", "json", "delta"] = "parquet"
