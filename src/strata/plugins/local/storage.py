"""Local storage plugin for file-based sources."""

from __future__ import annotations

from typing import Literal

import strata.plugins.base as base


class LocalStorage(base.BaseStorage):
    kind: Literal["local"] = "local"
    path: str
    catalog: str


class LocalSourceConfig(base.BaseSourceConfig):
    """Local file source configuration.

    Example:
        from strata.plugins.local.storage import LocalSourceConfig

        config = LocalSourceConfig(
            path="./data/events.parquet",
            format="parquet",
        )
    """
    path: str
    format: Literal["parquet", "csv", "json", "delta"] = "parquet"
