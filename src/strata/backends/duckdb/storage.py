"""DuckDB storage plugin for local file sources."""

from __future__ import annotations

from typing import Literal

import strata.backends.base as base


class DuckDBSourceConfig(base.BaseSourceConfig):
    """DuckDB source configuration for local files.

    Example:
        from strata.backends.duckdb.storage import DuckDBSourceConfig

        config = DuckDBSourceConfig(
            path="./data/transactions.parquet",
            format="parquet",
        )
    """

    path: str
    format: Literal["parquet", "csv", "json"] = "parquet"
