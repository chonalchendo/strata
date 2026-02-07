from typing import Literal

import pydantic as pdt

import strata.backends.base as base


class DuckDBCompute(base.BaseCompute):
    kind: Literal["duckdb"] = "duckdb"
    extensions: list[str] = pdt.Field(default_factory=list)
