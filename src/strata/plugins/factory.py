from typing import Annotated

import pydantic as pdt

import strata.plugins.duckdb as duckdb
import strata.plugins.local as local
import strata.plugins.sqlite as sqlite

RegistryKind = Annotated[sqlite.SqliteRegistry, pdt.Field(discriminator="kind")]

# duckdb can scan data from local storage so it can be viewed there
StorageKind = Annotated[local.LocalStorage, pdt.Field(discriminator="kind")]
ComputeKind = Annotated[duckdb.DuckDBCompute, pdt.Field(discriminator="kind")]
