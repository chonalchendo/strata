from typing import Annotated

import pydantic as pdt

import strata.backends.duckdb as duckdb
import strata.backends.local as local
import strata.backends.sqlite as sqlite

RegistryKind = Annotated[sqlite.SqliteRegistry, pdt.Field(discriminator="kind")]

# duckdb can scan data from local storage so it can be viewed there
StorageKind = Annotated[local.LocalStorage, pdt.Field(discriminator="kind")]
ComputeKind = Annotated[duckdb.DuckDBCompute, pdt.Field(discriminator="kind")]
