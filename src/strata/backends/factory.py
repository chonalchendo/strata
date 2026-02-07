from typing import Annotated

import pydantic as pdt

import strata.backends.duckdb as duckdb
import strata.backends.sqlite as sqlite

RegistryKind = Annotated[sqlite.SqliteRegistry, pdt.Field(discriminator="kind")]

BackendKind = Annotated[duckdb.DuckDBBackend, pdt.Field(discriminator="kind")]
