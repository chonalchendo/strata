"""Consolidated infrastructure factory types.

Defines discriminated unions for all infrastructure components:
- BackendKind: offline compute+storage backends (DuckDB, etc.)
- RegistryKind: metadata registries (SQLite, etc.)
- OnlineStoreKind: online feature stores (SQLite, etc.)
"""

import strata.infra.backends.duckdb as duckdb
import strata.infra.backends.sqlite as sqlite
import strata.infra.serving.sqlite as sqlite_serving

RegistryKind = sqlite.SqliteRegistry
BackendKind = duckdb.DuckDBBackend
OnlineStoreKind = sqlite_serving.SqliteOnlineStore
