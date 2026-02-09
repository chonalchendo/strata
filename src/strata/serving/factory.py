"""Online store factory types.

OnlineStoreKind is a simple union type (not a discriminated union).
When RedisOnlineStore or PostgresOnlineStore are added, extend the union.
"""

from __future__ import annotations

import strata.serving.sqlite as sqlite

# Simple union type -- expand when adding new online store backends
# e.g., OnlineStoreKind = sqlite.SqliteOnlineStore | redis.RedisOnlineStore
OnlineStoreKind = sqlite.SqliteOnlineStore
