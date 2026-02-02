from typing import Literal

import strata.plugins.base as base


class SqliteRegistry(base.BaseRegistry):
    kind: Literal["sqlite"] = "sqlite"
    path: str
