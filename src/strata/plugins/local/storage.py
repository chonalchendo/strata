from typing import Literal

import strata.plugins.base as base


class LocalStorage(base.BaseStorage):
    kind: Literal["local"] = "local"
    path: str
    catalog: str
