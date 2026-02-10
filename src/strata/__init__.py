from . import _compat as _compat  # noqa: F401  -- Python 3.14 sqlglot workaround

from .core import AggFunction, Dataset, DType, Entity, Feature, FeatureTable, Field, Schema, SourceTable
from .backends.local import LocalSourceConfig
from .project import connect
from .sources import BatchSource, RealTimeSource, StreamSource

__all__ = [
    # core
    "SourceTable",
    "FeatureTable",
    "Feature",
    "Dataset",
    "Entity",
    "Schema",
    "Field",
    "DType",
    "AggFunction",
    # sources
    "BatchSource",
    "StreamSource",
    "RealTimeSource",
    # configs
    "LocalSourceConfig",
    # project
    "connect",
]
