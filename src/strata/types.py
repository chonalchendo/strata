"""Core type definitions and aliases for Strata.

PyArrow is the data interchange format for all Strata operations.
This ensures zero-copy data transfer where possible and consistent
typing across the codebase.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    pass

# =============================================================================
# PyArrow Type Aliases (STG-05)
# =============================================================================
# PyArrow is the canonical interchange format for all data operations.
# These aliases provide clear typing throughout the codebase.

ArrowTable = pa.Table
ArrowSchema = pa.Schema
ArrowArray = pa.Array
ArrowChunkedArray = pa.ChunkedArray
ArrowField = pa.Field
ArrowDataType = pa.DataType

# Common data types for schema definitions
Int64 = pa.int64()
Float64 = pa.float64()
String = pa.string()
Bool = pa.bool_()
Timestamp = pa.timestamp("us")  # microsecond precision
Date = pa.date32()
