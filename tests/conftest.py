"""Global test configuration and fixtures.

Applies workarounds that must be in place before any test module imports.
"""

from __future__ import annotations

import decimal

# ---------------------------------------------------------------------------
# Python 3.14 workaround for sqlglot / ibis
# ---------------------------------------------------------------------------
# sqlglot's Oracle compiler triggers decimal.InvalidOperation when parsing
# the literal "binary_double_nan" during ibis backend initialization. On
# Python 3.14+ this crashes the import of ibis.backends.sql.compilers,
# leaving the module in a permanently broken state for the rest of the
# test session.
#
# Disabling the trap early (before any ibis import touches compilers)
# allows the import to succeed harmlessly.
# ---------------------------------------------------------------------------
decimal.getcontext().traps[decimal.InvalidOperation] = False
