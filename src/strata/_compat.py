"""Python 3.14+ compatibility workarounds.

Applied once at package import time (from ``__init__.py``).

sqlglot workaround
------------------
sqlglot's Oracle compiler executes ``Literal.number("binary_double_nan")``
at class-definition time.  On Python 3.14+ the stricter ``decimal`` module
raises ``InvalidOperation`` for this non-numeric string, which crashes the
import of ``ibis.backends.sql.compilers`` and leaves it in a permanently
broken state.

Disabling the ``InvalidOperation`` trap before any ibis import allows the
literal to be created harmlessly.  The trap is *not* re-enabled because
sqlglot may lazily import additional compiler modules later.
"""

from __future__ import annotations

import decimal as _decimal

_decimal.getcontext().traps[_decimal.InvalidOperation] = False
