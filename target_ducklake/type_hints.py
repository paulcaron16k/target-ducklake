"""Map ``x-sql-datatype`` hints to DuckDB column types.

JSON Schema can't say smallint vs bigint, real vs double, decimal(p,s), or
timestamp vs timestamptz. Database taps carry the source column's declared type
in the ``x-sql-datatype`` annotation instead. This module is the one vocabulary
for those hints, shared by the DuckLake DDL (``connector``) and the parquet
staging schema (``parquet_utils``) so the two always agree.

A hint not listed here maps to nothing, and the column keeps the type its JSON
Schema type and format give it.
"""

from __future__ import annotations

import re
from typing import Any

# Integer widths: ISO names plus the intN spellings catalog-reading taps emit.
# `tinyint` is deliberately absent: MySQL uses tinyint(1) for booleans.
_INTEGER_HINTS = {
    "smallint": "SMALLINT",
    "int2": "SMALLINT",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int4": "INTEGER",
    "bigint": "BIGINT",
    "int8": "BIGINT",
}

# Floating point. An unqualified `float` is double precision, as in Postgres
# and SQL Server; ISO `float(p)` is single precision up to 24 binary digits.
_FLOAT_HINTS = {
    "real": "REAL",
    "float4": "REAL",
    "float": "DOUBLE",
    "float8": "DOUBLE",
    "double": "DOUBLE",
    "double precision": "DOUBLE",
}
_FLOAT_PRECISION_HINT = re.compile(r"^float\s*\(\s*(\d+)\s*\)$")
_MAX_SINGLE_PRECISION_BITS = 24

# Dates and times. An unqualified `timestamp` follows ISO (without time zone).
# `datetime` is absent: MySQL's DATETIME and TIMESTAMP mean the opposite of the
# ISO reading, so the name alone can't say which was meant. Snowflake's names
# state the answer in the name, so they are admitted.
#
# `timetz` / `time with time zone` are deliberately absent. DuckDB keeps a
# TIMETZ offset in memory, but writing it to Parquet (which DuckLake always
# does) drops the offset without converting, so '10:00+02' is stored as
# '10:00+00' -- a silent two-hour error. Unhinted, such a column stays VARCHAR
# and keeps its text intact.
_TEMPORAL_HINTS = {
    "timestamp": "TIMESTAMP",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
    "timestamp with time zone": "TIMESTAMPTZ",
    "timestamp_tz": "TIMESTAMPTZ",
    "timestamp_ltz": "TIMESTAMPTZ",
    "date": "DATE",
    "time": "TIME",
    "time without time zone": "TIME",
}

_OTHER_HINTS = {
    "uuid": "UUID",
    "uniqueidentifier": "UUID",
    "json": "JSON",
    "jsonb": "JSON",
}

_HINT_TYPES = {**_INTEGER_HINTS, **_FLOAT_HINTS, **_TEMPORAL_HINTS, **_OTHER_HINTS}

# `decimal(p,s)` and `numeric(p,s)` are the two portable spellings of the same
# exact numeric; a tap reading a catalog emits whichever the source declared.
_DECIMAL_HINT = re.compile(r"^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def sql_datatype_hint(jsonschema: dict[str, Any]) -> str:
    """Return the property's ``x-sql-datatype``, lowercased with spaces collapsed.

    Returns an empty string when the property carries no hint.
    """
    return " ".join(str(jsonschema.get("x-sql-datatype") or "").split()).lower()


def decimal_hint(jsonschema: dict[str, Any]) -> tuple[int, int] | None:
    """Return the (precision, scale) of a ``decimal(p,s)``/``numeric(p,s)`` hint.

    A hint DuckDB's DECIMAL can't represent (precision below 1, or scale above
    precision) returns None, so the column keeps its default mapping and the
    SDK warns that the hint isn't supported, instead of the type being
    invalid (``DECIMAL(0, 0)``) or silently changed (``DECIMAL(5, 10)`` to
    ``DECIMAL(5, 5)``). Precision above 38 is valid and is capped at 38 later.
    """
    if match := _DECIMAL_HINT.match(sql_datatype_hint(jsonschema)):
        precision, scale = int(match.group(1)), int(match.group(2))
        if precision >= 1 and scale <= precision:
            return precision, scale
    return None


def hinted_type(jsonschema: dict[str, Any]) -> str | None:
    """Return the DuckDB type a non-decimal hint maps to, or None if unsupported."""
    hint = sql_datatype_hint(jsonschema)
    if match := _FLOAT_PRECISION_HINT.match(hint):
        bits = int(match.group(1))
        return "REAL" if bits <= _MAX_SINGLE_PRECISION_BITS else "DOUBLE"
    return _HINT_TYPES.get(hint)


# The hinted column types a property's JSON values can be loaded into. A
# string can carry any type as text (a timestamp, a uuid, digits), so a string
# property takes any hint. Numbers and booleans load into numeric columns, and
# any value into JSON; object and array values only into JSON.
_NUMERIC_COLUMN_TYPES = frozenset(
    {*_INTEGER_HINTS.values(), *_FLOAT_HINTS.values(), "DECIMAL"}
)
_HINTABLE_COLUMN_TYPES: dict[str, frozenset[str]] = {
    "integer": _NUMERIC_COLUMN_TYPES | {"JSON"},
    "number": _NUMERIC_COLUMN_TYPES | {"JSON"},
    "boolean": _NUMERIC_COLUMN_TYPES | {"JSON"},
    "object": frozenset({"JSON"}),
    "array": frozenset({"JSON"}),
}


def hint_fits_json_type(jsonschema: dict[str, Any], column_type: str) -> bool:
    """Whether every JSON type the property declares can load into the hinted type.

    ``column_type`` is the hinted DuckDB type without parameters (``DECIMAL``
    for ``numeric(12,4)``). A hint that doesn't fit, such as ``uuid`` on an
    integer, would make a column no value of the property can be loaded into.
    """
    types = jsonschema.get("type") or []
    types = [types] if isinstance(types, str) else types
    return all(
        column_type in _HINTABLE_COLUMN_TYPES[t]
        for t in types
        if t in _HINTABLE_COLUMN_TYPES
    )
