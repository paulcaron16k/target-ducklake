# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
import re
import warnings
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Context, Decimal

import pyarrow as pa

from target_ducklake.type_hints import decimal_hint

# JSON Schema "number" maps to DECIMAL; DuckDB supports precision up to 38.
DECIMAL_MAX_PRECISION = 38
DECIMAL_DEFAULT_SCALE = 9
# Python's default decimal context keeps only 28 digits; DECIMAL(38, s) needs 38.
_DECIMAL_CONTEXT = Context(prec=DECIMAL_MAX_PRECISION, rounding=ROUND_HALF_EVEN)

logger = logging.getLogger(__name__)

EXTENSION_MAPPING = {
    "snappy": ".snappy",
    "gzip": ".gz",
    "brotli": ".br",
    "zstd": ".zstd",
    "lz4": ".lz4",
}


def number_precision_and_scale(jsonschema: dict) -> tuple[int, int]:
    """Return the (precision, scale) used for a decimal column.

    Shared by the DuckLake column DDL and the parquet staging schema so both
    always agree. An ``x-sql-datatype`` of ``decimal(p,s)`` or ``numeric(p,s)``
    wins; otherwise the schema's ``precision``/``scale`` are used when present
    (scale defaulting to 9 and never exceeding the precision), otherwise
    DECIMAL(38, 9). A precision above 38 is capped by ``_capped_to_38``.
    """
    precision, scale = decimal_hint(jsonschema) or (
        jsonschema.get("precision"),
        jsonschema.get("scale"),
    )
    if precision is None:
        return DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE
    scale = min(DECIMAL_DEFAULT_SCALE if scale is None else scale, precision)
    if precision > DECIMAL_MAX_PRECISION:
        return _capped_to_38(precision, scale)
    return precision, scale


def _capped_to_38(precision: int, scale: int) -> tuple[int, int]:
    """Fit DECIMAL(precision, scale) into DuckDB's 38 digits, with a warning.

    Fraction digits are dropped first, so the integer digits a value needs
    still fit: ``numeric(50,45)`` becomes DECIMAL(38, 33), not DECIMAL(38, 38),
    which can't hold 1. They are never dropped below ``min(scale, 9)``, the
    default scale, so a money column keeps its cents (``numeric(40,2)`` gives
    DECIMAL(38, 2)) and a capped type never keeps fewer fraction digits than
    the DECIMAL(38, 9) default. A value with more integer digits than are left
    fails the batch at staging (``decimal.InvalidOperation`` in
    ``_to_decimal``), as it would with DECIMAL(38, 9).
    """
    integer_digits = precision - scale
    fraction_floor = min(scale, DECIMAL_DEFAULT_SCALE)
    capped_scale = min(
        scale, max(DECIMAL_MAX_PRECISION - integer_digits, fraction_floor)
    )
    warnings.warn(
        f"DECIMAL({precision}, {scale}) exceeds DuckDB's 38-digit DECIMAL; "
        f"using DECIMAL({DECIMAL_MAX_PRECISION}, {capped_scale}).",
        UserWarning,
        stacklevel=3,
    )
    return DECIMAL_MAX_PRECISION, capped_scale


# DuckDB column types staged as int64 or float64. Every other type except
# DECIMAL and BOOLEAN (VARCHAR, JSON, UUID, temporal types) is staged as text,
# which DuckDB parses on insert.
_INTEGER_COLUMN_TYPES = {"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"}
_FLOAT_COLUMN_TYPES = {"REAL", "FLOAT", "FLOAT4", "DOUBLE", "FLOAT8"}
_DECIMAL_COLUMN_TYPE = re.compile(
    r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$"
)


def staging_type(column_type: str) -> pa.DataType:
    """Return the parquet type a column of this DuckDB type is staged as.

    Staging follows the column's own type, so every value the column can hold
    stages, and a value the column can't hold fails the batch at staging
    instead of being changed: a DECIMAL column is staged at its exact
    precision and scale, a REAL or DOUBLE column as float64 (decimal staging
    would round 1.5e-12 to 0), and a VARCHAR column, such as one declared with
    several JSON types, as text.
    """
    name = " ".join(column_type.upper().split())
    if match := _DECIMAL_COLUMN_TYPE.match(name):
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    if name in _INTEGER_COLUMN_TYPES:
        return pa.int64()
    if name in _FLOAT_COLUMN_TYPES:
        return pa.float64()
    if name == "BOOLEAN":
        return pa.bool_()
    return pa.string()


def staging_schema(columns: list[dict[str, str]]) -> pa.Schema:
    """Return the parquet schema that stages these DuckLake columns.

    ``columns`` is the ``[{"name": ..., "type": ...}]`` list the column DDL is
    built from, so staging and DDL can't disagree.
    """
    return pa.schema(
        [pa.field(column["name"], staging_type(column["type"])) for column in columns]
    )


def _normalize_datetime(value):
    """Convert a timezone-aware datetime to UTC (used with convert_tz_to_utc)."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        # Convert to UTC timezone
        return value.astimezone(timezone.utc)
    return value


def _normalize_data_for_pyarrow(data: dict, datetime_fields: list[str]) -> dict:
    """Normalize data values to be compatible with PyArrow.

    This handles datetime objects with non-standard timezone names.
    Modifies the data dict in-place for efficiency.
    """
    if not datetime_fields:
        return data

    for key in datetime_fields:
        if key in data:
            data[key] = [_normalize_datetime(v) for v in data[key]]
    return data


def _to_decimal(value: object, scale: int) -> Decimal | None:
    """Convert a numeric value to a Decimal rounded to ``scale`` digits.

    Rounds half-even, matching how DuckDB rounds when casting to DECIMAL.
    Floats go through ``str`` so their shortest repr is kept, not binary noise.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        value = int(value)
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(Decimal(1).scaleb(-scale), context=_DECIMAL_CONTEXT)


def _has_fixed_offset(value: object) -> bool:
    """Whether a value is a datetime with a fixed, non-zero UTC offset.

    The singer-sdk parses '...+02:00' into ``timezone(timedelta(hours=2))``.
    pyarrow looks such an offset up as a zone name and fails the whole batch
    ("Cannot locate timezone '+02:00'").
    """
    return (
        isinstance(value, datetime)
        and isinstance(value.tzinfo, timezone)
        and value.utcoffset() != timedelta(0)
    )


def _datetime_text(value: datetime) -> str:
    """Render a datetime as ISO text that DuckDB reads exactly.

    Naive: '2024-01-01 10:00:00.000000'; UTC: '...000000Z'; any other zone or
    offset: its own local time and offset, '...000000+02:00' (with seconds,
    '+02:00:30', when the offset has them). A named zone's local time that
    doesn't exist (a DST gap) is normalised through UTC first, as pyarrow does;
    fixed offsets have no gaps, so they skip that round trip, which would
    overflow at the edges of the year range.
    """
    if value.tzinfo is None:
        return _wall_clock_text(value)
    if value.tzinfo is timezone.utc:
        return _wall_clock_text(value) + "Z"
    local = value
    if not isinstance(value.tzinfo, timezone):
        try:
            local = value.astimezone(timezone.utc).astimezone(value.tzinfo)
        except OverflowError:
            local = value
    return _wall_clock_text(local) + _offset_text(local.utcoffset())


def _wall_clock_text(value: datetime) -> str:
    """'YYYY-MM-DD HH:MM:SS.ffffff' with a zero-padded year (strftime's %Y isn't)."""
    return value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")


def _offset_text(offset: timedelta | None) -> str:
    """'+HH:MM', or '+HH:MM:SS' when the offset has seconds (DuckDB reads both)."""
    seconds = int((offset or timedelta(0)).total_seconds())
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}:{minutes:02d}"
    return f"{text}:{secs:02d}" if secs else text


def _scalar_text(value: object) -> object:
    """Render a JSON scalar as the text a VARCHAR column stores for it.

    Booleans as 'true'/'false' (DuckDB's spelling), numbers with their exact
    digits ('1.50' stays '1.50', 1E+2 is '100'). Other values are left as
    they are: strings, None, and datetimes, which are formatted below.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return repr(value)
    return value


def _integer_value(value: object) -> object:
    """An integral Decimal or a bool as an int, so int64 staging keeps every digit.

    A Decimal with a fraction is left as it is, so the cast to int64 fails
    instead of truncating it.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def _coerce_column(values: list, field: pa.Field) -> list:
    """Coerce Python values so pyarrow can build the column with the field's type."""
    if pa.types.is_decimal(field.type):
        return [_to_decimal(v, field.type.scale) for v in values]
    if pa.types.is_integer(field.type):
        return [_integer_value(v) for v in values]
    if pa.types.is_string(field.type):
        values = [_scalar_text(v) for v in values]
    if any(_has_fixed_offset(v) for v in values):
        # pyarrow can't build this column, so render every datetime in it as
        # text, each in its own offset. Columns without fixed offsets are left
        # to pyarrow, so what loaded before stages byte-for-byte the same.
        values = [_datetime_text(v) if isinstance(v, datetime) else v for v in values]
    # Decimals in float columns become floats.
    return [float(v) if isinstance(v, Decimal) else v for v in values]


def create_pyarrow_table(
    list_dict: list[dict], pyarrow_schema: pa.Schema, datetime_fields: list = []
) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {
        field.name: [row.get(field.name) for row in list_dict]
        for field in pyarrow_schema
    }
    # With convert_tz_to_utc, move aware datetimes to UTC before they are
    # formatted as text below.
    normalized_data = _normalize_data_for_pyarrow(data, datetime_fields)
    coerced = {
        field.name: _coerce_column(normalized_data[field.name], field)
        for field in pyarrow_schema
    }
    return pa.table(coerced).cast(pyarrow_schema)


def concat_tables(
    records: list[dict],
    pyarrow_table: pa.Table,
    pyarrow_schema: pa.Schema,
    flattened_schema: dict,
    convert_tz_to_utc: bool = False,
) -> pa.Table:
    """Create a dataframe from records and concatenate with the existing one."""
    if not records:
        return pyarrow_table
    if convert_tz_to_utc:
        datetime_fields = [
            field
            for field in flattened_schema
            if "format" in flattened_schema[field]
            and flattened_schema[field]["format"] == "date-time"
        ]
    else:
        datetime_fields = []
    new_table = create_pyarrow_table(records, pyarrow_schema, datetime_fields)
    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table
