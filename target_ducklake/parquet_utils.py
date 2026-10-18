# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
import warnings
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Context, Decimal

import pyarrow as pa

from target_ducklake.type_hints import decimal_hint, is_float_hint

FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "OBJECT": pa.string(),
}

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

logger = logging.getLogger(__name__)


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


def _field_type_to_pyarrow_field(
    field_name: str, input_types: dict, required_fields: list[str]
) -> pa.Field:
    types = input_types.get("type", [])
    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)
    types = [types] if isinstance(types, str) else types
    types_uppercase = [item.upper() for item in types]
    nullable = "NULL" in types_uppercase or field_name not in required_fields
    if "NULL" in types_uppercase:
        types_uppercase.remove("NULL")
    input_type = next(iter(types_uppercase)) if types_uppercase else ""
    if input_type == "NUMBER" and is_float_hint(input_types):
        # A REAL or DOUBLE column: decimal staging would round away digits
        # beyond its scale (1.5e-12 would arrive as 0).
        pyarrow_type = pa.float64()
    elif input_type == "NUMBER":
        # Stage numbers as decimals, not float64, so values keep their exact digits
        # on the way into the DECIMAL column.
        pyarrow_type = pa.decimal128(*number_precision_and_scale(input_types))
    else:
        pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, pa.string())
    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order.

    E.g:
     dictionary = {
         'key_1': {'type': ['null', 'integer']},
         'key_2__key_3': {'type': ['null', 'string']},
         'key_2__key_4__key_5': {'type': ['null', 'integer']},
         'key_2__key_4__key_6': {'type': ['null', 'array']}
     }
    By calling the function with the dictionary above as parameter,
    you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    return pa.schema(
        [
            _field_type_to_pyarrow_field(
                field_name, field_input_types, required_fields=[]
            )
            for field_name, field_input_types in flatten_schema_dictionary.items()
        ]
    )


def _normalize_datetime(value):
    """Convert a timezone-aware datetime to UTC (used with convert_tz_to_utc).

    A date-time the singer-sdk didn't parse (it parses only top-level
    properties, so a flattened nested value arrives as text) is converted too:
    text with an offset, such as '2024-01-01T10:00:00+05:30', becomes UTC text
    ('2024-01-01 04:30:00.000000Z'). It is parsed as the SDK parses date-times.
    Text without an offset, text that doesn't parse, and text whose UTC time
    leaves the year range are left as they are.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        # Convert to UTC timezone
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            utc = parsed.astimezone(timezone.utc) if parsed.tzinfo else None
        except (ValueError, OverflowError):
            utc = None
        if utc is not None:
            return _datetime_text(utc)
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


def _coerce_column(values: list, field: pa.Field) -> list:
    """Coerce Python values so pyarrow can build the column with the field's type."""
    if pa.types.is_decimal(field.type):
        return [_to_decimal(v, field.type.scale) for v in values]
    if any(_has_fixed_offset(v) for v in values):
        # pyarrow can't build this column, so render every datetime in it as
        # text, each in its own offset. Columns without fixed offsets are left
        # to pyarrow, so what loaded before stages byte-for-byte the same.
        values = [_datetime_text(v) if isinstance(v, datetime) else v for v in values]
    # Decimals in non-number columns become floats (the previous behaviour).
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
