# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
from datetime import datetime, timezone
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
    (precision capped at 38, scale defaulting to 9 and never exceeding the
    precision), otherwise DECIMAL(38, 9).
    """
    precision, scale = decimal_hint(jsonschema) or (
        jsonschema.get("precision"),
        jsonschema.get("scale"),
    )
    if precision is None:
        return DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE
    precision = min(precision, DECIMAL_MAX_PRECISION)
    scale = DECIMAL_DEFAULT_SCALE if scale is None else scale
    return precision, min(scale, precision)


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


def _to_staged_value(value: object) -> object:
    """Prepare a value for a non-decimal column.

    - A timezone-aware datetime becomes ISO text carrying its own offset
      ('2024-01-01 10:00:00+02:00'). pyarrow renders a named zone that way
      itself, but looks a fixed offset such as +02:00 up as a zone name and
      fails; the text keeps the instant for TIMESTAMPTZ columns, and a
      TIMESTAMP column keeps the wall-clock time, as for named zones.
    - A Decimal becomes a float (the previous behaviour).
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.isoformat(sep=" ")
    if isinstance(value, Decimal):
        return float(value)
    return value


def _coerce_column(values: list, field: pa.Field) -> list:
    """Coerce Python values so pyarrow can build the column with the field's type."""
    if pa.types.is_decimal(field.type):
        return [_to_decimal(v, field.type.scale) for v in values]
    return [_to_staged_value(v) for v in values]


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
