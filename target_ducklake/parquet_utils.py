# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Context, Decimal

import pyarrow as pa

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
    """Return the (precision, scale) used for a JSON Schema "number" property.

    Shared by the DuckLake column DDL and the parquet staging schema so both
    always agree. Uses the schema's ``precision``/``scale`` when present
    (precision capped at 38, scale defaulting to 9 and never exceeding the
    precision), otherwise DECIMAL(38, 9).
    """
    precision = jsonschema.get("precision")
    scale = jsonschema.get("scale")
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
    if input_type == "NUMBER":
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
    """Normalize datetime objects to UTC timezone for PyArrow compatibility.

    PyArrow doesn't recognize custom timezone strings like 'UTC-07:00'.
    This function converts datetime objects with any timezone to UTC.
    """
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


def _coerce_column(values: list, field: pa.Field) -> list:
    """Coerce Python values so pyarrow can build the column with the field's type."""
    if pa.types.is_decimal(field.type):
        return [_to_decimal(v, field.type.scale) for v in values]
    # Non-number columns keep the previous behaviour: Decimals become floats.
    return [float(v) if isinstance(v, Decimal) else v for v in values]


def create_pyarrow_table(
    list_dict: list[dict], pyarrow_schema: pa.Schema, datetime_fields: list = []
) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {
        field.name: _coerce_column([row.get(field.name) for row in list_dict], field)
        for field in pyarrow_schema
    }
    # Normalize datetime values to UTC timezone for PyArrow compatibility
    normalized_data = _normalize_data_for_pyarrow(data, datetime_fields)
    return pa.table(normalized_data).cast(pyarrow_schema)


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
