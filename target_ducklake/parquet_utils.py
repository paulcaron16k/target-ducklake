# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import logging
import math
import re
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation

import pyarrow as pa

from target_ducklake.type_hints import decimal_hint

# JSON Schema "number" maps to DECIMAL; DuckDB supports precision up to 38.
DECIMAL_MAX_PRECISION = 38
DECIMAL_DEFAULT_SCALE = 9
# Python's default decimal context keeps only 28 digits; DECIMAL(38, s) needs 38.
# DuckDB's cast of a DECIMAL rounds half away from zero (1.225 to 1.23 in
# DECIMAL(10, 2), 2.5 to 3 in BIGINT), which is Python's ROUND_HALF_UP. (Its
# cast of a DOUBLE rounds half to even, but values reach staging as the exact
# Decimals the JSON held, so they are rounded as decimals, whatever the hint.)
_DECIMAL_CONTEXT = Context(prec=DECIMAL_MAX_PRECISION, rounding=ROUND_HALF_UP)

# value_type_checking: what staging does with a value its column would round.
STRICT = "strict"
PERMISSIVE = "permissive"
VALUE_TYPE_CHECKING_MODES = (PERMISSIVE, STRICT)

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
    fails the batch at staging (``ValueError`` in ``_decimal_column``), as
    it would with DECIMAL(38, 9).
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


def _as_utc_instant(value: object) -> object:
    """A date-time as UTC text, a value without a zone or offset read as UTC.

    Every spelling of an instant becomes the same text
    ('2024-01-01 12:00:00.000000Z'), so the in-batch key dedup, which compares
    the staged values, sees '...12:00Z', '...21:00+09:00' and a naive
    '...12:00' as one key. Text the singer-sdk didn't parse (a flattened
    nested value, say) is parsed as the SDK parses date-times. Text that
    doesn't parse, and a value whose UTC time leaves the year range, are left
    as they are for DuckDB to read or refuse.
    """
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return value
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        return _datetime_text(value.replace(tzinfo=timezone.utc))
    try:
        return _datetime_text(value.astimezone(timezone.utc))
    except OverflowError:
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


def _as_decimal(value: object) -> Decimal | None:
    """Convert a numeric value to the Decimal it represents exactly.

    Floats go through ``str`` so their shortest repr is kept, not binary noise.
    """
    if value is None or isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        value = int(value)
    return Decimal(str(value))


@dataclass(frozen=True)
class StagingRules:
    """What staging does with a value its column would round.

    A value with more fraction digits than its DECIMAL column holds, or a
    fraction for an integer column, is one DuckDB's cast would round. With
    ``strict`` it fails the batch, naming the column and the value. Otherwise
    it is rounded as DuckDB's cast of a DECIMAL rounds it (half away from
    zero), and a warning counts the rounded
    values per column; for a key column the warning says records may have
    merged. A value a column can't hold at all (too many integer digits, text
    that isn't a number) fails either way, as DuckDB's cast fails on it.

    ``key_columns`` holds the key columns' lowercased names.

    ``utc_columns`` holds the lowercased names of the TIMESTAMPTZ columns in
    which a date-time without a zone or offset is read as UTC, as
    target-iceberg reads it, instead of in DuckDB's session time zone (the
    machine's local zone by default).
    """

    strict: bool = False
    key_columns: frozenset[str] = frozenset()
    utc_columns: frozenset[str] = frozenset()

    def is_key(self, column: str) -> bool:
        """Whether the column is a key column, matched case-insensitively."""
        return column.lower() in self.key_columns

    def is_utc(self, column: str) -> bool:
        """Whether a zone-less date-time in the column is UTC (case-insensitive)."""
        return column.lower() in self.utc_columns


DEFAULT_STAGING_RULES = StagingRules()

_MAX_SHOWN_VALUE = 50


def _shown(value: object) -> str:
    """The value as given, cut to 50 characters, since it may be source text."""
    shown = repr(value)
    if len(shown) > _MAX_SHOWN_VALUE:
        shown = shown[: _MAX_SHOWN_VALUE - 3] + "..."
    return shown


def _column_kind(field: pa.Field) -> str:
    """How an error describes the column's type: 'is DECIMAL(10, 2)'."""
    if pa.types.is_decimal(field.type):
        return f"is DECIMAL({field.type.precision}, {field.type.scale})"
    return "holds whole numbers"


class _DecimalMismatchError(ValueError):
    """A value a DECIMAL(p, s) column can't hold; names the column and the value."""

    def __init__(self, field: pa.Field, value: object) -> None:
        super().__init__(
            f"Column {field.name!r} {_column_kind(field)}; {_shown(value)} doesn't fit."
        )


def _check_rounding(
    field: pa.Field, value: object, result: object, rules: StagingRules
) -> None:
    """Fail on a value staging would round, if ``value_type_checking`` is strict."""
    if not rules.strict:
        return
    message = (
        f"Column {field.name!r} {_column_kind(field)}; {_shown(value)} would be "
        f"rounded to {result}"
    )
    if rules.is_key(field.name):
        message += (
            ", and the column is a key: records with different keys would merge "
            "into one row"
        )
    message += (
        ". value_type_checking is strict; set it to permissive to round such "
        "values with a warning."
    )
    raise ValueError(message)


def _report_rounding(
    field: pa.Field, changed: int, rounding: str, rules: StagingRules
) -> None:
    """Warn how many of a batch's values in a column were rounded."""
    if not changed:
        return
    if rules.is_key(field.name):
        logger.warning(
            "%d key value(s) in column %r %s. Records whose keys round to the "
            "same value are merged into one row; set value_type_checking to "
            "strict to fail instead.",
            changed,
            field.name,
            rounding,
        )
    else:
        logger.warning("%d value(s) in column %r %s.", changed, field.name, rounding)


def _decimal_column(
    values: list, field: pa.Field, rules: StagingRules = DEFAULT_STAGING_RULES
) -> list[Decimal | None]:
    """Round each value to the column's scale, failing on any that doesn't fit.

    Extra fraction digits are rounded half away from zero, as DuckDB's
    cast to the column rounds them, and a warning reports how many values
    were rounded; with strict ``rules`` such a value fails the batch instead.
    A value with more integer digits than DECIMAL(p, s) holds, a non-finite
    number or text that isn't a number raises ValueError naming the column.
    """
    precision, scale = field.type.precision, field.type.scale
    quantum = Decimal(1).scaleb(-scale)
    rounded: list[Decimal | None] = []
    changed = 0
    for value in values:
        try:
            exact = _as_decimal(value)
            if exact is None:
                rounded.append(None)
                continue
            result = exact.quantize(quantum, context=_DECIMAL_CONTEXT)
        except InvalidOperation:  # not a number, or more than 38 digits
            raise _DecimalMismatchError(field, value) from None
        if not result.is_finite() or (
            result and result.adjusted() >= precision - scale
        ):
            raise _DecimalMismatchError(field, value)
        if result != exact:
            _check_rounding(field, value, result, rules)
            changed += 1
        rounded.append(result)
    _report_rounding(
        field,
        changed,
        f"had more than {scale} fraction digits and were rounded to fit "
        f"DECIMAL({precision}, {scale})",
        rules,
    )
    return rounded


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


def _mixes_zones(values: list) -> bool:
    """Whether a column's datetimes are in more than one zone (naive counts as one).

    pyarrow converts such a column to the zone of its first value, so a
    timestamp column's wall-clock times would depend on the order of the
    records: [UTC 10:00, Paris 10:00] staged Paris as 09:00, and [Paris 10:00,
    UTC 10:00] staged UTC as 11:00. A naive value was read as UTC after an
    aware one, and an aware value lost its zone after a naive one.
    """
    return len({v.tzinfo for v in values if isinstance(v, datetime)}) > 1


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


def _number_text(value: object) -> object:
    """Text holding a number as a Decimal, read as DuckDB's cast reads it.

    Surrounding spaces, a sign, an exponent and trailing zeros are accepted
    ('+5', ' 7 ', '1e2', '12.0'), so text that loaded through DuckDB's cast
    when it was staged as text still loads. Anything else is left as it is.
    """
    if isinstance(value, str):
        try:
            return Decimal(value.strip())
        except InvalidOperation:
            pass
    return value


_INT64_RANGE = range(-(2**63), 2**63)
_INT64_DIGITS = len(str(2**63))  # 19: a Decimal with more digits can't fit


def _out_of_range(column: str, value: object) -> ValueError:
    return ValueError(
        f"Column {column!r} holds 64-bit integers; {_shown(value)} is out of range."
    )


def _number_value(value: object, column: str) -> int | Decimal | None:
    """A number (int, bool, Decimal, float or number text) as an int or a Decimal.

    A whole number is an int; a number with a fraction is the exact Decimal.
    Raises ValueError naming the column for anything else (a non-finite
    number, text that isn't a number, a number far outside int64), which
    DuckDB's cast to an integer fails on too.
    """
    number = _number_text(value)
    if number is None:
        return None
    if isinstance(number, float) and math.isfinite(number):
        number = _as_decimal(number)
    if isinstance(number, int):  # bool is an int
        return int(number)
    if isinstance(number, Decimal) and number.is_finite():
        if number and number.adjusted() >= _INT64_DIGITS:
            # Rejected on magnitude: int() of '1e1000000' would take minutes.
            # (A zero's adjusted() is no magnitude: '0e100' is 0.)
            raise _out_of_range(column, value)
        return int(number) if number == number.to_integral_value() else number
    msg = f"Column {column!r} holds whole numbers; {_shown(value)} is not a number."
    raise ValueError(msg)


def _integer_column(
    values: list, field: pa.Field, rules: StagingRules = DEFAULT_STAGING_RULES
) -> list[int | None]:
    """Each value as an exact int, failing on any the column can't hold.

    A fraction is rounded half away from zero, as DuckDB's cast to an integer
    rounds it (pyarrow would truncate), and a warning reports how many values
    were rounded; with strict ``rules`` such a value fails the batch instead.
    """
    integers: list[int | None] = []
    changed = 0
    for value in values:
        number = _number_value(value, field.name)
        if isinstance(number, Decimal):
            integer = int(number.to_integral_value(rounding=ROUND_HALF_UP))
            _check_rounding(field, value, integer, rules)
            changed += 1
            number = integer
        if number is not None and number not in _INT64_RANGE:
            raise _out_of_range(field.name, value)
        integers.append(number)
    _report_rounding(
        field, changed, "had a fraction and were rounded to whole numbers", rules
    )
    return integers


def _coerce_column(
    values: list, field: pa.Field, rules: StagingRules = DEFAULT_STAGING_RULES
) -> list:
    """Coerce Python values so pyarrow can build the column with the field's type.

    ``rules`` say what to do with a number its DECIMAL or integer column would
    round.
    """
    if pa.types.is_decimal(field.type):
        return _decimal_column(values, field, rules)
    if pa.types.is_integer(field.type):
        return _integer_column(values, field, rules)
    if pa.types.is_floating(field.type):
        values = [_number_text(v) for v in values]
        for value in values:
            # A ValueError, as for every value staging refuses; pyarrow's
            # own error wouldn't name the column.
            if isinstance(value, str):
                msg = (
                    f"Column {field.name!r} holds floating-point numbers; "
                    f"{_shown(value)} is not a number."
                )
                raise ValueError(msg)  # noqa: TRY004
    if pa.types.is_string(field.type):
        values = [_scalar_text(v) for v in values]
    if any(_has_fixed_offset(v) for v in values) or _mixes_zones(values):
        # pyarrow can't build a column with a fixed offset, and moves a column
        # mixing zones into its first value's zone, so render every datetime
        # in it as text, each in its own offset. Columns in one zone are left
        # to pyarrow, so what loaded before stages byte-for-byte the same.
        values = [_datetime_text(v) if isinstance(v, datetime) else v for v in values]
    # Decimals in float columns become floats.
    return [float(v) if isinstance(v, Decimal) else v for v in values]


def create_pyarrow_table(
    list_dict: list[dict],
    pyarrow_schema: pa.Schema,
    datetime_fields: list = [],
    rules: StagingRules = DEFAULT_STAGING_RULES,
) -> pa.Table:
    """Create a pyarrow Table from a python list of dict.

    ``rules`` say what to do with a number its column would round.
    """
    data = {
        field.name: [row.get(field.name) for row in list_dict]
        for field in pyarrow_schema
    }
    for name, values in data.items():
        if rules.is_utc(name):
            data[name] = [_as_utc_instant(v) for v in values]
    # With convert_tz_to_utc, move aware datetimes to UTC before they are
    # formatted as text below.
    normalized_data = _normalize_data_for_pyarrow(data, datetime_fields)
    coerced = {
        field.name: _coerce_column(normalized_data[field.name], field, rules)
        for field in pyarrow_schema
    }
    return pa.table(coerced).cast(pyarrow_schema)


def concat_tables(  # noqa: PLR0913 (the last two are optional staging options)
    records: list[dict],
    pyarrow_table: pa.Table,
    pyarrow_schema: pa.Schema,
    flattened_schema: dict,
    convert_tz_to_utc: bool = False,
    rules: StagingRules = DEFAULT_STAGING_RULES,
) -> pa.Table:
    """Create a dataframe from records and concatenate with the existing one.

    ``rules`` say what to do with a number its column would round.
    """
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
    new_table = create_pyarrow_table(records, pyarrow_schema, datetime_fields, rules)
    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table
