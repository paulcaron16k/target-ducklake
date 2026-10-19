"""Adapted from target-duckdb and Meltano SDK
https://github.com/jwills/target-duckdb/blob/36c8ce68a0b2584c4bbb07325482968b1edc0c40/target_duckdb/db_sync.py#L74
https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342
"""

import collections
import copy
import itertools
import json
from datetime import date, datetime
from decimal import Decimal

from target_ducklake.type_hints import sql_datatype_hint

TIMESTAMP_COLUMN_NAMES = {
    "timestamp",
    "created_at",
    "updated_at",
    "deleted_at",
    "modified_at",
    "last_modified",
    "updatedat",
    "createdat",
    "created_time",
    "updated_time",
}


class CustomJSONEncoder(json.JSONEncoder):
    # sometimes Python Decimal objects are returned, we need to convert them to floats when flattening the schema
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format string
        if isinstance(obj, date):
            return obj.isoformat()  # Convert date to ISO format string
        return super().default(obj)


def flatten_key(k, parent_key, sep):
    return sep.join(parent_key + [k])


def _should_auto_cast_to_timestamp(
    column_name: str, schema_entry: dict, auto_cast_timestamps: bool
) -> bool:
    """Check if a column should be auto-cast to timestamp format."""
    if not auto_cast_timestamps:
        return False

    # Skip if format is already specified
    if schema_entry.get("format") is not None:
        return False

    # Check if column name matches timestamp patterns
    if column_name.lower() not in TIMESTAMP_COLUMN_NAMES:
        return False

    # Check if it's a string type (either single string or list containing string)
    entry_type = schema_entry.get("type", [])
    if isinstance(entry_type, str):
        return entry_type == "string"
    if isinstance(entry_type, list):
        return "string" in entry_type

    return False


def _apply_timestamp_format(schema_entry: dict) -> dict:
    """Apply timestamp format to a schema entry."""
    result = dict(schema_entry)  # Create a copy
    result["format"] = "date-time"
    return result


# pylint: disable-msg=too-many-arguments
def flatten_record(
    d, flatten_schema=None, parent_key=[], sep="__", level=0, max_level=0
):
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(
                flatten_record(
                    v,
                    flatten_schema,
                    parent_key + [k],
                    sep=sep,
                    level=level + 1,
                    max_level=max_level,
                ).items()
            )
        else:
            items.append(
                (
                    new_key,
                    json.dumps(
                        v, cls=CustomJSONEncoder
                    )  # use custom encoder to convert Decimal type to float
                    # A missing value stays None (SQL NULL), not the JSON text
                    # 'null'; a null inside a value is still dumped as JSON.
                    if v is not None and _should_json_dump_value(k, v, flatten_schema)
                    else v,
                )
            )
    return dict(items)


# A property with no usable type can hold any value, so it is stored as JSON
# text, which keeps every value intact.
UNTYPED_COLUMN: dict[str, list[str]] = {"type": ["null", "object"]}


# JSON Schema keywords that list the alternatives a value can take. A property
# declared with one of them (and no "type") gets its column from a branch.
_CHOICE_KEYWORDS = ("anyOf", "oneOf", "allOf")


def _non_null_types(branch: object) -> list[str]:
    """A branch's declared types other than null, in their declared order."""
    if not isinstance(branch, dict):
        return []
    types = branch.get("type") or []
    types = [types] if isinstance(types, str) else types
    return [t for t in types if t != "null"]


def _choice_column(prop: dict, keyword: str) -> dict:
    """The column for an anyOf/oneOf/allOf property: its first typed branch.

    Null-only and untyped branches (a ``$ref``, a nested choice, ``{}``) are
    skipped; with no typed branch left, the column is ``UNTYPED_COLUMN``. The
    column's type is null followed by the branch's types in their declared
    order, as for a plain type list, so it is the same in every process.
    """
    branches = prop[keyword] if isinstance(prop[keyword], list) else []
    branch = next((b for b in branches if _non_null_types(b)), None)
    if branch is None:
        return UNTYPED_COLUMN
    column = {**branch, "type": ["null", *_non_null_types(branch)]}
    return _with_wrapper_hint(prop, column)


def _with_wrapper_hint(prop: dict, branch: dict) -> dict:
    """Carry a choice property's own x-sql-datatype hint into its chosen branch.

    The hint describes the column whichever branch is chosen; a hint on the
    branch itself wins, and an empty one counts as no hint. Only the hint is
    carried over, so no other key on the wrapper (such as ``format``) changes
    the column's type.
    """
    if sql_datatype_hint(prop) and not sql_datatype_hint(branch):
        return {**branch, "x-sql-datatype": prop["x-sql-datatype"]}
    return branch


# pylint: disable=dangerous-default-value,invalid-name
def flatten_schema(
    d, parent_key=[], sep="__", level=0, max_level=0, auto_cast_timestamps=False
):
    """This function is a bit of a blackbox. Adapted from DuckDB target and Meltano SDK.
    See note mentioned here in official Meltano SDK repo:
    https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342
    """
    items = []

    if "properties" not in d:
        return {}

    for k, prop in d["properties"].items():
        new_key = flatten_key(k, parent_key, sep)
        if not isinstance(prop, dict):  # a boolean schema: true allows any value
            items.append((new_key, UNTYPED_COLUMN))
            continue
        # some schemas from airbyte sources have description field which is not needed and causes issues
        # (left out of a copy, so the caller's schema keeps it)
        v = {key: value for key, value in prop.items() if key != "description"}

        if "type" in v.keys():
            if "object" in v["type"] and "properties" in v and level < max_level:
                items.extend(
                    flatten_schema(
                        v,
                        parent_key + [k],
                        sep=sep,
                        level=level + 1,
                        max_level=max_level,
                        auto_cast_timestamps=auto_cast_timestamps,
                    ).items()
                )
            # certain taps (e.g. postgres) sometimes return a list of types for JSON columns
            # we need to handle this specific case, if we don't the schema returns VARCHAR
            # may need to revisit this for other types
            elif "array" in v["type"]:
                items.append((new_key, {"type": ["null", "array"]}))
            elif "object" in v["type"]:
                items.append((new_key, {"type": ["null", "object"]}))
            # Apply timestamp auto-casting if applicable
            elif _should_auto_cast_to_timestamp(new_key, v, auto_cast_timestamps):
                items.append((new_key, _apply_timestamp_format(v)))
            else:
                items.append((new_key, v))
        elif keyword := next((kw for kw in _CHOICE_KEYWORDS if kw in v), None):
            items.append((new_key, _choice_column(v, keyword)))
        else:  # no type information at all, e.g. {}
            items.append((new_key, UNTYPED_COLUMN))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError(f"Duplicate column name produced in schema: {k}")

    # A deep copy, so editing a column never reaches the caller's schema or
    # UNTYPED_COLUMN.
    return copy.deepcopy(dict(sorted_items))


def nullable_choices(schema: dict) -> dict:
    """Return a copy of a stream schema in which anyOf/oneOf properties accept null.

    Every DuckLake column this target creates is nullable, and record
    validation has always accepted null for most of these properties, so a tap
    that sends null where its choice list doesn't declare it still loads. Any
    property with anyOf or oneOf accepts null, whatever else it declares, at
    any depth of nested object properties. Only null is added; other values
    validate as before. The given schema is not changed.
    """
    if "properties" not in schema:
        return schema
    properties = {}
    for name, prop in schema["properties"].items():
        if isinstance(prop, dict) and ("anyOf" in prop or "oneOf" in prop):
            properties[name] = {"anyOf": [prop, {"type": "null"}]}
        elif isinstance(prop, dict):
            properties[name] = nullable_choices(prop)
        else:
            properties[name] = prop
    return {**schema, "properties": properties}


def truncate_column_names(
    schema: dict, max_length: int = 63
) -> tuple[dict, dict[str, str]]:
    """Truncate schema column names to max_length, resolving collisions.

    PostgreSQL truncates identifiers to 63 characters (NAMEDATALEN).
    This function applies the same truncation to avoid DuckLake binder errors
    when using a Postgres catalog.

    Returns (new_schema, old_to_new_name_mapping).
    The mapping only includes names that actually changed.

    Collision resolution: when multiple columns map to the same truncated name,
    each is shortened further and given a ``_{i}`` suffix (0-indexed).
    """
    if max_length <= 0:
        return schema, {}

    # First pass: truncate names that exceed max_length
    truncated: dict[str, str] = {}
    for name in schema:
        truncated[name] = name[:max_length] if len(name) > max_length else name

    # Detect collisions
    seen: dict[str, list[str]] = {}
    for original, trunc in truncated.items():
        seen.setdefault(trunc, []).append(original)

    # Resolve collisions with _{i} suffixes
    mapping: dict[str, str] = {}
    for trunc, originals in seen.items():
        if len(originals) == 1:
            original = originals[0]
            if trunc != original:
                mapping[original] = trunc
        else:
            for i, original in enumerate(originals):
                suffix = f"_{i}"
                new_name = trunc[: max_length - len(suffix)] + suffix
                mapping[original] = new_name

    # Build new schema preserving insertion order
    new_schema = {mapping.get(name, name): value for name, value in schema.items()}
    return new_schema, mapping


# pylint: disable=redefined-outer-name
def _should_json_dump_value(key, value, flatten_schema=None):
    """Adapted from here: https://github.com/jwills/target-duckdb/blob/36c8ce68a0b2584c4bbb07325482968b1edc0c40/target_duckdb/db_sync.py#L125
    """
    if isinstance(value, (dict, list)):
        return True

    if flatten_schema and key in flatten_schema and "type" in flatten_schema[key]:
        t = flatten_schema[key]["type"]
        t = [t] if isinstance(t, str) else t
        # If array OR object type, return True. We need to JSON dump these values.
        if "array" in t or "object" in t:
            return True

    return False
