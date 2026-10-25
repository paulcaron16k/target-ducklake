"""Per-stream table layout: partition keys and sort order.

The config shape follows target-iceberg's, so xflow can configure both targets
the same way. ``stream_defaults`` applies to every stream and
``stream_overrides.<stream>`` is merged over it, key by key::

    stream_defaults:
      sort_order: [updated_at]
    stream_overrides:
      orders:
        partition:
          - {column: occurred_at, transform: day}
          - {column: tenant_id, transform: bucket, num_buckets: 16}
        sort_order: [customer_id, "occurred_at DESC"]

Unlike target-iceberg, which fixes the partition spec at table creation and
doesn't apply ``sort_order``, DuckLake changes partitioning and sort order as
metadata (new files follow the new layout), so a changed layout is applied to
an existing table. An unchanged one is left alone: re-applying it would add a
snapshot every run.

The older ``partition_fields`` setting still works for a stream with no
``partition`` in the new shape.
"""

from __future__ import annotations

import functools
import json
import re
import typing as t
from dataclasses import dataclass
from pathlib import Path

if t.TYPE_CHECKING:
    import logging
    from collections.abc import Mapping, Sequence

__all__ = [
    "LayoutError",
    "PartitionKey",
    "SortKey",
    "TableLayout",
    "check_layout_config",
    "read_table_config",
    "resolve_layout",
]

# DuckLake 1.5.2's partition transforms. target-iceberg also offers truncate and
# void, which DuckLake rejects ("Unsupported partition function truncate").
_TIME_TRANSFORMS = ("year", "month", "day", "hour")
TRANSFORMS = frozenset({"identity", *_TIME_TRANSFORMS, "bucket"})
_ICEBERG_ONLY_TRANSFORMS = frozenset({"truncate", "void"})
_PARTITION_KEYS = frozenset({"column", "transform", "num_buckets", "width", "name"})
_LAYOUT_KEYS = frozenset({"partition", "sort_order"})
# Characters target-iceberg's conform_name strips from identifiers.
_NOT_KEPT = re.compile(r"[^a-zA-Z0-9_\-\.\s]")
# target-iceberg's per-stream settings. Those that only configure Iceberg are
# ignored, so a config shared with target-iceberg works; those that change
# what data is loaded are rejected until implemented, never ignored.
_ICEBERG_ONLY_LAYOUT_KEYS = frozenset(
    {"delete_strategy", "merge_on_read_maintenance_acknowledged"}
)
_UNIMPLEMENTED_LAYOUT_KEYS = {
    "write_mode": "DUCKLKD-41",
    "key_properties": "DUCKLKD-41",
    "type_source": "DUCKLKD-43",
}
# target-iceberg's on_error applies only to type_source columns, so without
# type_source it changes nothing; its schema injects on_error: raise into
# every stream_defaults. With type_source it is rejected along with it.
# Revisit when DUCKLKD-43 implements type_source.
_TYPE_SOURCE_ONLY_KEY = "on_error"

# The Zamboni table-config version read here. tests/contracts holds Zamboni's
# published spec, pinned by bin/refresh-table-config-contract.sh, and the
# contract tests fail if it no longer matches the names below.
TABLE_CONFIG_VERSION = 2
_TABLE_CONFIG_ROOT_KEYS = frozenset({"version", "warehouse", "defaults", "namespaces"})
# The keys of a table block (and of defaults): those applied, and those that
# only configure Zamboni's maintenance.
_TABLE_CONFIG_APPLIED_KEYS = frozenset({"partition", "ordering"})
_TABLE_CONFIG_IGNORED_KEYS = frozenset(
    {
        "description",
        "min_input_files",
        "partition_evolution",
        "retention",
        "skip_partitions_newer_than_windows",
        "target_file_size_bytes",
    }
)
_ORDERING_MODES = frozenset({"none", "sort", "zorder"})
_ORDERING_SORT_KEYS = frozenset({"column", "direction", "nulls"})
_SORT_DIRECTIONS = frozenset({"asc", "desc"})
_SORT_NULLS = frozenset({"first", "last"})

# "column", or "column DESC", "column ASC NULLS FIRST": the sort entry forms.
_SORT_ENTRY = re.compile(
    r'^\s*(?P<column>"[^"]+"|[^\s"]+)'
    r"(?:\s+(?P<direction>asc|desc))?"
    r"(?:\s+nulls\s+(?P<nulls>first|last))?\s*$",
    re.IGNORECASE,
)


class LayoutError(ValueError):
    """A table layout setting that can't be applied; the message names the fix."""


@dataclass(frozen=True)
class PartitionKey:
    """One partition key: a column and its transform (``bucket(16)`` for buckets)."""

    column: str
    transform: str

    def sql(self) -> str:
        """The key as a DuckLake ``SET PARTITIONED BY`` expression."""
        column = _quoted(self.column)
        if self.transform == "identity":
            return column
        if self.transform.startswith("bucket("):
            buckets = self.transform[len("bucket(") : -1]
            return f"bucket({buckets}, {column})"
        return f"{self.transform}({column})"

    def compare_key(self) -> tuple[str, ...]:
        """The key's identity, with the column name compared case-insensitively."""
        return (self.column.lower(), self.transform)


@dataclass(frozen=True)
class SortKey:
    """One sort key: a column, ``ASC``/``DESC`` and ``NULLS_FIRST``/``NULLS_LAST``."""

    column: str
    direction: str = "ASC"
    nulls: str = "NULLS_LAST"

    def sql(self) -> str:
        """The key as a DuckLake ``SET SORTED BY`` expression."""
        return f"{_quoted(self.column)} {self.direction} {self.nulls.replace('_', ' ')}"

    def compare_key(self) -> tuple[str, ...]:
        """The key's identity, with the column name compared case-insensitively."""
        return (self.column.lower(), self.direction, self.nulls)


@dataclass(frozen=True)
class TableLayout:
    """A table's partition keys and sort order; ``None`` means not configured."""

    partition: tuple[PartitionKey, ...] | None = None
    sort: tuple[SortKey, ...] | None = None


def _quoted(column: str) -> str:
    return '"' + column.replace('"', '""') + '"'


def resolve_layout(  # noqa: PLR0913 (the last two are optional stream context)
    config: Mapping[str, t.Any],
    stream_name: str,
    columns: Sequence[str],
    logger: logging.Logger | None = None,
    *,
    table: tuple[str, str] | None = None,
    aliases: Mapping[str, str] | None = None,
) -> TableLayout:
    """Resolve and validate one stream's layout from the target config.

    The partition keys come from the stream's ``partition``, else the
    ``table_config`` file's, else the older ``partition_fields``. The sort
    order comes from the stream's ``sort_order``, else the file's ``ordering``
    when its mode is ``sort``.

    Args:
        config: The target config.
        stream_name: The stream (the key of ``stream_overrides`` and of the
            older ``partition_fields``).
        columns: The stream's column names, as the table has them.
        logger: Used to report which source a setting came from when two
            declare it. Iceberg-only settings are reported once, at startup,
            by ``check_layout_config``.
        table: ``(schema, table)``, how ``table_config`` keys its tables.
        aliases: Configured column names mapped to the table's, for columns
            whose names were truncated (Postgres catalogs cap them at 63).

    Returns:
        The stream's layout.

    Raises:
        LayoutError: If a setting is unknown, malformed, or names a column the
            stream doesn't have. Every rejection names the fix: a layout typo
            silently ignored would leave the table laid out differently than
            intended.
    """
    where = f"stream {stream_name!r}"
    layout = _stream_layout(config, stream_name, where)
    by_name = {column.lower(): column for column in columns}
    for name, column in (aliases or {}).items():
        by_name.setdefault(name.lower(), column)
    block = _table_config_block(config, table, logger)
    file_where = f"table_config {'.'.join(table or ())}"

    partition = None
    legacy = (config.get("partition_fields") or {}).get(stream_name)
    sources = [
        name
        for name, present in (
            ("partition", layout.get("partition") is not None),
            ("table_config", "partition" in block),
            ("partition_fields", bool(legacy)),
        )
        if present
    ]
    if len(sources) > 1 and logger is not None:
        logger.info(
            "%s declare a partition for %s; using %s.",
            " and ".join(sources),
            stream_name,
            sources[0],
        )
    if layout.get("partition") is not None:
        partition = _partition_keys(layout["partition"], by_name, where, None)
    elif "partition" in block:
        partition = _partition_keys(block["partition"] or [], by_name, file_where, None)
    elif legacy:
        partition = _legacy_partition_keys(legacy, by_name, where)

    sort = None
    if layout.get("sort_order") is not None:
        sort = _sort_keys(layout["sort_order"], by_name, where)
    elif "ordering" in block:
        sort = _ordering_sort_keys(block["ordering"], by_name, file_where, None)
    return TableLayout(partition=partition, sort=sort)


def check_layout_config(
    config: Mapping[str, t.Any], logger: logging.Logger | None = None
) -> None:
    """Check every stream's and every table's layout settings, without columns.

    Run at startup, so a typo in the override of a stream whose records never
    arrive is reported too, as target-iceberg does, and Iceberg-only settings
    are reported once. Column names are checked per stream by
    ``resolve_layout``, once the stream's schema is known.

    Raises:
        LayoutError: As ``resolve_layout``, except for unknown columns, and if
            ``table_config`` can't be read.
    """
    overrides = config.get("stream_overrides") or {}
    if not isinstance(overrides, dict):
        msg = "stream_overrides must map stream names to layout settings."
        raise LayoutError(msg)
    for stream_name in ["", *overrides]:
        where = f"stream {stream_name!r}" if stream_name else "stream_defaults"
        layout = _stream_layout(config, stream_name, where)
        if stream_name:
            own = overrides.get(stream_name) or {}
        else:
            own = config.get("stream_defaults") or {}
        _report_ignored(own, where, logger)
        if layout.get("partition") is not None:
            _partition_keys(layout["partition"], None, where, logger)
        if layout.get("sort_order") is not None:
            _sort_keys(layout["sort_order"], None, where)
    if config.get("table_config"):
        _check_table_config(config, logger)


def _check_table_config(
    config: Mapping[str, t.Any], logger: logging.Logger | None
) -> None:
    """Check the layout sections of every table block in ``table_config``.

    Keys target-ducklake doesn't know are reported, not rejected: Zamboni
    validates the file, and a key it adds for maintenance shouldn't stop
    loads. The contract tests catch new keys when the spec is refreshed.
    """
    document = read_table_config(config["table_config"])
    unknown = sorted(set(document) - _TABLE_CONFIG_ROOT_KEYS)
    if logger is not None and unknown:
        logger.info("table_config: ignoring unknown key(s) %s.", unknown)
    known = _TABLE_CONFIG_APPLIED_KEYS | _TABLE_CONFIG_IGNORED_KEYS
    for table, block in _table_config_blocks(config):
        where = f"table_config {table}"
        if logger is not None and (unknown := sorted(set(block) - known)):
            logger.info("%s: ignoring unknown key(s) %s.", where, unknown)
        if block.get("partition"):
            _partition_keys(block["partition"], None, where, logger)
        if "ordering" in block:
            _ordering_sort_keys(block["ordering"], None, where, logger)


def _report_ignored(
    own: Mapping[str, t.Any], where: str, logger: logging.Logger | None
) -> None:
    """Report the ignored settings a stream_defaults/override entry sets itself.

    Reported where they are set, not again for every override inheriting them.
    """
    if logger is None:
        return
    if ignored := sorted(set(own) & _ICEBERG_ONLY_LAYOUT_KEYS):
        logger.info(
            "%s: ignoring the Iceberg-only setting(s) %s. DuckLake deletes always "
            "write delete files (merge-on-read); compaction is maintenance.",
            where,
            ignored,
        )
    if _TYPE_SOURCE_ONLY_KEY in own and "type_source" not in own:
        logger.info(
            "%s: ignoring 'on_error', which applies only to type_source columns.",
            where,
        )


def _stream_layout(
    config: Mapping[str, t.Any], stream_name: str, where: str
) -> dict[str, t.Any]:
    """``stream_defaults`` with this stream's ``stream_overrides`` merged over it.

    Iceberg-only settings are dropped (``check_layout_config`` reports them);
    settings that would change the data loaded but aren't implemented are
    rejected, as are unknown ones.
    """
    defaults = config.get("stream_defaults") or {}
    override = (config.get("stream_overrides") or {}).get(stream_name) or {}
    if not isinstance(defaults, dict) or not isinstance(override, dict):
        msg = (
            f"{where}: stream_defaults and each stream_overrides entry must be "
            "mappings."
        )
        raise LayoutError(msg)
    layout = {**defaults, **override}
    if "type_source" not in layout:
        layout.pop(_TYPE_SOURCE_ONLY_KEY, None)
    if unimplemented := sorted(set(layout) & set(_UNIMPLEMENTED_LAYOUT_KEYS)):
        issues = sorted({_UNIMPLEMENTED_LAYOUT_KEYS[key] for key in unimplemented})
        msg = (
            f"{where}: {unimplemented} would change the data loaded, and "
            f"target-ducklake doesn't implement them yet ({', '.join(issues)}). "
            "Rejected rather than ignored: target-iceberg ignores write_mode, and "
            "silently appends where upsert was meant."
        )
        raise LayoutError(msg)
    if unknown := sorted(
        set(layout) - _LAYOUT_KEYS - _ICEBERG_ONLY_LAYOUT_KEYS - {_TYPE_SOURCE_ONLY_KEY}
    ):
        msg = (
            f"{where}: unknown stream_defaults/stream_overrides setting(s) "
            f"{unknown}. target-ducklake applies {sorted(_LAYOUT_KEYS)} and ignores "
            f"the Iceberg-only {sorted(_ICEBERG_ONLY_LAYOUT_KEYS)}."
        )
        raise LayoutError(msg)
    return {key: value for key, value in layout.items() if key in _LAYOUT_KEYS}


def read_table_config(path: str) -> dict[str, t.Any]:
    """Read a Zamboni ``table-config.json`` (cached per path).

    Raises:
        LayoutError: If the file is missing or isn't a JSON object.
    """
    return dict(_read_table_config(str(path)))


@functools.lru_cache(maxsize=8)
def _read_table_config(path: str) -> dict[str, t.Any]:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        msg = f"table_config: no such file {path!r}."
        raise LayoutError(msg) from None
    except json.JSONDecodeError as error:
        msg = f"table_config: {path!r} is not valid JSON ({error})."
        raise LayoutError(msg) from None
    if not isinstance(document, dict):
        msg = f"table_config: {path!r} must hold a JSON object."
        raise LayoutError(msg)
    _check_table_config_version(document, path)
    return document


def _check_table_config_version(document: Mapping[str, t.Any], path: str) -> None:
    """Refuse a file in a format other than ``TABLE_CONFIG_VERSION``, as Zamboni does.

    A file Zamboni refuses isn't a shared config, so it isn't loaded here
    either. The version 1 shape (tables keyed 'namespace.table') is named.
    """
    if "tables" in document and "namespaces" not in document:
        msg = (
            f"table_config: {path!r} has 'tables' at the top level, the version 1 "
            "shape, which Zamboni no longer reads. Version 2 states the namespace: "
            "{'version': 2, 'warehouse': ..., 'namespaces': {'<namespace>': "
            "{'tables': {'<table>': {...}}}}}."
        )
        raise LayoutError(msg)
    version = document.get("version", TABLE_CONFIG_VERSION)
    if type(version) is not int or version != TABLE_CONFIG_VERSION:
        msg = (
            f"table_config: {path!r} has version {version!r}; target-ducklake reads "
            f"version {TABLE_CONFIG_VERSION}."
        )
        raise LayoutError(msg)


def _mapping(value: object, what: str) -> dict[str, t.Any]:
    """``value`` as a mapping (``{}`` for null), or a LayoutError naming ``what``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        msg = f"table_config: {what} must be an object, got {type(value).__name__}."
        raise LayoutError(msg)
    return value


def _file_tables(
    document: Mapping[str, t.Any],
) -> dict[tuple[str, str], dict[str, t.Any]]:
    """Every table block of the file by (schema, table), shapes checked.

    Tables are nested under namespaces; the namespace is the target schema.
    """
    tables: dict[tuple[str, str], dict[str, t.Any]] = {}
    namespaces = _mapping(document.get("namespaces"), "namespaces")
    for namespace, raw_entry in namespaces.items():
        entry = _mapping(raw_entry, f"namespaces.{namespace}")
        entry_tables = _mapping(entry.get("tables"), f"namespaces.{namespace}.tables")
        for name, block in entry_tables.items():
            tables[namespace, name] = _mapping(
                block, f"namespaces.{namespace}.tables.{name}"
            )
    return tables


def _table_config_blocks(
    config: Mapping[str, t.Any],
) -> list[tuple[str, dict[str, t.Any]]]:
    """Every table block of the file, merged over ``defaults``, by 'schema.table'.

    Includes ``defaults`` itself, so a malformed default is reported too.
    """
    document = read_table_config(config["table_config"])
    defaults = _mapping(document.get("defaults"), "defaults")
    blocks = [("defaults", defaults)]
    blocks += [
        (f"{schema}.{name}", {**defaults, **block})
        for (schema, name), block in _file_tables(document).items()
    ]
    return blocks


def _conformed(name: str) -> str:
    """A name conformed as target-iceberg (and the SQL SDK) conforms it."""
    conformed = _NOT_KEPT.sub("", name).lower().strip()
    conformed = re.sub(r"[.\- ]", "_", conformed)
    if conformed and conformed[0].isdigit():
        conformed = "abcdefghij"[int(conformed[0])] + conformed[1:]
    return conformed


def _table_config_block(
    config: Mapping[str, t.Any],
    table: tuple[str, str] | None,
    logger: logging.Logger | None = None,
) -> dict[str, t.Any]:
    """One table's block of the file, merged over ``defaults``; ``{}`` if unset.

    As in Zamboni, a key the table's block contains wins, whatever its value,
    and lists replace whole. The table is matched case-insensitively, as
    DuckDB matches identifiers, then by target-iceberg's conformed name (a
    file written for target-iceberg names ``Orders Daily`` ``orders_daily``).
    A table with no block takes ``defaults``, which is logged.
    """
    if not config.get("table_config") or table is None:
        return {}
    document = read_table_config(config["table_config"])
    defaults = _mapping(document.get("defaults"), "defaults")
    tables = _file_tables(document)
    schema, name = table
    block = None
    for key_of in (str.lower, _conformed):
        wanted = (key_of(schema), key_of(name))
        matches = [
            (key, block)
            for key, block in tables.items()
            if (key_of(key[0]), key_of(key[1])) == wanted
        ]
        if matches:
            if len(matches) > 1 and logger is not None:
                logger.info(
                    "table_config has %d blocks matching %s.%s; using %s.",
                    len(matches),
                    schema,
                    name,
                    ".".join(matches[0][0]),
                )
            block = matches[0][1]
            break
    if block is None and logger is not None:
        logger.info(
            "table_config has no block for %s.%s; using its defaults.", schema, name
        )
    return {**defaults, **(block or {})}


def _column(name: object, by_name: Mapping[str, str] | None, at: str) -> str:
    """The table's name for a configured column (any name, without columns)."""
    if not isinstance(name, str) or not name:
        msg = f"{at}: a column name is required."
        raise LayoutError(msg)
    if by_name is None:
        return name.strip('"')
    column = by_name.get(name.strip('"').lower())
    if column is None:
        msg = (
            f"{at}: column {name!r} is not in this stream's columns "
            f"{sorted(by_name.values())}. Use the column name as the table has it "
            "(flattened names use '__')."
        )
        raise LayoutError(msg)
    return column


def _partition_keys(
    entries: object,
    by_name: Mapping[str, str] | None,
    where: str,
    logger: logging.Logger | None,
) -> tuple[PartitionKey, ...] | None:
    """The keys of a partition list; ``None`` if only Iceberg-only entries were given.

    An entry with an Iceberg-only transform (truncate, void) is skipped and
    reported, given a logger. If that leaves no key from a non-empty list, the
    partition counts as not configured, so a table's existing partitioning is
    not reset by accident; an explicitly empty list still resets it.
    """
    if not isinstance(entries, list):
        msg = f"{where}: partition must be a list of {{column, transform}} entries."
        raise LayoutError(msg)
    keys = []
    for index, entry in enumerate(entries):
        at = f"{where} partition[{index}]"
        if not isinstance(entry, dict):
            msg = f"{at}: each entry must be a mapping, e.g. {{column: ts}}."
            raise LayoutError(msg)
        unknown = sorted(set(entry) - _PARTITION_KEYS)
        if unknown:
            msg = (
                f"{at}: unknown key(s) {unknown}. "
                f"Valid keys: {sorted(_PARTITION_KEYS)}."
            )
            raise LayoutError(msg)
        transform = _partition_transform(entry, at)
        if transform is None:
            if logger is not None:
                logger.info(
                    "%s: skipping the Iceberg-only transform %r; DuckLake offers %s.",
                    at,
                    entry.get("transform"),
                    sorted(TRANSFORMS),
                )
            continue
        # Checked after the transform, as a skipped entry's column doesn't matter.
        column = _column(entry.get("column"), by_name, at)
        if "name" in entry and logger is not None:
            logger.info(
                "%s: ignoring 'name', which names an Iceberg partition field; "
                "DuckLake partition keys have no name.",
                at,
            )
        keys.append(PartitionKey(column, transform))
    if entries and not keys:
        return None
    return tuple(keys)


def _partition_transform(entry: Mapping[str, t.Any], at: str) -> str | None:
    """An entry's transform: ``identity``, a time transform, or ``bucket(N)``.

    ``None`` for an Iceberg-only transform (truncate, void), which is skipped.
    """
    # Exact, as in Zamboni's spec and target-iceberg: "Day" is refused.
    transform = str(entry.get("transform", "identity"))
    if transform in _ICEBERG_ONLY_TRANSFORMS:
        return None
    if transform not in TRANSFORMS:
        msg = f"{at}: unknown transform {transform!r}. Supported: {sorted(TRANSFORMS)}."
        raise LayoutError(msg)
    if "width" in entry:
        msg = f"{at}: 'width' applies only to the truncate transform."
        raise LayoutError(msg)
    if transform != "bucket":
        if "num_buckets" in entry:
            msg = f"{at}: 'num_buckets' applies only to the bucket transform."
            raise LayoutError(msg)
        return transform
    buckets = entry.get("num_buckets")
    if not isinstance(buckets, int) or isinstance(buckets, bool) or buckets < 1:
        msg = f"{at}: transform 'bucket' requires a positive integer 'num_buckets'."
        raise LayoutError(msg)
    return f"bucket({buckets})"


def _legacy_partition_keys(
    entries: Sequence[Mapping[str, t.Any]], by_name: Mapping[str, str], where: str
) -> tuple[PartitionKey, ...]:
    """Keys from the older ``partition_fields`` form."""
    keys = []
    for index, entry in enumerate(entries):
        at = f"{where} partition_fields[{index}]"
        column = _column(entry.get("column_name"), by_name, at)
        kind = entry.get("type")
        if kind == "identifier":
            keys.append(PartitionKey(column, "identity"))
        elif kind == "timestamp":
            if not entry.get("granularity"):
                # Expanding to no key would reset the table's partitioning.
                msg = f"{at}: a timestamp entry needs at least one granularity."
                raise LayoutError(msg)
            for granularity in entry["granularity"]:
                if granularity not in _TIME_TRANSFORMS:
                    msg = f"{at}: unsupported granularity {granularity!r}."
                    raise LayoutError(msg)
                keys.append(PartitionKey(column, granularity))
        else:
            msg = f"{at}: unsupported partition type {kind!r}."
            raise LayoutError(msg)
    return tuple(keys)


def _sort_keys(
    entries: object, by_name: Mapping[str, str] | None, where: str
) -> tuple[SortKey, ...]:
    if not isinstance(entries, list):
        msg = f"{where}: sort_order must be a list of column names."
        raise LayoutError(msg)
    keys = []
    for index, entry in enumerate(entries):
        at = f"{where} sort_order[{index}]"
        match = _SORT_ENTRY.match(entry) if isinstance(entry, str) else None
        if match is None:
            msg = (
                f"{at}: {entry!r} is not a sort entry. Use a column name, optionally "
                "followed by ASC or DESC and NULLS FIRST or NULLS LAST."
            )
            raise LayoutError(msg)
        column = _column(match["column"], by_name, at)
        direction = (match["direction"] or "asc").upper()
        nulls = f"NULLS_{(match['nulls'] or 'last').upper()}"
        keys.append(SortKey(column, direction, nulls))
    return tuple(keys)


def _ordering_sort_keys(
    ordering: object,
    by_name: Mapping[str, str] | None,
    where: str,
    logger: logging.Logger | None,
) -> tuple[SortKey, ...] | None:
    """The sort keys of a table-config ``ordering``; ``None`` for z-order.

    ``mode: sort`` gives the keys (``column``, ``direction`` asc/desc,
    ``nulls`` first/last); ``mode: none``, the default as in Zamboni, gives
    no keys, removing a sort order; ``mode: zorder`` is Iceberg-only and
    skipped (reported, given a logger).
    """
    mode, sort = _ordering_mode(ordering, where)
    if mode == "none":
        return ()
    if mode == "zorder":
        if logger is not None:
            logger.info(
                "%s: skipping ordering mode 'zorder', which DuckLake lacks.", where
            )
        return None
    keys = []
    for index, entry in enumerate(sort):
        at = f"{where} ordering.sort[{index}]"
        if not isinstance(entry, dict):
            msg = f"{at}: each entry must be a mapping, e.g. {{column: ts}}."
            raise LayoutError(msg)
        if unknown := sorted(set(entry) - _ORDERING_SORT_KEYS):
            msg = (
                f"{at}: unknown key(s) {unknown}. "
                f"Valid keys: {sorted(_ORDERING_SORT_KEYS)}."
            )
            raise LayoutError(msg)
        column = _column(entry.get("column"), by_name, at)
        direction = entry.get("direction", "asc")
        nulls = entry.get("nulls", "last")
        if not _one_of(direction, _SORT_DIRECTIONS) or not _one_of(nulls, _SORT_NULLS):
            msg = f"{at}: direction must be asc or desc, and nulls first or last."
            raise LayoutError(msg)
        keys.append(SortKey(column, direction.upper(), f"NULLS_{nulls.upper()}"))
    return tuple(keys)


def _ordering_mode(ordering: object, where: str) -> tuple[str, list[t.Any]]:
    """The mode and sort list of an ``ordering``, checked as Zamboni checks them."""
    if not isinstance(ordering, dict):
        msg = f"{where}: ordering must be an object with a mode."
        raise LayoutError(msg)
    mode = ordering.get("mode", "none")
    if not _one_of(mode, _ORDERING_MODES):
        msg = f"{where}: ordering mode must be one of {sorted(_ORDERING_MODES)}."
        raise LayoutError(msg)
    sort, zorder = ordering.get("sort"), ordering.get("zorder")
    # A zorder block counts when present, even empty, as in Zamboni.
    if mode == "none" and (sort or zorder is not None):
        msg = (
            f"{where}: ordering mode is none but an ordering is defined; set "
            "mode to sort or zorder to enable it, as Zamboni requires."
        )
        raise LayoutError(msg)
    if mode == "sort" and (not (isinstance(sort, list) and sort) or zorder is not None):
        msg = (
            f"{where}: ordering mode sort requires a non-empty sort list and no "
            "zorder block."
        )
        raise LayoutError(msg)
    if mode == "zorder" and (zorder is None or sort):
        msg = f"{where}: ordering mode zorder requires a zorder block and no sort list."
        raise LayoutError(msg)
    return mode, sort if isinstance(sort, list) else []


def _one_of(value: object, choices: frozenset[str]) -> t.TypeGuard[str]:
    """Whether ``value`` is one of the string ``choices`` (any JSON value)."""
    return isinstance(value, str) and value in choices
