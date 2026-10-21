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
    "on_error": "DUCKLKD-43",
}

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


def resolve_layout(
    config: Mapping[str, t.Any],
    stream_name: str,
    columns: Sequence[str],
    logger: logging.Logger | None = None,
    table: tuple[str, str] | None = None,
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
    block = _table_config_block(config, table)
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
        if (ignored := sorted(set(own) & _ICEBERG_ONLY_LAYOUT_KEYS)) and logger:
            # Reported where it is set, not again for every override inheriting it.
            logger.info(
                "%s: ignoring the Iceberg-only setting(s) %s. DuckLake deletes always "
                "write delete files (merge-on-read); compaction is maintenance.",
                where,
                ignored,
            )
        if layout.get("partition") is not None:
            _partition_keys(layout["partition"], None, where, logger)
        if layout.get("sort_order") is not None:
            _sort_keys(layout["sort_order"], None, where)
    if config.get("table_config"):
        _check_table_config(config, logger)


def _check_table_config(
    config: Mapping[str, t.Any], logger: logging.Logger | None
) -> None:
    """Check the layout sections of every table block in ``table_config``."""
    for table, block in _table_config_blocks(config):
        where = f"table_config {table}"
        if block.get("partition"):
            _partition_keys(block["partition"], None, where, logger)
        if "ordering" in block:
            _ordering_sort_keys(block["ordering"], None, where, logger)


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
    if unimplemented := sorted(set(layout) & set(_UNIMPLEMENTED_LAYOUT_KEYS)):
        issues = sorted({_UNIMPLEMENTED_LAYOUT_KEYS[key] for key in unimplemented})
        msg = (
            f"{where}: {unimplemented} would change the data loaded, and "
            f"target-ducklake doesn't implement them yet ({', '.join(issues)}). "
            "Rejected rather than ignored: target-iceberg ignores write_mode, and "
            "silently appends where upsert was meant."
        )
        raise LayoutError(msg)
    if unknown := sorted(set(layout) - _LAYOUT_KEYS - _ICEBERG_ONLY_LAYOUT_KEYS):
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
    return document


def _table_config_blocks(
    config: Mapping[str, t.Any],
) -> list[tuple[str, dict[str, t.Any]]]:
    """Every table block of the file, merged over ``defaults``, by 'schema.table'.

    Includes ``defaults`` itself, so a malformed default is reported too.
    """
    document = read_table_config(config["table_config"])
    defaults = document.get("defaults") or {}
    blocks = [("defaults", defaults)]
    if "namespaces" in document:  # version 2
        for namespace, entry in (document.get("namespaces") or {}).items():
            for name, block in ((entry or {}).get("tables") or {}).items():
                blocks.append((f"{namespace}.{name}", {**defaults, **(block or {})}))
    else:  # version 1, as target-iceberg reads it
        for key, block in (document.get("tables") or {}).items():
            blocks.append((key, {**defaults, **(block or {})}))
    return blocks


def _table_config_block(
    config: Mapping[str, t.Any], table: tuple[str, str] | None
) -> dict[str, t.Any]:
    """One table's block of the file, merged over ``defaults``; ``{}`` if unset.

    As in Zamboni, a key the table's block contains wins, whatever its value,
    and lists replace whole. Version 2 nests tables under namespaces (the
    namespace is the target schema); version 1 keys them 'namespace.table'.
    """
    if not config.get("table_config") or table is None:
        return {}
    document = read_table_config(config["table_config"])
    schema, name = table
    if "namespaces" in document:
        namespace = (document.get("namespaces") or {}).get(schema) or {}
        block = (namespace.get("tables") or {}).get(name)
    else:
        block = (document.get("tables") or {}).get(f"{schema}.{name}")
    return {**(document.get("defaults") or {}), **(block or {})}


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
    transform = str(entry.get("transform", "identity")).lower()
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
    ``nulls`` first/last); ``mode: none`` gives no keys, removing a sort
    order; ``mode: zorder`` is Iceberg-only and skipped (reported, given a
    logger).
    """
    if not isinstance(ordering, dict):
        msg = f"{where}: ordering must be an object with a mode."
        raise LayoutError(msg)
    mode = ordering.get("mode")
    if mode == "none":
        return ()
    if mode == "zorder":
        if logger is not None:
            logger.info(
                "%s: skipping ordering mode 'zorder', which DuckLake lacks.", where
            )
        return None
    if mode != "sort" or not isinstance(ordering.get("sort"), list):
        msg = f"{where}: ordering mode must be sort (with a sort list), zorder or none."
        raise LayoutError(msg)
    keys = []
    for index, entry in enumerate(ordering["sort"]):
        at = f"{where} ordering.sort[{index}]"
        if not isinstance(entry, dict):
            msg = f"{at}: each entry must be a mapping, e.g. {{column: ts}}."
            raise LayoutError(msg)
        column = _column(entry.get("column"), by_name, at)
        direction = str(entry.get("direction", "asc")).upper()
        nulls = str(entry.get("nulls", "last")).upper()
        if direction not in {"ASC", "DESC"} or nulls not in {"FIRST", "LAST"}:
            msg = f"{at}: direction must be asc or desc, and nulls first or last."
            raise LayoutError(msg)
        keys.append(SortKey(column, direction, f"NULLS_{nulls}"))
    return tuple(keys)
