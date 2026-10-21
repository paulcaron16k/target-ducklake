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

import re
import typing as t
from dataclasses import dataclass

if t.TYPE_CHECKING:
    import logging
    from collections.abc import Mapping, Sequence

__all__ = [
    "LayoutError",
    "PartitionKey",
    "SortKey",
    "TableLayout",
    "resolve_layout",
]

# DuckLake 1.5.2's partition transforms. target-iceberg also offers truncate and
# void, which DuckLake rejects ("Unsupported partition function truncate").
_TIME_TRANSFORMS = ("year", "month", "day", "hour")
TRANSFORMS = frozenset({"identity", *_TIME_TRANSFORMS, "bucket"})
_ICEBERG_ONLY_TRANSFORMS = frozenset({"truncate", "void"})
_PARTITION_KEYS = frozenset({"column", "transform", "num_buckets", "width", "name"})
_LAYOUT_KEYS = frozenset({"partition", "sort_order"})

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
) -> TableLayout:
    """Resolve and validate one stream's layout from the target config.

    Args:
        config: The target config.
        stream_name: The stream (the key of ``stream_overrides`` and of the
            older ``partition_fields``).
        columns: The stream's column names, as the table has them.
        logger: Used to report a ``partition`` that replaces
            ``partition_fields``, and a partition ``name``, which DuckLake
            doesn't use.

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

    partition = None
    legacy = (config.get("partition_fields") or {}).get(stream_name)
    if layout.get("partition") is not None:
        partition = _partition_keys(layout["partition"], by_name, where, logger)
        if legacy and logger is not None:
            logger.info(
                "Both partition and partition_fields are set for %s; using partition.",
                stream_name,
            )
    elif legacy:
        partition = _legacy_partition_keys(legacy, by_name, where)

    sort = None
    if layout.get("sort_order") is not None:
        sort = _sort_keys(layout["sort_order"], by_name, where)
    return TableLayout(partition=partition, sort=sort)


def _stream_layout(
    config: Mapping[str, t.Any], stream_name: str, where: str
) -> dict[str, t.Any]:
    """``stream_defaults`` with this stream's ``stream_overrides`` merged over it."""
    defaults = dict(config.get("stream_defaults") or {})
    overrides = dict(config.get("stream_overrides") or {})
    layout = {**defaults, **(overrides.get(stream_name) or {})}
    unknown = sorted(set(layout) - _LAYOUT_KEYS)
    if unknown:
        msg = (
            f"{where}: unsupported stream_defaults/stream_overrides setting(s) "
            f"{unknown}. target-ducklake supports {sorted(_LAYOUT_KEYS)}; per-stream "
            "write_mode and key_properties are not implemented (DUCKLKD-41). Rejected "
            "rather than ignored, so the stream isn't loaded differently than intended."
        )
        raise LayoutError(msg)
    return layout


def _column(name: object, by_name: Mapping[str, str], at: str) -> str:
    if not isinstance(name, str) or not name:
        msg = f"{at}: a column name is required."
        raise LayoutError(msg)
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
    by_name: Mapping[str, str],
    where: str,
    logger: logging.Logger | None,
) -> tuple[PartitionKey, ...]:
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
        column = _column(entry.get("column"), by_name, at)
        transform = _partition_transform(entry, at)
        if "name" in entry and logger is not None:
            logger.info(
                "%s: 'name' names an Iceberg partition field; DuckLake partition keys "
                "have no name, so it is not used.",
                at,
            )
        keys.append(PartitionKey(column, transform))
    return tuple(keys)


def _partition_transform(entry: Mapping[str, t.Any], at: str) -> str:
    """An entry's transform: ``identity``, a time transform, or ``bucket(N)``."""
    transform = str(entry.get("transform", "identity")).lower()
    if transform in _ICEBERG_ONLY_TRANSFORMS:
        msg = (
            f"{at}: transform {transform!r} is not supported by DuckLake, which "
            f"offers {sorted(TRANSFORMS)}."
        )
        raise LayoutError(msg)
    if transform not in TRANSFORMS:
        msg = f"{at}: unknown transform {transform!r}. Supported: {sorted(TRANSFORMS)}."
        raise LayoutError(msg)
    if "width" in entry:
        msg = (
            f"{at}: 'width' applies only to the truncate transform, which DuckLake "
            "lacks."
        )
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
            for granularity in entry.get("granularity") or []:
                if granularity not in _TIME_TRANSFORMS:
                    msg = f"{at}: unsupported granularity {granularity!r}."
                    raise LayoutError(msg)
                keys.append(PartitionKey(column, granularity))
        else:
            msg = f"{at}: unsupported partition type {kind!r}."
            raise LayoutError(msg)
    return tuple(keys)


def _sort_keys(
    entries: object, by_name: Mapping[str, str], where: str
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
