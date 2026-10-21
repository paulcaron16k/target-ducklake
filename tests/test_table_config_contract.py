"""target-ducklake's table_config reader against Zamboni's published contract.

Zamboni owns the table-config.json format. tests/contracts holds a pinned copy
of its spec and example file (bin/refresh-table-config-contract.sh), and these
tests check layout.py against it: the version it reads, and every key,
transform and ordering mode it applies or ignores. When the copy is refreshed,
a format change fails here, naming what moved, instead of surfacing as a
lookup that silently finds nothing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import jsonschema
import pytest

from target_ducklake.layout import (
    _ICEBERG_ONLY_TRANSFORMS,
    _ORDERING_MODES,
    _ORDERING_SORT_KEYS,
    _PARTITION_KEYS,
    _SORT_DIRECTIONS,
    _SORT_NULLS,
    _TABLE_CONFIG_APPLIED_KEYS,
    _TABLE_CONFIG_IGNORED_KEYS,
    _TABLE_CONFIG_ROOT_KEYS,
    TABLE_CONFIG_VERSION,
    TRANSFORMS,
    PartitionKey,
    SortKey,
    TableLayout,
    _read_table_config,
    check_layout_config,
    resolve_layout,
)

CONTRACTS = Path(__file__).parent / "contracts"
SPEC_PATH = CONTRACTS / "zamboni-table-config_spec.json"
EXAMPLE_PATH = CONTRACTS / "zamboni-table-config_example.json"


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    """The vendored spec."""
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def _table_block(spec: dict[str, Any]) -> dict[str, Any]:
    namespace = spec["properties"]["namespaces"]["additionalProperties"]
    return namespace["properties"]["tables"]["additionalProperties"]


def _enum(schema: dict[str, Any]) -> set[str]:
    return set(schema["enum"])


def test_the_vendored_spec_is_the_version_this_target_reads(spec: dict[str, Any]):
    """layout.py refuses other versions, so the pinned spec must be its version."""
    assert spec["properties"]["version"]["const"] == TABLE_CONFIG_VERSION


def test_the_root_and_namespace_keys_agree(spec: dict[str, Any]):
    """The file is warehouse -> namespaces -> tables, as _file_tables reads it."""
    assert set(spec["properties"]) == _TABLE_CONFIG_ROOT_KEYS
    namespace = spec["properties"]["namespaces"]["additionalProperties"]
    assert set(namespace["properties"]) == {"tables"}


def test_every_table_key_is_applied_or_ignored_by_name(spec: dict[str, Any]):
    """A new key needs a decision: apply it, or ignore it as maintenance-only."""
    assert not _TABLE_CONFIG_APPLIED_KEYS & _TABLE_CONFIG_IGNORED_KEYS
    known = _TABLE_CONFIG_APPLIED_KEYS | _TABLE_CONFIG_IGNORED_KEYS
    assert set(_table_block(spec)["properties"]) == known


def test_defaults_take_the_same_keys_as_a_table(spec: dict[str, Any]):
    """A table block is merged over defaults key by key, which needs one shape."""
    defaults = spec["properties"]["defaults"]
    assert defaults["properties"] == _table_block(spec)["properties"]


def test_the_partition_entries_agree(spec: dict[str, Any]):
    """Each transform is applied or skipped as Iceberg-only; the keys are read."""
    entry = _table_block(spec)["properties"]["partition"]["items"]
    assert set(entry["properties"]) == _PARTITION_KEYS
    assert not TRANSFORMS & _ICEBERG_ONLY_TRANSFORMS
    assert _enum(entry["properties"]["transform"]) == TRANSFORMS | (
        _ICEBERG_ONLY_TRANSFORMS
    )


def test_the_ordering_agrees(spec: dict[str, Any]):
    """The modes, sort entry keys, directions and nulls placements read."""
    ordering = _table_block(spec)["properties"]["ordering"]["properties"]
    assert set(ordering) == {"mode", "sort", "zorder"}
    assert _enum(ordering["mode"]) == _ORDERING_MODES
    entry = ordering["sort"]["items"]["properties"]
    assert set(entry) == _ORDERING_SORT_KEYS
    assert _enum(entry["direction"]) == _SORT_DIRECTIONS
    assert _enum(entry["nulls"]) == _SORT_NULLS


def _config(path: Path) -> dict[str, Any]:
    _read_table_config.cache_clear()
    return {"table_config": str(path)}


COLUMNS = ["occurred_at", "tenant_id", "actor_id", "customer_id", "id"]


def test_zamboni_s_example_file_loads():
    """The example Zamboni ships passes the startup check and resolves."""
    config = _config(EXAMPLE_PATH)
    logger = Mock()
    check_layout_config(config, logger)
    assert not [
        call for call in logger.info.call_args_list if "unknown" in call.args[0]
    ]
    events = resolve_layout(config, "e", COLUMNS, table=("analytics", "events"))
    assert events == TableLayout(
        partition=(
            PartitionKey("occurred_at", "day"),
            PartitionKey("tenant_id", "bucket(16)"),
        ),
        sort=None,
    )
    audit = resolve_layout(config, "a", COLUMNS, table=("analytics", "audit_log"))
    assert audit == TableLayout(
        partition=(PartitionKey("occurred_at", "day"),),
        sort=(SortKey("actor_id", "ASC", "NULLS_LAST"),),
    )
    assert (
        resolve_layout(config, "d", COLUMNS, table=("analytics", "dim_customer"))
        == TableLayout()
    )


def test_zamboni_s_example_satisfies_the_vendored_spec(spec: dict[str, Any]):
    """The two vendored files agree, so the pin took both from one ref."""
    example = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(spec).validate(example)
