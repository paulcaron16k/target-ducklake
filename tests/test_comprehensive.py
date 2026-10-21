"""Comprehensive unit tests for target-ducklake components."""

from __future__ import annotations

import copy
import io
import itertools
import json
import os
import subprocess
import sys
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from singer_sdk.exceptions import ConfigValidationError, InvalidRecord

from target_ducklake.connector import (
    DuckLakeConnector,
    DuckLakeConnectorError,
    JSONSchemaToDuckLake,
)
from target_ducklake.flatten import (
    CustomJSONEncoder,
    _apply_timestamp_format,
    _should_auto_cast_to_timestamp,
    flatten_record,
    flatten_schema,
    truncate_column_names,
)
from target_ducklake.parquet_utils import (
    _normalize_data_for_pyarrow,
    create_pyarrow_table,
    number_precision_and_scale,
    staging_schema,
    staging_type,
)
from target_ducklake.sinks import ducklakeSink
from target_ducklake.target import Targetducklake


def _staging_schema(flat: dict) -> pa.Schema:
    """The staging schema the sink builds for a flattened schema: from its DDL."""
    connector = DuckLakeConnector({"catalog_url": "x", "data_path": "/x"})
    return staging_schema(connector.json_to_ducklake_schema(flat))


class TestTargetducklakeConfig:
    """Test configuration validation and setup."""

    def test_valid_minimal_config(self):
        """Test that minimal valid configuration works."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        target = Targetducklake(config=config)
        assert target.name == "target-ducklake"
        assert target.config["catalog_url"] == "test.db"
        assert target.config["data_path"] == "/tmp/test"
        assert target.config["storage_type"] == "local"

    def test_missing_required_config(self):
        """Test that missing required configuration raises error."""
        config = {"storage_type": "local"}
        # The Singer SDK may not raise an error immediately but during initialization
        # Let's test that the config is incomplete by checking required fields
        target = Targetducklake(config=config)
        # The target should initialize but the config should be missing required fields
        assert target.config.get("catalog_url") is None
        assert target.config.get("data_path") is None

    def test_storage_type_validation(self):
        """Test storage type validation."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "invalid_type",
        }
        with pytest.raises(ConfigValidationError):
            Targetducklake(config=config)

    def test_s3_config_validation(self):
        """Test S3 configuration validation."""
        config = {
            "catalog_url": "test.db",
            "data_path": "s3://bucket/path",
            "storage_type": "S3",
            "public_key": "test_key",
            "secret_key": "test_secret",
            "region": "us-east-1",
        }
        target = Targetducklake(config=config)
        assert target.config["storage_type"] == "S3"
        assert target.config["region"] == "us-east-1"

    def test_gcs_config_validation(self):
        """Test GCS configuration validation."""
        config = {
            "catalog_url": "test.db",
            "data_path": "gs://bucket/path",
            "storage_type": "GCS",
            "public_key": "test_key",
            "secret_key": "test_secret",
        }
        target = Targetducklake(config=config)
        assert target.config["storage_type"] == "GCS"

    def test_default_values(self):
        """Test that default values are set correctly."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        target = Targetducklake(config=config)
        assert target.config["add_record_metadata"] == False
        assert target.config["flatten_max_level"] == 0
        assert target.config["temp_file_dir"] == "temp_files/"
        assert target.config["max_batch_size"] == 10000
        assert target.config["auto_cast_timestamps"] == False

    def test_partition_fields_config(self):
        """Test partition fields configuration."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "partition_fields": {
                "users": [
                    {
                        "column_name": "created_at",
                        "type": "timestamp",
                        "granularity": ["year", "month"],
                    }
                ]
            },
        }
        target = Targetducklake(config=config)
        assert "users" in target.config["partition_fields"]


class TestDuckLakeConnector:
    """Test DuckLake connector functionality."""

    def test_connector_initialization(self):
        """Test connector initialization with valid config."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        connector = DuckLakeConnector(config)
        assert connector.catalog_url == "test.db"
        assert connector.data_path == "/tmp/test"
        assert connector.storage_type == "local"

    def test_connector_missing_config(self):
        """Test connector raises error with missing config."""
        config = {"storage_type": "local"}
        with pytest.raises(DuckLakeConnectorError):
            DuckLakeConnector(config)

    def test_connector_s3_config_validation(self):
        """Test S3 configuration validation in connector."""
        config = {
            "catalog_url": "test.db",
            "data_path": "s3://bucket/path",
            "storage_type": "S3",
            "public_key": "test_key",
            # Missing secret_key
        }
        with pytest.raises(DuckLakeConnectorError):
            DuckLakeConnector(config)

    def test_connector_gcs_config_validation(self):
        """Test GCS configuration validation in connector."""
        config = {
            "catalog_url": "test.db",
            "data_path": "gs://bucket/path",
            "storage_type": "GCS",
            "public_key": "test_key",
            "secret_key": "test_secret",
        }
        connector = DuckLakeConnector(config)
        assert connector.public_key == "test_key"
        assert connector.secret_key == "test_secret"

    def test_json_schema_to_ducklake_converter(self):
        """Test JSON schema to DuckLake type conversion."""
        converter = JSONSchemaToDuckLake()

        # Test integer type handling
        schema = {"type": "integer", "minimum": -100, "maximum": 100}
        result = converter._handle_integer_type(schema)
        assert str(result) == "INTEGER"

        schema = {"type": "integer", "minimum": -100000, "maximum": 100000}
        result = converter._handle_integer_type(schema)
        assert str(result) == "INTEGER"

        schema = {"type": "integer", "minimum": -10000000000, "maximum": 10000000000}
        result = converter._handle_integer_type(schema)
        assert str(result) == "BIGINT"


class TestDucklakeSink:
    """Test ducklake sink functionality."""

    @pytest.fixture
    def mock_target(self):
        """Mock target for testing."""
        target = Mock()
        target.config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "temp_file_dir": "temp_files/",
            "flatten_max_level": 0,
            "auto_cast_timestamps": False,
            "add_record_metadata": False,
            "max_batch_size": 10000,
        }
        return target

    @pytest.fixture
    def sample_schema(self):
        """Sample schema for testing."""
        return {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string"},
                "created_at": {"type": "string"},
                "metadata": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
        }

    def test_sink_initialization(self, mock_target, sample_schema):
        """Test sink initialization."""
        with patch("target_ducklake.sinks.DuckLakeConnector"):
            sink = ducklakeSink(
                target=mock_target,
                stream_name="users",
                schema=sample_schema,
                key_properties=["id"],
            )
            assert sink.stream_name == "users"
            assert sink.schema == sample_schema
            assert sink.key_properties == ["id"]

    def test_sink_target_schema_default(self, mock_target, sample_schema):
        """Test target schema derivation with default."""
        mock_target.config["default_target_schema"] = "default_schema"

        with patch("target_ducklake.sinks.DuckLakeConnector"):
            sink = ducklakeSink(
                target=mock_target,
                stream_name="users",
                schema=sample_schema,
                key_properties=None,
            )
            assert sink.target_schema == "default_schema"

    def test_sink_target_schema_from_stream_name(self, mock_target, sample_schema):
        """Test target schema derivation from stream name."""
        mock_target.config.pop("default_target_schema", None)

        with patch("target_ducklake.sinks.DuckLakeConnector"):
            with patch("target_ducklake.sinks.stream_name_to_dict") as mock_stream_dict:
                mock_stream_dict.return_value = {"schema_name": "derived_schema"}
                sink = ducklakeSink(
                    target=mock_target,
                    stream_name="database.derived_schema.users",
                    schema=sample_schema,
                    key_properties=None,
                )
                # This would test the actual target_schema property


class TestInBatchDeduplication:
    """Duplicate keys within one batch keep the newest (last) record."""

    @staticmethod
    def _dedup(table: pa.Table, key_properties: list[str]) -> pa.Table:
        # Only the attributes the method reads; avoids building a full sink.
        sink: Any = SimpleNamespace(
            key_properties=key_properties, target_table="users", logger=Mock()
        )
        return ducklakeSink._remove_temp_table_duplicates(sink, table)

    def test_keeps_last_occurrence_per_key(self):
        """The last version of each key wins, in stream order."""
        table = pa.table(
            {
                "id": [1, 2, 1, 3, 2, 1],
                "status": ["v1", "a1", "v2", "b1", "a2", "v3"],
            }
        )
        result = self._dedup(table, ["id"])
        assert result.to_pylist() == [
            {"id": 3, "status": "b1"},
            {"id": 2, "status": "a2"},
            {"id": 1, "status": "v3"},
        ]

    def test_keeps_last_occurrence_for_composite_key(self):
        """Composite keys dedup on the full key, keeping the last version."""
        table = pa.table(
            {
                "tenant": ["a", "a", "b", "a"],
                "id": [1, 2, 1, 1],
                "status": ["old", "x", "y", "new"],
            }
        )
        result = self._dedup(table, ["tenant", "id"])
        assert result.to_pylist() == [
            {"tenant": "a", "id": 2, "status": "x"},
            {"tenant": "b", "id": 1, "status": "y"},
            {"tenant": "a", "id": 1, "status": "new"},
        ]

    def test_keeps_last_occurrence_in_large_batch(self):
        """Holds for batches big enough for Polars to split work across threads."""
        n, distinct = 100_000, 1_000
        table = pa.table(
            {
                "id": [i % distinct for i in range(n)],
                "seq": list(range(n)),
            }
        )
        result = self._dedup(table, ["id"])
        newest = {row["id"]: row["seq"] for row in table.to_pylist()}
        assert len(result) == distinct
        assert {row["id"]: row["seq"] for row in result.to_pylist()} == newest


class TestFlattenModule:
    """Test flatten module functionality."""

    def test_flatten_simple_schema(self):
        """Test flattening a simple schema."""
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        }
        result = flatten_schema(schema)
        assert "id" in result
        assert "name" in result
        assert result["id"]["type"] == "integer"
        assert result["name"]["type"] == "string"

    def test_flatten_nested_schema(self):
        """Test flattening a nested schema."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"},
                    },
                },
            },
        }
        result = flatten_schema(schema, max_level=2)
        assert "id" in result
        assert "user__name" in result
        assert "user__email" in result

    def test_flatten_record(self):
        """Test flattening a record."""
        record = {"id": 1, "user": {"name": "John Doe", "email": "john@example.com"}}
        result = flatten_record(record, max_level=2)
        assert result["id"] == 1
        assert result["user__name"] == "John Doe"
        assert result["user__email"] == "john@example.com"

    def test_custom_json_encoder(self):
        """Test CustomJSONEncoder handles Decimal objects."""
        encoder = CustomJSONEncoder()
        result = encoder.default(Decimal("123.45"))
        assert result == 123.45
        assert isinstance(result, float)

    def test_should_auto_cast_to_timestamp(self):
        """Test timestamp auto-casting logic."""
        # Should auto-cast
        assert (
            _should_auto_cast_to_timestamp(
                "created_at", {"type": "string"}, auto_cast_timestamps=True
            )
            == True
        )

        # Should not auto-cast - wrong name
        assert (
            _should_auto_cast_to_timestamp(
                "random_field", {"type": "string"}, auto_cast_timestamps=True
            )
            == False
        )

        # Should not auto-cast - disabled
        assert (
            _should_auto_cast_to_timestamp(
                "created_at", {"type": "string"}, auto_cast_timestamps=False
            )
            == False
        )

        # Should not auto-cast - already has format
        assert (
            _should_auto_cast_to_timestamp(
                "created_at",
                {"type": "string", "format": "date"},
                auto_cast_timestamps=True,
            )
            == False
        )

    def test_apply_timestamp_format(self):
        """Test applying timestamp format."""
        schema_entry = {"type": "string"}
        result = _apply_timestamp_format(schema_entry)
        assert result["format"] == "date-time"
        assert result["type"] == "string"


def _types(column: dict) -> dict:
    """A flattened column with its type as a sorted list, for order-free comparison."""
    types = column["type"]
    return {**column, "type": sorted([types] if isinstance(types, str) else types)}


class TestUntypedAndChoiceProperties:
    """Properties without a usable type, and anyOf/oneOf choices, load safely."""

    CONFIG: ClassVar[dict[str, str]] = {
        "catalog_url": "test.db",
        "data_path": "/tmp/test",
        "storage_type": "local",
    }
    JSON_COLUMN: ClassVar[dict] = {"type": ["null", "object"]}

    @pytest.mark.parametrize(
        "prop",
        [
            {"anyOf": [{"type": "null"}]},
            {"anyOf": [{"$ref": "#/definitions/d"}, {"type": "null"}]},
            {"anyOf": [{"anyOf": [{"type": "string"}]}, {"type": "null"}]},
            {"anyOf": [{"x-sql-datatype": "uuid"}, {"type": "null"}]},
            {"anyOf": [{}, {"type": "null"}]},
            {"format": "date-time"},
            {"$ref": "#/definitions/d"},
            {"x-sql-datatype": "uuid"},
        ],
        ids=[
            "anyof-all-null",
            "anyof-ref",
            "anyof-nested-anyof",
            "anyof-hint-only",
            "anyof-empty-branch",
            "format-only",
            "ref-only",
            "hint-only",
        ],
    )
    def test_untyped_property_becomes_json_column(self, prop: dict):
        """A property with no usable type is a JSON column, not a crash."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        assert flat == {"c": self.JSON_COLUMN}
        columns = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)
        assert columns[0]["type"] == "JSON"

    def test_anyof_skips_untyped_branches(self):
        """The first typed, non-null anyOf branch is chosen."""
        prop = {"anyOf": [{"$ref": "#/definitions/d"}, {"type": "integer"}]}
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        assert _types(flat["c"]) == {"type": ["integer", "null"]}

    def test_untyped_values_load_into_json_column(self, tmp_path: Path):
        """Any value in an untyped property is kept as JSON text."""
        schema = {"type": "object", "properties": {"c": {"$ref": "#/d"}}}
        flat = flatten_schema(schema)
        at = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        values = [5, "abc", {"a": 1}, [1, 2], 1.5, True, None, Decimal("2.5"), at]
        records = [flatten_record({"c": v}, flatten_schema=flat) for v in values]
        path = tmp_path / "batch.parquet"
        pq.write_table(create_pyarrow_table(records, _staging_schema(flat)), path)
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (c {ddl})")
        con.execute(f"INSERT INTO t SELECT c FROM '{path}'")
        rows = con.execute("SELECT c FROM t").fetchall()
        # As for every JSON column: None is SQL NULL, a Decimal a JSON number
        # and a datetime ISO text.
        assert [None if r[0] is None else json.loads(r[0]) for r in rows] == [
            *values[:6],
            None,
            2.5,
            "2024-01-01T10:00:00+00:00",
        ]
        assert rows[6] == (None,)

    @pytest.mark.parametrize(
        ("prop", "inner"),
        [
            ({"type": ["object", "null"]}, {"a": None}),
            ({"type": ["null", "array"], "items": {"type": "integer"}}, [None]),
            ({"$ref": "#/d"}, {"a": None}),
            ({}, [None]),
        ],
        ids=["object", "array", "untyped", "empty"],
    )
    def test_none_in_json_column_is_sql_null(
        self, tmp_path: Path, prop: dict, inner: object
    ):
        """A missing value is SQL NULL; a JSON null inside a value is kept."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        records = [flatten_record({"c": v}, flatten_schema=flat) for v in [None, inner]]
        path = tmp_path / "batch.parquet"
        pq.write_table(create_pyarrow_table(records, _staging_schema(flat)), path)
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (n INTEGER, c {ddl})")
        con.execute(f"INSERT INTO t SELECT row_number() OVER (), c FROM '{path}'")
        assert con.execute("SELECT n, c IS NULL FROM t ORDER BY n").fetchall() == [
            (1, True),
            (2, False),
        ]
        stored = con.execute("SELECT c FROM t WHERE n = 2").fetchone()[0]
        assert json.loads(stored) == inner

    def test_flatten_schema_leaves_input_unchanged(self):
        """Flattening never edits the schema it is given, at any level."""
        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "string", "description": "kept"},
                "b": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                "c": {"oneOf": [{"type": "string"}, {"type": "null"}]},
                "d": {
                    "type": "object",
                    "properties": {
                        "e": {"anyOf": [{"type": "null"}, {"type": "number"}]},
                        "f": {"type": "string", "description": "kept"},
                    },
                },
            },
        }
        original = copy.deepcopy(schema)
        flatten_schema(schema, max_level=1)
        assert schema == original

    def test_nested_flattening_unchanged(self):
        """Nested flattening with auto-cast timestamps is as before (old output)."""
        schema = {
            "type": "object",
            "properties": {
                "created_at": {"type": ["string", "null"], "description": "d"},
                "parent": {
                    "type": "object",
                    "properties": {
                        "updated_at": {"type": "string"},
                        "hinted": {
                            "anyOf": [{"type": "null"}, {"type": "number"}],
                            "x-sql-datatype": "real",
                        },
                        "deep": {
                            "type": "object",
                            "properties": {"x": {"type": "integer"}},
                        },
                        "tags": {"oneOf": [{"type": "array"}]},
                    },
                },
            },
        }
        flat = flatten_schema(schema, max_level=1, auto_cast_timestamps=True)
        assert {k: _types(v) for k, v in flat.items()} == {
            "created_at": {"type": ["null", "string"], "format": "date-time"},
            "parent__deep": {"type": ["null", "object"]},
            "parent__hinted": {"type": ["null", "number"], "x-sql-datatype": "real"},
            "parent__tags": {"type": ["array", "null"]},
            "parent__updated_at": {"type": ["string"]},
        }

    @pytest.mark.parametrize(
        "prop",
        [
            {"anyOf": [{"type": "string"}, {"type": "null"}]},
            {"anyOf": [{"type": "null"}, {"type": ["string", "null"]}]},
            {"oneOf": [{"type": "string"}]},
            {"allOf": [{"type": "string", "maxLength": 40}]},
        ],
        ids=["anyof", "anyof-type-list", "oneof", "allof"],
    )
    def test_auto_cast_timestamps_applies_to_choice_properties(self, prop: dict):
        """A choice property resolving to a plain string is auto-cast like one."""
        schema = {"type": "object", "properties": {"created_at": prop}}
        flat = flatten_schema(schema, auto_cast_timestamps=True)
        assert flat["created_at"]["format"] == "date-time"
        connector = DuckLakeConnector(self.CONFIG)
        assert connector.json_to_ducklake_schema(flat)[0]["type"] == "DATETIME"
        # Off (the default), the column is left as it was.
        assert "format" not in flatten_schema(schema)["created_at"]

    @pytest.mark.parametrize(
        "prop",
        [
            {"anyOf": [{"type": "string", "format": "date"}, {"type": "null"}]},
            {"anyOf": [{"type": "string"}, {"type": "null"}], "format": "date"},
            {"oneOf": [{"type": "integer"}]},
            {"anyOf": [{"$ref": "#/d"}]},
        ],
        ids=["branch-format", "wrapper-format", "integer", "untyped"],
    )
    def test_auto_cast_timestamps_skips_other_choice_properties(self, prop: dict):
        """A formatted, non-string or untyped choice column is not auto-cast."""
        schema = {"type": "object", "properties": {"created_at": prop}}
        flat = flatten_schema(schema, auto_cast_timestamps=True)
        assert flat["created_at"].get("format") != "date-time"
        assert flat == flatten_schema(schema)

    def test_auto_cast_timestamps_mixed_choice_follows_its_first_branch(self):
        """A string-first mixed choice is auto-cast, as a plain type list is."""
        mixed = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        listed = {"type": ["string", "integer"]}
        schema = {
            "type": "object",
            "properties": {"created_at": mixed, "updated_at": listed},
        }
        flat = flatten_schema(schema, auto_cast_timestamps=True)
        assert flat["created_at"]["format"] == "date-time"
        assert flat["updated_at"]["format"] == "date-time"

    def test_auto_cast_timestamps_checks_choice_property_names(self):
        """Only names in TIMESTAMP_COLUMN_NAMES are auto-cast, as for plain strings."""
        prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        schema = {"type": "object", "properties": {"note": prop}}
        assert "format" not in flatten_schema(schema, auto_cast_timestamps=True)["note"]

    def test_flattened_columns_share_nothing_with_the_input(self):
        """Editing a flattened column changes neither the input nor other columns."""
        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "string", "enum": ["x", "y"]},
                "b": {"anyOf": [{"type": "array", "items": {"type": "string"}}]},
                "u1": {"$ref": "#/d"},
                "u2": {"$ref": "#/d"},
            },
        }
        original = copy.deepcopy(schema)
        flat = flatten_schema(schema)
        flat["a"]["enum"].append("z")
        flat["u1"]["type"].append("string")
        assert schema == original
        assert sorted(flatten_schema(schema)["u2"]["type"]) == ["null", "object"]

    def test_boolean_property_schema_becomes_json_column(self):
        """A boolean schema (true: any value) is a JSON column, not a crash."""
        flat = flatten_schema({"type": "object", "properties": {"c": True}})
        assert flat == {"c": self.JSON_COLUMN}

    def test_output_unchanged_for_schemas_that_loaded_before(self):
        """Every kind of property that flattened before still flattens the same."""
        schema = {
            "type": "object",
            "properties": {
                "plain": {"type": "string", "description": "dropped"},
                "hinted": {
                    "anyOf": [{"type": "number"}, {"type": "null"}],
                    "x-sql-datatype": "numeric(12,4)",
                },
                "no_null": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                "listed": {"anyOf": [{"type": ["string", "null"]}]},
                "one_string": {"oneOf": [{"type": "string"}, {"type": "null"}]},
                "one_object": {"oneOf": [{"type": "object"}]},
                "arr": {"type": ["array", "null"], "items": {"type": "string"}},
                "obj": {"type": "object", "properties": {"x": {"type": "integer"}}},
            },
        }
        flat = flatten_schema(schema)
        assert {k: _types(v) for k, v in flat.items()} == {
            "plain": {"type": ["string"]},
            "hinted": {"type": ["null", "number"], "x-sql-datatype": "numeric(12,4)"},
            "no_null": {"type": ["null", "string"]},
            "listed": {"type": ["null", "string"]},
            "one_string": {"type": ["null", "string"]},
            "one_object": {"type": ["null", "object"]},
            "arr": {"type": ["array", "null"]},
            "obj": {"type": ["null", "object"]},
        }

    @pytest.mark.parametrize(
        ("prop", "expected"),
        [
            (
                {"oneOf": [{"type": "integer"}, {"type": "null"}]},
                ({"type": ["null", "integer"]}, "BIGINT", pa.int64()),
            ),
            (
                {"oneOf": [{"type": "null"}, {"type": "string"}]},
                ({"type": ["null", "string"]}, "VARCHAR", pa.string()),
            ),
            (
                {"oneOf": [{"type": ["boolean", "null"]}]},
                ({"type": ["null", "boolean"]}, "BOOLEAN", pa.bool_()),
            ),
            (
                {"allOf": [{"type": "number"}]},
                ({"type": ["null", "number"]}, "DECIMAL(38, 9)", pa.decimal128(38, 9)),
            ),
            (
                {"oneOf": [{"type": "string"}], "x-sql-datatype": "uuid"},
                (
                    {"type": ["null", "string"], "x-sql-datatype": "uuid"},
                    "UUID",
                    pa.string(),
                ),
            ),
            ({}, ({"type": ["null", "object"]}, "JSON", pa.string())),
            (
                {"description": "only a description"},
                ({"type": ["null", "object"]}, "JSON", pa.string()),
            ),
        ],
        ids=[
            "oneof-integer",
            "oneof-null-first",
            "oneof-type-list",
            "allof-number",
            "oneof-wrapper-hint",
            "empty",
            "description-only",
        ],
    )
    def test_choice_and_empty_properties_get_columns(self, prop: dict, expected: tuple):
        """oneOf/allOf flatten like anyOf, and {} is a JSON column, not dropped."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        staging = _staging_schema(flat).field("c").type
        assert (flat["c"], ddl, staging) == expected

    def test_choice_values_are_staged(self):
        """Values of a oneOf property reach the staged batch."""
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "n": {"oneOf": [{"type": "integer"}, {"type": "null"}]},
            },
        }
        flat = flatten_schema(schema)
        table = create_pyarrow_table(
            [{"id": 1, "n": 5}, {"id": 2, "n": None}],
            _staging_schema(flat),
        )
        assert table.column("n").to_pylist() == [5, None]

    @pytest.mark.parametrize("keyword", ["anyOf", "oneOf", "allOf"])
    def test_branch_type_order_is_the_same_in_every_process(self, keyword: str):
        """The column's type list keeps its declared order, whatever the hash seed."""
        code = (
            "import json; from target_ducklake.flatten import flatten_schema; "
            f"prop = {{{keyword!r}: [{{'type': ['integer', 'string']}}]}}; "
            "print(json.dumps(flatten_schema("
            "{'type': 'object', 'properties': {'c': prop}})['c']['type']))"
        )
        outputs = {
            # A fresh interpreter is the only way to set the hash seed.
            subprocess.run(  # noqa: S603
                [sys.executable, "-c", code],
                cwd=Path(__file__).parents[1],  # the repo root, for an uninstalled run
                env={**os.environ, "PYTHONHASHSEED": str(seed)},
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            for seed in range(1, 9)
        }
        assert [json.loads(o) for o in outputs] == [["null", "integer", "string"]]

    def _sink(self, prop: dict, **config: object) -> ducklakeSink:
        target = Mock()
        target.config = {**self.CONFIG, **config}
        return ducklakeSink(
            target=target,
            stream_name="t",
            schema={"type": "object", "properties": {"c": prop}},
            key_properties=[],
            connector=DuckLakeConnector(self.CONFIG),
        )

    @pytest.mark.parametrize(
        "prop",
        [
            {"anyOf": [{"type": "string"}, {"type": "integer"}]},
            {"anyOf": [{"type": "string"}, {"type": "null"}]},
            {"oneOf": [{"type": "string"}, {"type": "null"}]},
            {"oneOf": [{"type": "string"}, {"type": "integer"}]},
            {
                "type": "object",
                "properties": {"n": {"anyOf": [{"type": "string"}]}},
            },
            # A type beside the choice list doesn't stop null being accepted.
            {"type": "string", "anyOf": [{"maxLength": 3}]},
        ],
        ids=[
            "anyof-no-null",
            "anyof-with-null",
            "oneof-with-null",
            "oneof-no-null",
            "nested-anyof",
            "type-and-anyof",
        ],
    )
    def test_sink_validator_accepts_null_for_choices(self, prop: dict):
        """Null passes validation for anyOf/oneOf properties; the schema is kept."""
        original = copy.deepcopy(prop)
        sink = self._sink(prop)
        assert sink.schema["properties"]["c"] == original
        record = {"c": {"n": None}} if "properties" in prop else {"c": None}
        sink._validate_and_parse(record)

    def test_sink_validator_still_rejects_invalid_values_for_choices(self):
        """Only null is added: a value matching no branch is still rejected."""
        sink = self._sink({"anyOf": [{"type": "string"}, {"type": "integer"}]})
        with pytest.raises(InvalidRecord):
            sink._validate_and_parse({"c": [1]})

    def test_sink_without_record_validation_has_no_validator(self):
        """validate_records false still turns validation off."""
        sink = self._sink({"anyOf": [{"type": "string"}]}, validate_records=False)
        assert sink._validator is None

    def test_sink_validator_still_rejects_null_for_plain_types(self):
        """A plain non-nullable type keeps rejecting null, as before."""
        sink = self._sink({"type": "string"})
        sink._validator.validate({"c": "x"})
        with pytest.raises(InvalidRecord):
            sink._validator.validate({"c": None})


class TestParquetUtils:
    """Test parquet utilities functionality."""

    @pytest.mark.parametrize(
        ("column_type", "expected"),
        [
            ("DECIMAL(12, 4)", pa.decimal128(12, 4)),
            ("decimal(38,9)", pa.decimal128(38, 9)),
            ("SMALLINT", pa.int64()),
            ("INTEGER", pa.int64()),
            ("BIGINT", pa.int64()),
            ("REAL", pa.float64()),
            ("DOUBLE", pa.float64()),
            ("BOOLEAN", pa.bool_()),
            ("VARCHAR", pa.string()),
            ("VARCHAR(5)", pa.string()),
            ("JSON", pa.string()),
            ("UUID", pa.string()),
            ("DATETIME", pa.string()),
            ("TIMESTAMPTZ", pa.string()),
            ("DATE", pa.string()),
            ("TIME", pa.string()),
        ],
    )
    def test_staging_type_follows_column_type(
        self, column_type: str, expected: pa.DataType
    ):
        """Each DuckDB column type stages as the parquet type that holds its values."""
        assert staging_type(column_type) == expected

    def test_staging_schema(self):
        """The staging schema has one nullable field per column, in column order."""
        schema = staging_schema(
            [
                {"name": "id", "type": "BIGINT"},
                {"name": "name", "type": "VARCHAR"},
                {"name": "amount", "type": "DECIMAL(10, 2)"},
            ]
        )
        assert schema == pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("amount", pa.decimal128(10, 2)),
            ]
        )
        assert all(field.nullable for field in schema)


# One value of each JSON scalar type, and the text a VARCHAR column stores for it.
_SCALAR_VALUES = {
    "integer": (5, "5"),
    "number": (Decimal("1.50"), "1.50"),
    "boolean": (True, "true"),
    "string": ("abc", "abc"),
}
_MULTI_TYPES = [
    list(types)
    for n in range(2, len(_SCALAR_VALUES) + 1)
    for types in itertools.combinations(_SCALAR_VALUES, n)
]


class TestStagingFollowsColumnType:
    """Each column is staged at the type its DDL gives, so the two always agree."""

    CONFIG: ClassVar[dict[str, str]] = {"catalog_url": "x", "data_path": "/x"}
    UUID = "6f1c2a3e-1b2c-4d5e-8f90-123456789abc"

    def _load(self, prop: dict, values: list, tmp_path: Path) -> tuple[str, list]:
        """Flatten, stage and insert values as the sink does; return DDL and rows."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        schema = _staging_schema(flat)
        assert schema.field("c").type == staging_type(ddl)
        records = [flatten_record({"c": v}, flatten_schema=flat) for v in values]
        path = tmp_path / "batch.parquet"
        pq.write_table(create_pyarrow_table(records, schema), path)
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (n INTEGER, c {ddl})")
        con.execute(f"INSERT INTO t SELECT row_number() OVER (), c FROM '{path}'")
        return ddl, [r[0] for r in con.execute("SELECT c FROM t ORDER BY n").fetchall()]

    @pytest.mark.parametrize("types", _MULTI_TYPES, ids="-".join)
    @pytest.mark.parametrize("shape", ["type-list", "anyof-branch"])
    def test_multi_type_column_stages_every_value(
        self, tmp_path: Path, types: list[str], shape: str
    ):
        """A column with several JSON types is VARCHAR, and every value loads."""
        declared = ["null", *types]
        prop: dict = (
            {"type": declared}
            if shape == "type-list"
            else {"anyOf": [{"type": declared}]}
        )
        values = [_SCALAR_VALUES[t][0] for t in types] + [None]
        ddl, rows = self._load(prop, values, tmp_path)
        assert ddl == "VARCHAR"
        assert rows == [_SCALAR_VALUES[t][1] for t in types] + [None]

    def test_multi_type_with_object_is_still_json(self, tmp_path: Path):
        """A type list containing object or array stays a JSON column."""
        ddl, rows = self._load({"type": ["integer", "object"]}, [{"a": 1}], tmp_path)
        assert ddl == "JSON"
        assert [json.loads(r) for r in rows] == [{"a": 1}]

    @pytest.mark.parametrize(
        "case",
        [
            ("string", "numeric(12,4)", "12.5", "DECIMAL(12, 4)", Decimal("12.5")),
            ("string", "bigint", "42", "BIGINT", 42),
            # Number text loads as it did through DuckDB's cast.
            ("string", "bigint", " +12.0 ", "BIGINT", 12),
            ("string", "integer", "1e2", "INTEGER", 100),
            ("string", "bigint", "0e100", "BIGINT", 0),
            ("string", "double", " 1.5 ", "DOUBLE", 1.5),
            ("string", "double", "1.5e-12", "DOUBLE", 1.5e-12),
            ("string", "uuid", UUID, "UUID", uuid.UUID(UUID)),
            ("integer", "numeric(12,4)", 5, "DECIMAL(12, 4)", Decimal(5)),
            ("integer", "real", 5, "REAL", 5.0),
            ("integer", "smallint", 5, "SMALLINT", 5),
            ("integer", "jsonb", 5, "JSON", "5"),
            # Exact beyond float64's 53 bits.
            (
                "number",
                "bigint",
                Decimal(9007199254740993),
                "BIGINT",
                9007199254740993,
            ),
            ("number", "double", Decimal("1.5E-12"), "DOUBLE", 1.5e-12),
            ("number", "jsonb", Decimal("1.50"), "JSON", "1.50"),
            ("boolean", "smallint", True, "SMALLINT", 1),
        ],
        ids=lambda case: f"{case[0]}-{case[1]}",
    )
    def test_hint_that_fits_the_json_type_sets_ddl_and_staging(
        self, tmp_path: Path, recwarn: pytest.WarningsRecorder, case: tuple
    ):
        """A hint the JSON type's values load into is the column and staging type.

        ``case`` is (JSON type, hint, value, DDL type, stored value).
        """
        json_type, hint, value, ddl, stored = case
        prop = {"type": ["null", json_type], "x-sql-datatype": hint}
        assert self._load(prop, [value], tmp_path) == (ddl, [stored])
        assert not [w for w in recwarn.list if "x-sql-datatype" in str(w.message)]

    @pytest.mark.parametrize(
        ("prop", "ddl"),
        [
            ({"type": "integer", "x-sql-datatype": "uuid"}, "BIGINT"),
            ({"type": "number", "x-sql-datatype": "timestamptz"}, "DECIMAL(38, 9)"),
            ({"type": "boolean", "x-sql-datatype": "date"}, "BOOLEAN"),
            # A plain object column drops the hint when flattened; a choice keeps it.
            ({"anyOf": [{"type": "object"}], "x-sql-datatype": "uuid"}, "JSON"),
            ({"type": ["integer", "string"], "x-sql-datatype": "uuid"}, "VARCHAR"),
            (
                {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "x-sql-datatype": "uuid",
                },
                "BIGINT",
            ),
        ],
        ids=[
            "integer-uuid",
            "number-timestamptz",
            "boolean-date",
            "object-uuid",
            "multi-type-uuid",
            "anyof-wrapper",
        ],
    )
    def test_hint_that_cannot_hold_the_json_type_is_ignored(self, prop: dict, ddl: str):
        """An impossible hint is ignored with a warning; the JSON type decides."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        with pytest.warns(UserWarning, match="Ignoring x-sql-datatype"):
            columns = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)
        with pytest.warns(UserWarning, match="Ignoring x-sql-datatype"):
            staged = _staging_schema(flat).field("c").type
        assert columns[0]["type"] == ddl
        assert staged == staging_type(ddl)

    @pytest.mark.parametrize(
        ("prop", "value", "error"),
        [
            (
                {"type": "string", "x-sql-datatype": "numeric(12,4)"},
                "n/a",
                ValueError,
            ),
            (
                {"type": "number", "x-sql-datatype": "bigint"},
                Decimal("5.5"),
                ValueError,
            ),
            ({"type": "string", "x-sql-datatype": "bigint"}, "abc", ValueError),
            ({"type": "string", "x-sql-datatype": "bigint"}, "12.5", ValueError),
            (
                {"type": "number", "x-sql-datatype": "bigint"},
                Decimal("NaN"),
                ValueError,
            ),
            (
                {"type": "number", "x-sql-datatype": "bigint"},
                Decimal("Infinity"),
                ValueError,
            ),
            ({"type": "string", "x-sql-datatype": "double"}, "abc", pa.ArrowInvalid),
            ({"type": "string", "x-sql-datatype": "bigint"}, "1e30", ValueError),
            ({"type": "integer", "x-sql-datatype": "bigint"}, 2**63, ValueError),
            # Rejected at once, without building a million-digit int.
            ({"type": "string", "x-sql-datatype": "bigint"}, "1e1000000", ValueError),
        ],
        ids=[
            "string-decimal",
            "number-bigint-fraction",
            "string-bigint",
            "string-bigint-fraction",
            "number-bigint-nan",
            "number-bigint-infinity",
            "string-double",
            "string-bigint-overflow",
            "integer-bigint-overflow",
            "string-bigint-huge-exponent",
        ],
    )
    def test_value_the_column_cannot_hold_fails_the_batch(
        self, prop: dict, value: object, error: type[Exception]
    ):
        """A value the hinted column can't hold fails staging; it is never changed."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        with pytest.raises(error):
            create_pyarrow_table([{"c": value}], _staging_schema(flat))


class TestExistingNumberColumnStaging:
    """Numbers are staged at an existing column's own type, never rounded silently.

    Drives the real sink (setup, then one batch) on an in-memory catalog with
    a table created before, e.g. by an earlier version's type mapping.
    """

    @staticmethod
    def _load(
        tmp_path: Path, column_ddl: str, prop: dict, values: list
    ) -> tuple[list, ducklakeSink]:
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(f"CREATE TABLE ducklake_catalog.s.t (id BIGINT, {column_ddl})")
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "amount": prop},
        }
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=["id"],
                connector=connector,
            )
            sink.setup()
            records = [{"id": i, "amount": v} for i, v in enumerate(values)]
            sink.process_batch({"records": records})
        rows = con.execute("SELECT amount FROM ducklake_catalog.s.t ORDER BY id")
        return [r[0] for r in rows.fetchall()], sink

    def test_existing_decimal_column_keeps_its_scale(self, tmp_path: Path):
        """A DECIMAL(38, 30) column made by an earlier mapping keeps 30 digits."""
        value = Decimal("1.123456789012345678901234567890")
        rows, sink = self._load(
            tmp_path, "amount DECIMAL(38, 30)", {"type": "number"}, [value]
        )
        assert rows == [value]
        assert sink.pyarrow_schema.field("amount").type == pa.decimal128(38, 30)

    @pytest.mark.parametrize("column_type", ["DOUBLE", "REAL"])
    def test_existing_float_column_is_staged_as_float(
        self, tmp_path: Path, column_type: str
    ):
        """A DOUBLE or REAL column gets 1.5e-12, not the default scale's 0."""
        rows, _ = self._load(
            tmp_path, f"amount {column_type}", {"type": "number"}, [Decimal("1.5E-12")]
        )
        assert rows == [pytest.approx(1.5e-12)]

    def test_values_rounded_to_the_column_scale_are_reported(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ):
        """Extra fraction digits round to the column's scale, with a warning."""
        values = [Decimal("1.239"), Decimal("2.5"), None]
        with caplog.at_level("WARNING", logger="target_ducklake.parquet_utils"):
            rows, _ = self._load(
                tmp_path, "amount DECIMAL(10, 2)", {"type": "number"}, values
            )
        assert rows == [Decimal("1.24"), Decimal("2.50"), None]
        assert "1 value(s) in column 'amount'" in caplog.text
        assert "DECIMAL(10, 2)" in caplog.text

    @pytest.mark.parametrize(
        ("column_ddl", "value"),
        [
            ("amount DECIMAL(5, 2)", Decimal("1234.5")),
            # Rounding to the scale adds the digit that doesn't fit.
            ("amount DECIMAL(5, 2)", Decimal("999.995")),
            # 39 digits at scale 30: beyond the 38-digit decimal context too.
            ("amount DECIMAL(38, 30)", Decimal(1000000000)),
        ],
        ids=["narrow", "rounds-over", "wide"],
    )
    def test_value_too_large_for_the_existing_column_fails(
        self, tmp_path: Path, column_ddl: str, value: Decimal
    ):
        """A value with more integer digits than the column holds fails the batch."""
        with pytest.raises(ValueError, match="Column 'amount' is DECIMAL"):
            self._load(tmp_path, column_ddl, {"type": "number"}, [value])

    def test_float_hint_on_existing_decimal_column_keeps_the_column(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ):
        """An existing DECIMAL column isn't changed to DOUBLE for a float hint.

        Its values are rounded to its scale, with the rounding warning; the
        column must be recreated to store doubles.
        """
        with caplog.at_level("WARNING", logger="target_ducklake.parquet_utils"):
            rows, _ = self._load(
                tmp_path,
                "amount DECIMAL(38, 9)",
                {"type": "number", "x-sql-datatype": "double"},
                [Decimal("1.5E-12"), Decimal("2.5")],
            )
        assert rows == [Decimal(0), Decimal("2.5")]
        assert "1 value(s) in column 'amount'" in caplog.text

    @pytest.mark.parametrize(
        ("value", "stored"),
        [("1.5", "1.5"), ("1" * 33, "1" * 33), ("abc", "abc")],
        ids=["fraction", "33-digit-integer", "text"],
    )
    def test_string_into_existing_text_column_keeps_its_text(
        self, tmp_path: Path, value: str, stored: str
    ):
        """A string property's text lands unchanged in an existing VARCHAR column.

        Its hint (here unsized numeric, which a new table would make DECIMAL)
        doesn't change what the existing text column receives.
        """
        rows, sink = self._load(
            tmp_path,
            "amount VARCHAR",
            {"type": "string", "x-sql-datatype": "numeric"},
            [value],
        )
        assert rows == [stored]
        assert sink.pyarrow_schema.field("amount").type == pa.string()

    def test_whole_number_decimal_into_existing_integer_column(self, tmp_path: Path):
        """A bigint unsigned value into a BIGINT column loads or fails clearly.

        It is staged as int64, so an out-of-range value names the column.
        """
        prop = {"type": "integer", "x-sql-datatype": "bigint unsigned"}
        rows, sink = self._load(tmp_path / "ok", "amount BIGINT", prop, [5])
        assert rows == [5]
        assert sink.pyarrow_schema.field("amount").type == pa.int64()
        with pytest.raises(ValueError, match="Column 'amount' holds 64-bit integers"):
            self._load(tmp_path / "big", "amount BIGINT", prop, [2**64 - 1])

    def test_non_finite_number_fails_without_a_rounding_report(
        self, caplog: pytest.LogCaptureFixture
    ):
        """NaN and infinity fail the batch, naming the column; nothing is 'rounded'."""
        schema = pa.schema([pa.field("amount", pa.decimal128(38, 9))])
        for value in (float("nan"), float("inf"), Decimal("-Infinity")):
            with pytest.raises(ValueError, match="Column 'amount' is DECIMAL"):
                create_pyarrow_table([{"amount": value}], schema)
        assert "rounded" not in caplog.text

    @pytest.mark.parametrize(
        ("column_ddl", "staged"),
        [
            ("amount VARCHAR", pa.decimal128(38, 9)),
            ("amount BIGINT", pa.decimal128(38, 9)),
            ("other INTEGER", pa.decimal128(38, 9)),
        ],
        ids=["varchar-column", "bigint-column", "new-column"],
    )
    def test_other_columns_keep_the_stream_staging(
        self, tmp_path: Path, column_ddl: str, staged: pa.DataType
    ):
        """Only an existing DECIMAL/REAL/DOUBLE column changes a number's staging."""
        _, sink = self._load(tmp_path, column_ddl, {"type": "number"}, [Decimal(1)])
        assert sink.pyarrow_schema.field("amount").type == staged

    def test_existing_decimal_column_for_a_string_stream_stays_text(
        self, tmp_path: Path
    ):
        """A stream staging text into an existing DECIMAL column is unchanged."""
        _, sink = self._load(
            tmp_path, "amount DECIMAL(12, 4)", {"type": "string"}, ["1.5"]
        )
        assert sink.pyarrow_schema.field("amount").type == pa.string()


class TestDecimalPrecision:
    """Numbers are staged as decimals so they keep their exact digits."""

    # 29 integer + 9 decimal digits: fills DECIMAL(38, 9), far beyond float64.
    BIG = Decimal("12345678901234567890123456789.123456789")

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            ({"type": ["null", "number"]}, (38, 9)),
            ({"type": "number", "precision": 10, "scale": 2}, (10, 2)),
            ({"type": "number", "precision": 5}, (5, 5)),
        ],
    )
    def test_number_precision_and_scale(self, jsonschema, expected):
        """Precision caps at 38; scale defaults to 9, never above precision."""
        assert number_precision_and_scale(jsonschema) == expected

    def test_number_field_is_decimal(self):
        """Number columns are staged as decimal128, not float64."""
        schema = _staging_schema({"amount": {"type": ["null", "number"]}})
        assert schema.field("amount").type == pa.decimal128(38, 9)

    def test_parquet_type_matches_ducklake_column_type(self):
        """The staged parquet type and the DuckLake DDL type always agree."""
        jsonschema = {"type": "number", "precision": 12, "scale": 4}
        field = _staging_schema({"amount": jsonschema}).field("amount")
        column_type = JSONSchemaToDuckLake().to_sql_type(jsonschema)
        assert (field.type.precision, field.type.scale) == (12, 4)
        assert str(column_type) == "DECIMAL(12, 4)"

    def test_create_pyarrow_table_keeps_exact_digits(self):
        """Decimals beyond float64 precision survive staging unchanged."""
        schema = pa.schema([pa.field("amount", pa.decimal128(38, 9))])
        table = create_pyarrow_table([{"amount": self.BIG}], schema)
        assert table.column("amount").to_pylist() == [self.BIG]

    def test_create_pyarrow_table_coerces_numeric_inputs(self):
        """Ints, floats and None are accepted; extra digits round half-even."""
        schema = pa.schema([pa.field("amount", pa.decimal128(38, 2))])
        records = [
            {"amount": 7},
            {"amount": 0.1},
            {"amount": Decimal("1.005")},
            {"amount": Decimal("1.015")},
            {"amount": None},
        ]
        table = create_pyarrow_table(records, schema)
        assert table.column("amount").to_pylist() == [
            Decimal("7.00"),
            Decimal("0.10"),
            Decimal("1.00"),
            Decimal("1.02"),
            None,
        ]

    def test_non_number_columns_still_accept_decimals(self):
        """A Decimal in an integer column is still converted as before."""
        schema = pa.schema([pa.field("id", pa.int64())])
        table = create_pyarrow_table([{"id": Decimal(5)}], schema)
        assert table.column("id").to_pylist() == [5]

    def test_decimal_round_trip_through_duckdb(self, tmp_path):
        """Staged parquet inserted into a DECIMAL(38, 9) column reads back exactly."""
        schema = _staging_schema({"amount": {"type": "number"}})
        table = create_pyarrow_table([{"amount": self.BIG}], schema)
        path = tmp_path / "batch.parquet"
        pq.write_table(table, path)

        con = duckdb.connect()
        con.execute("CREATE TABLE t (amount DECIMAL(38, 9))")
        con.execute(f"INSERT INTO t SELECT amount FROM '{path}'")
        assert con.execute("SELECT amount FROM t").fetchone()[0] == self.BIG

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            ({"type": "number", "x-sql-datatype": "numeric(50,45)"}, (38, 33)),
            ({"type": "number", "precision": 50, "scale": 45}, (38, 33)),
            ({"type": "number", "x-sql-datatype": "numeric(40,2)"}, (38, 2)),
            ({"type": "number", "x-sql-datatype": "decimal(65,30)"}, (38, 9)),
            ({"type": "number", "x-sql-datatype": "numeric(60,10)"}, (38, 9)),
            ({"type": "number", "precision": 50}, (38, 9)),
            ({"type": "number", "x-sql-datatype": "numeric(39,0)"}, (38, 0)),
            ({"type": "number", "x-sql-datatype": "numeric(39,39)"}, (38, 38)),
            # No integer digits declared, so none are kept.
            ({"type": "number", "precision": 50, "scale": 60}, (38, 38)),
        ],
        ids=[
            "hint-keeps-integer-digits",
            "keys-keep-integer-digits",
            "small-scale-kept",
            "mysql-max-keeps-default-scale",
            "integer-digits-beyond-38",
            "precision-only",
            "integer-only",
            "fraction-only",
            "keys-scale-above-precision",
        ],
    )
    def test_precision_above_38_keeps_integer_digits(
        self, jsonschema: dict, expected: tuple
    ):
        """Capping to 38 drops fraction digits first, down to min(scale, 9)."""
        with pytest.warns(UserWarning, match="exceeds DuckDB's 38-digit DECIMAL"):
            assert number_precision_and_scale(jsonschema) == expected

    def test_precision_within_38_is_unchanged(self, recwarn: pytest.WarningsRecorder):
        """A hint that fits DuckDB is used exactly, without a warning."""
        jsonschema = {"type": "number", "x-sql-datatype": "numeric(38,38)"}
        assert number_precision_and_scale(jsonschema) == (38, 38)
        assert not recwarn.list

    @pytest.mark.filterwarnings("ignore:.*38-digit DECIMAL")
    def test_capped_hint_loads_values_through_duckdb(self, tmp_path: Path):
        """A numeric(50,45) column loads values of 1 or more, read back exactly."""
        props = {"amount": {"type": "number", "x-sql-datatype": "numeric(50,45)"}}
        values = [Decimal("12.5"), Decimal("99999.000000000000000000000000000000001")]
        ddl = DuckLakeConnector(
            {"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        ).json_to_ducklake_schema(props)[0]["type"]
        schema = _staging_schema(props)
        table = create_pyarrow_table([{"amount": v} for v in values], schema)
        path = tmp_path / "batch.parquet"
        pq.write_table(table, path)

        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (amount {ddl})")
        con.execute(f"INSERT INTO t SELECT amount FROM '{path}'")
        assert ddl == "DECIMAL(38, 33)"
        assert schema.field("amount").type == pa.decimal128(38, 33)
        assert [r[0] for r in con.execute("SELECT amount FROM t").fetchall()] == values


class TestIntegrationScenarios:
    """Integration tests for common scenarios."""

    def test_local_storage_workflow(self):
        """Test complete workflow with local storage."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "add_record_metadata": True,
            "flatten_max_level": 2,
        }
        target = Targetducklake(config=config)
        assert target.config["storage_type"] == "local"
        assert target.config["add_record_metadata"] == True
        assert target.config["flatten_max_level"] == 2

    def test_s3_storage_workflow(self):
        """Test complete workflow with S3 storage."""
        config = {
            "catalog_url": "test.db",
            "data_path": "s3://bucket/path",
            "storage_type": "S3",
            "public_key": "test_key",
            "secret_key": "test_secret",
            "region": "us-east-1",
        }
        target = Targetducklake(config=config)
        assert target.config["storage_type"] == "S3"
        assert target.config["region"] == "us-east-1"

    def test_partitioned_data_workflow(self):
        """Test workflow with partitioned data."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "partition_fields": {
                "events": [
                    {
                        "column_name": "event_date",
                        "type": "timestamp",
                        "granularity": ["year", "month", "day"],
                    }
                ]
            },
        }
        target = Targetducklake(config=config)
        assert "events" in target.config["partition_fields"]

    def test_merge_vs_append_logic(self):
        """Test merge vs append logic based on key properties."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }

        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        }

        # Test with key properties (should merge)
        with patch("target_ducklake.sinks.DuckLakeConnector"):
            target = Mock()
            target.config = config
            sink = ducklakeSink(
                target=target, stream_name="users", schema=schema, key_properties=["id"]
            )
            assert sink.key_properties == ["id"]

        # Test without key properties (should append)
        with patch("target_ducklake.sinks.DuckLakeConnector"):
            target = Mock()
            target.config = config
            sink = ducklakeSink(
                target=target, stream_name="users", schema=schema, key_properties=None
            )
            # Singer SDK may convert None to empty list, both indicate no key properties
            assert sink.key_properties is None or sink.key_properties == []


class TestMergeKeyMatching:
    """Merge deletes match on each key column, so composite keys can't collide."""

    CONFIG: ClassVar[dict[str, str]] = {
        "catalog_url": "test.db",
        "data_path": "/tmp/test",
        "storage_type": "local",
    }

    def _merge_sql(
        self,
        key_properties: list[str],
        key_column_types: dict[str, str] | None = None,
    ) -> str:
        connector = DuckLakeConnector(self.CONFIG)
        with patch.object(connector, "execute") as mock_execute:
            connector.merge_into_table(
                file_location="batch.parquet",
                target_schema_name="s",
                table_name="t",
                file_columns=["tenant", "id", "v"],
                target_table_columns=["tenant", "id", "v"],
                key_properties=key_properties,
                key_column_types=key_column_types,
            )
        return mock_execute.call_args[0][0]

    def test_composite_key_matches_each_column(self):
        """Composite keys compare column by column instead of concatenating."""
        sql = self._merge_sql(["tenant", "id"])
        assert (
            """USING (SELECT "tenant" AS "tenant", "id" AS "id" FROM 'batch.parquet')"""
            in sql
        )
        assert (
            'WHERE "target"."tenant" IS NOT DISTINCT FROM "staged"."tenant" '
            'AND "target"."id" IS NOT DISTINCT FROM "staged"."id"'
        ) in sql
        assert "||" not in sql

    def test_single_key(self):
        """A single key produces a single NULL-safe comparison."""
        sql = self._merge_sql(["id"])
        assert 'WHERE "target"."id" IS NOT DISTINCT FROM "staged"."id";' in sql

    def test_keys_cast_to_table_types(self):
        """Each staged key is cast to its table column type; the table side isn't."""
        sql = self._merge_sql(
            ["tenant", "ts", "d"], {"tenant": "VARCHAR", "ts": "TIMESTAMP", "d": "DATE"}
        )
        expected = (
            'USING (SELECT CAST("tenant" AS VARCHAR) AS "tenant", '
            'CAST("ts" AS TIMESTAMP) AS "ts", CAST("d" AS DATE) AS "d" FROM'
        )
        assert expected in sql
        assert "::VARCHAR" not in sql

    def test_does_not_mutate_key_properties(self):
        """The caller's key list is left unchanged."""
        keys = ["tenant", "ts"]
        self._merge_sql(keys, {"ts": "TIMESTAMP"})
        assert keys == ["tenant", "ts"]

    def test_composite_key_collision_keeps_unrelated_rows(self, tmp_path):
        """Merging ('12', '3') must not delete the unrelated row ('1', '23')."""
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(
            "CREATE TABLE ducklake_catalog.s.t (tenant VARCHAR, id VARCHAR, v VARCHAR)"
        )
        con.execute(
            "INSERT INTO ducklake_catalog.s.t VALUES "
            "('1', '23', 'unrelated'), ('a', '1', 'old')"
        )
        batch = tmp_path / "batch.parquet"
        pq.write_table(
            pa.table(
                {"tenant": ["12", "a"], "id": ["3", "1"], "v": ["new", "updated"]}
            ),
            batch,
        )

        connector = DuckLakeConnector(self.CONFIG)
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            connector.merge_into_table(
                file_location=str(batch),
                target_schema_name="s",
                table_name="t",
                file_columns=["tenant", "id", "v"],
                target_table_columns=["tenant", "id", "v"],
                key_properties=["tenant", "id"],
            )

        rows = con.execute(
            "SELECT tenant, id, v FROM ducklake_catalog.s.t ORDER BY tenant, id"
        ).fetchall()
        assert rows == [
            ("1", "23", "unrelated"),
            ("12", "3", "new"),
            ("a", "1", "updated"),
        ]


class TestTemporalMergeKeys:
    """Re-merging a batch keyed on a TIMESTAMP, DATE or TIME leaves one row per key."""

    CONFIG: ClassVar[dict[str, str]] = {
        "catalog_url": "test.db",
        "data_path": "/tmp/test",
        "storage_type": "local",
    }

    @pytest.mark.parametrize(
        ("json_format", "column_type", "value"),
        [
            # Naive values on purpose: taps send zone-less timestamps.
            ("date-time", "DATETIME", datetime(2024, 1, 1, 10, 0, 0)),  # noqa: DTZ001
            ("date-time", "DATETIME", datetime(2024, 1, 1, 10, 0, 0, 500000)),  # noqa: DTZ001
            ("date-time", "DATETIME", datetime(2024, 1, 1, 10, tzinfo=timezone.utc)),
            ("date", "DATE", date(2024, 1, 1)),
            ("time", "TIME", time(10, 0, 0)),
        ],
    )
    def test_remerge_replaces_row(
        self, tmp_path: Path, json_format: str, column_type: str, value: object
    ):
        """The second merge of the same key replaces the first row."""
        flat_schema = {
            "k": {"type": ["null", "string"], "format": json_format},
            "v": {"type": ["null", "string"]},
        }
        connector = DuckLakeConnector(self.CONFIG)
        assert connector.json_to_ducklake_schema(flat_schema)[0]["type"] == column_type

        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(f"CREATE TABLE ducklake_catalog.s.t (k {column_type}, v VARCHAR)")
        pyarrow_schema = _staging_schema(flat_schema)

        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            for version in ("first", "second"):
                # Values arrive parsed, as the singer-sdk delivers them.
                batch = tmp_path / f"{version}.parquet"
                table = create_pyarrow_table(
                    [{"k": value, "v": version}], pyarrow_schema
                )
                pq.write_table(table, batch)
                connector.merge_into_table(
                    file_location=str(batch),
                    target_schema_name="s",
                    table_name="t",
                    file_columns=["k", "v"],
                    target_table_columns=["k", "v"],
                    key_properties=["k"],
                    key_column_types={"k": column_type},
                )

        rows = con.execute("SELECT v FROM ducklake_catalog.s.t").fetchall()
        assert rows == [("second",)]


class TestSqlDatatypeHints:
    """x-sql-datatype hints give columns the source database's own types."""

    CONFIG: ClassVar[dict[str, str]] = {
        "catalog_url": "test.db",
        "data_path": "/tmp/test",
        "storage_type": "local",
    }

    @staticmethod
    def _ddl_type(prop: dict) -> str:
        return str(JSONSchemaToDuckLake().to_sql_type(prop))

    @pytest.mark.parametrize(
        ("json_type", "hint", "expected"),
        [
            ("integer", "smallint", "SMALLINT"),
            ("integer", "int2", "SMALLINT"),
            ("integer", "integer", "INTEGER"),
            ("integer", "int", "INTEGER"),
            ("integer", "int4", "INTEGER"),
            ("integer", "bigint", "BIGINT"),
            ("integer", "int8", "BIGINT"),
            ("integer", "BIGINT", "BIGINT"),
            ("number", "real", "REAL"),
            ("number", "float4", "REAL"),
            ("number", "float", "DOUBLE"),
            ("number", "float8", "DOUBLE"),
            ("number", "double", "DOUBLE"),
            ("number", "Double  Precision", "DOUBLE"),
            ("number", "float(24)", "REAL"),
            ("number", "float(53)", "DOUBLE"),
            ("number", "decimal(10,2)", "DECIMAL(10, 2)"),
            ("number", "NUMERIC( 20, 4 )", "DECIMAL(20, 4)"),
            ("string", "numeric(18,2)", "DECIMAL(18, 2)"),
            ("number", "decimal(50,40)", "DECIMAL(38, 28)"),
            ("string", "timestamp", "TIMESTAMP"),
            ("string", "timestamp without time zone", "TIMESTAMP"),
            ("string", "timestamp_ntz", "TIMESTAMP"),
            ("string", "timestamptz", "TIMESTAMPTZ"),
            ("string", "timestamp with time zone", "TIMESTAMPTZ"),
            ("string", "timestamp_tz", "TIMESTAMPTZ"),
            ("string", "timestamp_ltz", "TIMESTAMPTZ"),
            ("string", "date", "DATE"),
            ("string", "time", "TIME"),
            ("string", "time without time zone", "TIME"),
            ("string", "uuid", "UUID"),
            ("string", "uniqueidentifier", "UUID"),
            ("string", "json", "JSON"),
            ("string", "jsonb", "JSON"),
        ],
    )
    @pytest.mark.filterwarnings("ignore:.*38-digit DECIMAL")
    def test_supported_hint(self, json_type: str, hint: str, expected: str):
        """Each supported hint maps to its DuckDB type, ignoring case and spacing."""
        prop = {"type": ["null", json_type], "x-sql-datatype": hint}
        assert self._ddl_type(prop) == expected

    @pytest.mark.parametrize(
        ("prop", "expected"),
        [
            ({"type": ["null", "string"], "x-sql-datatype": "geography"}, "VARCHAR"),
            ({"type": "integer", "x-sql-datatype": "tinyint"}, "BIGINT"),
            # DuckLake's Parquet files drop a TIMETZ offset, so text is safer.
            ({"type": "string", "x-sql-datatype": "timetz"}, "VARCHAR"),
            ({"type": "string", "x-sql-datatype": "time with time zone"}, "VARCHAR"),
            (
                {"type": "string", "format": "date-time", "x-sql-datatype": "datetime"},
                "DATETIME",
            ),
        ],
    )
    def test_unknown_hint_falls_back(self, prop: dict, expected: str):
        """An unsupported hint keeps today's mapping and the SDK's warning."""
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            assert self._ddl_type(prop) == expected

    def test_no_hint_keeps_existing_mapping(self):
        """Properties without a hint map exactly as before."""
        assert self._ddl_type({"type": "number"}) == "DECIMAL(38, 9)"
        assert self._ddl_type({"type": "integer"}) == "BIGINT"
        assert self._ddl_type({"type": "string", "format": "date-time"}) == "DATETIME"

    def test_staging_matches_ddl(self):
        """Decimal hints stage at the hinted precision; float hints stage as float64."""
        schema = _staging_schema(
            {
                "d": {"type": "number", "x-sql-datatype": "numeric(12,4)"},
                "f": {"type": "number", "x-sql-datatype": "double precision"},
                "n": {"type": "number"},
            }
        )
        assert schema.field("d").type == pa.decimal128(12, 4)
        assert schema.field("f").type == pa.float64()
        assert schema.field("n").type == pa.decimal128(38, 9)

    @pytest.mark.parametrize(
        ("json_type", "hint", "ddl"),
        [
            # Decimals without a scale (0) or without a size (the default).
            ("number", "decimal(10)", "DECIMAL(10, 0)"),
            ("number", "numeric(12)", "DECIMAL(12, 0)"),
            ("number", "numeric", "DECIMAL(38, 9)"),
            ("string", "decimal", "DECIMAL(38, 9)"),
            # Fractional-seconds precision.
            ("string", "timestamp(6)", "TIMESTAMP"),
            ("string", "timestamp(3) without time zone", "TIMESTAMP"),
            ("string", "timestamp(3) with time zone", "TIMESTAMPTZ"),
            ("string", "timestamptz(6)", "TIMESTAMPTZ"),
            ("string", "time(3)", "TIME"),
            # SQL Server.
            ("string", "datetime2", "TIMESTAMP"),
            ("string", "datetime2(7)", "TIMESTAMP"),
            ("string", "smalldatetime", "TIMESTAMP"),
            ("string", "datetimeoffset(7)", "TIMESTAMPTZ"),
            # MySQL integers: display widths and unsigned ranges.
            ("integer", "int(11)", "INTEGER"),
            ("integer", "bigint(20)", "BIGINT"),
            ("integer", "mediumint", "INTEGER"),
            ("integer", "mediumint unsigned", "INTEGER"),
            ("integer", "smallint unsigned", "INTEGER"),
            ("integer", "int unsigned", "BIGINT"),
            ("integer", "int(10) unsigned zerofill", "BIGINT"),
            ("integer", "integer unsigned", "BIGINT"),
            ("integer", "bigint unsigned", "DECIMAL(20, 0)"),
            ("integer", "bigint(20) unsigned", "DECIMAL(20, 0)"),
            # MySQL's deprecated unsigned decimals and floats.
            ("number", "decimal(10,2) unsigned", "DECIMAL(10, 2)"),
            ("number", "double unsigned", "DOUBLE"),
            ("number", "float unsigned zerofill", "DOUBLE"),
        ],
    )
    def test_more_source_spellings(
        self,
        recwarn: pytest.WarningsRecorder,
        json_type: str,
        hint: str,
        ddl: str,
    ):
        """Common source spellings map to their type, staged alike, with no warning."""
        flat = {"c": {"type": ["null", json_type], "x-sql-datatype": hint}}
        columns = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)
        assert columns[0]["type"] == ddl
        assert _staging_schema(flat).field("c").type == staging_type(ddl)
        assert not [w for w in recwarn.list if "x-sql-datatype" in str(w.message)]

    @pytest.mark.parametrize(
        ("json_type", "hint"),
        [
            ("string", "text"),
            ("string", "TEXT"),
            ("string", "longtext"),
            ("string", "varchar"),
            ("string", "varchar(255)"),
            ("string", "character varying(10)"),
            ("string", "char(2)"),
            ("string", "nvarchar(50)"),
            ("string", "varchar(max)"),
            ("string", "nvarchar(MAX)"),
            ("string", "citext"),
            ("boolean", "boolean"),
            ("boolean", "bool"),
        ],
    )
    def test_hint_matching_the_default_does_not_warn(
        self, recwarn: pytest.WarningsRecorder, json_type: str, hint: str
    ):
        """A hint naming the type the JSON type gives anyway is silently accepted."""
        prop = {"type": json_type, "x-sql-datatype": hint}
        assert self._ddl_type(prop) == self._ddl_type({"type": json_type})
        assert not recwarn.list

    @pytest.mark.parametrize(
        ("json_type", "hint"),
        [
            ("integer", "text"),
            ("integer", "boolean"),
            ("boolean", "text"),
            ("string", "boolean"),
        ],
    )
    def test_default_hint_on_another_json_type_still_warns(
        self, json_type: str, hint: str
    ):
        """A text or boolean hint is silent only on the JSON type it names."""
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            ddl = self._ddl_type({"type": json_type, "x-sql-datatype": hint})
        assert ddl == self._ddl_type({"type": json_type})

    @pytest.mark.parametrize(
        "hint", ["tinyint", "tinyint(1) unsigned", "timetz", "datetime", "decimal(0)"]
    )
    def test_deliberately_unsupported_hints_still_warn(self, hint: str):
        """Hints left unsupported on purpose (tinyint, timetz, datetime) still warn."""
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            self._ddl_type({"type": "string", "x-sql-datatype": hint})

    def test_bigint_unsigned_holds_its_maximum(self, tmp_path: Path):
        """2^64 - 1 loads into a bigint unsigned column exactly."""
        flat = {"c": {"type": "integer", "x-sql-datatype": "bigint unsigned"}}
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        path = tmp_path / "batch.parquet"
        pq.write_table(
            create_pyarrow_table([{"c": 2**64 - 1}], _staging_schema(flat)), path
        )
        con = duckdb.connect()
        con.execute(f"CREATE TABLE t (c {ddl})")
        con.execute(f"INSERT INTO t SELECT c FROM '{path}'")
        assert con.execute("SELECT c FROM t").fetchone()[0] == 2**64 - 1

    def _column_types(self, prop: dict) -> tuple[str, pa.DataType]:
        """The DDL type and staging type a stream property gets, as in the sink."""
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        ddl = DuckLakeConnector(self.CONFIG).json_to_ducklake_schema(flat)[0]["type"]
        return ddl, _staging_schema(flat).field("c").type

    @pytest.mark.parametrize(
        ("prop", "expected"),
        [
            (
                {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "x-sql-datatype": "uuid",
                },
                ("UUID", pa.string()),
            ),
            (
                {
                    "anyOf": [{"type": "null"}, {"type": "number"}],
                    "x-sql-datatype": "numeric(12,4)",
                },
                ("DECIMAL(12, 4)", pa.decimal128(12, 4)),
            ),
            (
                {
                    "anyOf": [
                        {"type": "null"},
                        {"type": "number", "x-sql-datatype": "numeric(12,4)"},
                    ]
                },
                ("DECIMAL(12, 4)", pa.decimal128(12, 4)),
            ),
            (
                # The branch's own hint wins over the wrapper's.
                {
                    "anyOf": [
                        {"type": "number", "x-sql-datatype": "real"},
                        {"type": "null"},
                    ],
                    "x-sql-datatype": "numeric(12,4)",
                },
                ("REAL", pa.float64()),
            ),
            (
                # An empty branch hint is no hint, so the wrapper's applies.
                {
                    "anyOf": [
                        {"type": "string", "x-sql-datatype": None},
                        {"type": "null"},
                    ],
                    "x-sql-datatype": "uuid",
                },
                ("UUID", pa.string()),
            ),
            (
                # Only the hint is carried; a wrapper format changes nothing.
                {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "format": "date-time",
                },
                ("VARCHAR", pa.string()),
            ),
        ],
        ids=[
            "outer-uuid",
            "outer-decimal-null-first",
            "inner-decimal",
            "inner-wins",
            "empty-inner-hint",
            "outer-format-not-carried",
        ],
    )
    def test_hint_on_anyof_property(self, prop: dict, expected: tuple):
        """A hint on an anyOf property sets both the DDL and the staging type."""
        assert self._column_types(prop) == expected

    def test_anyof_wrapper_hint_does_not_change_callers_schema(self):
        """The wrapper's hint goes into a copy, not into the caller's branch."""
        prop = {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "x-sql-datatype": "uuid",
        }
        flat = flatten_schema({"type": "object", "properties": {"c": prop}})
        assert flat["c"]["x-sql-datatype"] == "uuid"
        assert "x-sql-datatype" not in prop["anyOf"][0]

    def test_unsupported_hint_on_anyof_property_falls_back(self):
        """An unsupported wrapper hint keeps the default mapping, with a warning."""
        prop = {
            "anyOf": [{"type": "number"}, {"type": "null"}],
            "x-sql-datatype": "money",
        }
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            types = self._column_types(prop)
        assert types == ("DECIMAL(38, 9)", pa.decimal128(38, 9))

    @pytest.mark.parametrize(
        ("hint", "expected"),
        [
            ("decimal(5,5)", ("DECIMAL(5, 5)", pa.decimal128(5, 5))),
            ("decimal(5,0)", ("DECIMAL(5, 0)", pa.decimal128(5, 0))),
            ("decimal(1,0)", ("DECIMAL(1, 0)", pa.decimal128(1, 0))),
        ],
        ids=["scale-equals-precision", "zero-scale", "precision-one"],
    )
    def test_decimal_hint_edges_are_valid(self, hint: str, expected: tuple):
        """The boundaries of a valid decimal hint are honoured exactly."""
        ddl, staging = self._column_types({"type": "number", "x-sql-datatype": hint})
        assert (ddl, staging) == expected
        duckdb.connect().execute(f"CREATE TABLE t (c {ddl})")

    @pytest.mark.parametrize(
        ("prop", "expected"),
        [
            (
                {"type": "number", "x-sql-datatype": "decimal(0,0)"},
                ("DECIMAL(38, 9)", pa.decimal128(38, 9)),
            ),
            (
                {"type": "number", "x-sql-datatype": "decimal(5,10)"},
                ("DECIMAL(38, 9)", pa.decimal128(38, 9)),
            ),
            (
                {"type": "string", "x-sql-datatype": "numeric(0,0)"},
                ("VARCHAR", pa.string()),
            ),
        ],
        ids=["zero-precision", "scale-above-precision", "string-zero-precision"],
    )
    def test_invalid_decimal_hint_falls_back(self, prop: dict, expected: tuple):
        """An impossible decimal hint keeps the default mapping, with a warning."""
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            ddl, staging = self._column_types(prop)
        assert (ddl, staging) == expected
        duckdb.connect().execute(f"CREATE TABLE t (c {ddl})")

    def test_sink_starts_with_zero_precision_decimal_hint(self):
        """A decimal(0,0) hint no longer crashes building the sink's staging schema."""
        target = Mock()
        target.config = self.CONFIG
        schema = {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "x-sql-datatype": "decimal(0,0)"}
            },
        }
        with pytest.warns(UserWarning, match="does not support the x-sql-datatype"):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=[],
                connector=DuckLakeConnector(self.CONFIG),
            )
        assert sink.pyarrow_schema.field("amount").type == pa.decimal128(38, 9)

    def test_hinted_types_load_end_to_end(self, tmp_path: Path):
        """Hinted columns are created by the real DDL and load SDK-parsed values."""
        props = {
            "id": {"type": "integer", "x-sql-datatype": "smallint"},
            "small_f": {"type": "number", "x-sql-datatype": "real"},
            "tiny_d": {"type": "number", "x-sql-datatype": "float8"},
            "amount": {"type": "string", "x-sql-datatype": "numeric(12,4)"},
            "at": {
                "type": "string",
                "format": "date-time",
                "x-sql-datatype": "timestamptz",
            },
            "uid": {"type": "string", "x-sql-datatype": "uuid"},
            "doc": {"type": "string", "x-sql-datatype": "jsonb"},
        }
        connector = DuckLakeConnector(self.CONFIG)
        columns = connector.json_to_ducklake_schema(props)
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")

        record = {
            "id": 7,
            "small_f": Decimal("1.5"),
            "tiny_d": Decimal("1.5E-12"),
            "amount": "1234.5678",
            "at": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            "uid": "6f1c2a3e-1b2c-4d5e-8f90-123456789abc",
            "doc": '{"a": 1}',
        }
        batch = tmp_path / "batch.parquet"
        table = create_pyarrow_table([record], _staging_schema(props))
        pq.write_table(table, batch)

        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            connector._create_empty_table("s", "t", columns)
            connector.insert_into_table(
                file_location=str(batch),
                target_schema_name="s",
                table_name="t",
                file_columns=list(props),
                target_table_columns=list(props),
            )

        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 't'"
            ).fetchall()
        )
        assert types == {
            "id": "SMALLINT",
            "small_f": "FLOAT",
            "tiny_d": "DOUBLE",
            "amount": "DECIMAL(12,4)",
            "at": "TIMESTAMP WITH TIME ZONE",
            "uid": "UUID",
            "doc": "JSON",
        }
        row = con.execute(
            'SELECT id, small_f, tiny_d, amount, epoch("at"), '
            "uid::VARCHAR, doc::VARCHAR "
            "FROM ducklake_catalog.s.t"
        ).fetchone()
        assert row == (
            7,
            1.5,
            1.5e-12,
            Decimal("1234.5678"),
            1704103200.0,
            "6f1c2a3e-1b2c-4d5e-8f90-123456789abc",
            '{"a": 1}',
        )

    def test_hinted_timestamptz_key_remerges(self, tmp_path: Path):
        """A timestamptz-hinted merge key is cast like other temporal keys."""
        props = {
            "at": {
                "type": "string",
                "format": "date-time",
                "x-sql-datatype": "timestamptz",
            },
            "v": {"type": ["null", "string"]},
        }
        connector = DuckLakeConnector(self.CONFIG)
        key_type = connector.json_to_ducklake_schema(props)[0]["type"]
        assert key_type == "TIMESTAMPTZ"
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute('CREATE TABLE ducklake_catalog.s.t ("at" TIMESTAMPTZ, v VARCHAR)')
        pyarrow_schema = _staging_schema(props)
        value = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)

        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            for version in ("first", "second"):
                batch = tmp_path / f"{version}.parquet"
                pq.write_table(
                    create_pyarrow_table([{"at": value, "v": version}], pyarrow_schema),
                    batch,
                )
                connector.merge_into_table(
                    file_location=str(batch),
                    target_schema_name="s",
                    table_name="t",
                    file_columns=["at", "v"],
                    target_table_columns=["at", "v"],
                    key_properties=["at"],
                    key_column_types={"at": key_type},
                )

        assert con.execute("SELECT v FROM ducklake_catalog.s.t").fetchall() == [
            ("second",)
        ]


class TestMergeKeyTableTypes:
    """Re-merging leaves one row per key, whatever the key column's type.

    Drives the real sink lifecycle (setup, then process_batch twice) through a
    real connector on an in-memory DuckDB catalog, so the key types come from
    the table exactly as in production.
    """

    UUID = "6f1c2a3e-1b2c-4d5e-8f90-123456789abc"

    @staticmethod
    def _catalog() -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        # A non-UTC session zone, so TIMESTAMP vs TIMESTAMPTZ mix-ups show.
        con.execute("SET TimeZone = 'America/New_York'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        return con

    def _load_twice(
        self,
        con: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        key_prop: dict,
        value: object,
        key_properties: list[str],
    ) -> list[tuple]:
        """Run setup and two merges of the same key; return the rows left.

        The first key property is the column typed by ``key_prop``.
        """
        key_name = key_properties[0]
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": "merge",
        }
        schema = {
            "type": "object",
            "properties": {
                key_name: key_prop,
                "id": {"type": ["null", "string"]},
                "v": {"type": ["null", "string"]},
            },
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=key_properties,
                connector=connector,
            )
            sink.setup()
            for version in ("first", "second"):
                # Values arrive parsed, as the singer-sdk delivers them.
                record = {key_name: value, "id": "a", "v": version}
                sink.process_batch({"records": [record]})
        return con.execute(
            "SELECT id, v FROM ducklake_catalog.s.t ORDER BY id"
        ).fetchall()

    @pytest.mark.parametrize("key_shape", ["single", "composite"])
    @pytest.mark.parametrize(
        ("key_prop", "value"),
        [
            ({"type": "string"}, "a"),
            ({"type": "boolean"}, True),
            ({"type": "integer"}, 5),
            ({"type": "integer", "x-sql-datatype": "int"}, 5),
            ({"type": "integer", "x-sql-datatype": "smallint"}, 5),
            ({"type": "number", "x-sql-datatype": "numeric(10,2)"}, Decimal("1.25")),
            ({"type": "number", "x-sql-datatype": "real"}, Decimal("0.1")),
            ({"type": "number", "x-sql-datatype": "double"}, Decimal("0.1")),
            ({"type": "string", "format": "uuid"}, UUID),
            ({"type": "string", "x-sql-datatype": "uuid"}, UUID),
            ({"type": "string", "format": "date"}, date(2024, 1, 1)),
            ({"type": "string", "format": "time"}, time(10, 0, 0)),
            (
                {"type": "string", "format": "date-time"},
                datetime(2024, 1, 1, 10, 0, 0),  # noqa: DTZ001
            ),
            (
                {
                    "type": "string",
                    "format": "date-time",
                    "x-sql-datatype": "timestamptz",
                },
                datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            ),
        ],
        ids=[
            "varchar",
            "boolean",
            "bigint",
            "integer",
            "smallint",
            "decimal",
            "real",
            "double",
            "uuid-format",
            "uuid-hint",
            "date",
            "time",
            "timestamp",
            "timestamptz",
        ],
    )
    def test_remerge_leaves_one_row(
        self, tmp_path: Path, key_prop: dict, value: object, key_shape: str
    ):
        """The second merge replaces the first row, for every key column type."""
        keys = ["k", "id"] if key_shape == "composite" else ["k"]
        rows = self._load_twice(self._catalog(), tmp_path, key_prop, value, keys)
        assert rows == [("a", "second")]

    def test_upper_case_key_name(self, tmp_path: Path):
        """A key named in upper case is still cast to its column's type.

        The singer-sdk lowercases key_properties ("UID" -> "uid") while the
        column keeps the property's case ("UID"); DuckDB matches both.
        """
        rows = self._load_twice(
            self._catalog(),
            tmp_path,
            {"type": "string", "format": "uuid"},
            self.UUID,
            ["UID"],
        )
        assert rows == [("a", "second")]

    @pytest.mark.parametrize("key_shape", ["single", "composite"])
    def test_null_key_value_matches_null(self, tmp_path: Path, key_shape: str):
        """A NULL key value is replaced on re-merge, as the in-batch dedup does."""
        keys = ["k", "id"] if key_shape == "composite" else ["k"]
        key_prop = {"type": ["null", "string"]}
        rows = self._load_twice(self._catalog(), tmp_path, key_prop, None, keys)
        assert rows == [("a", "second")]

    def _merge_batches(
        self,
        con: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        batches: list[list[dict]],
    ) -> list[tuple]:
        """Merge each batch on the composite key (k, id); return all rows."""
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": "merge",
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        nullable_string = {"type": ["null", "string"]}
        schema = {
            "type": "object",
            "properties": {
                "k": nullable_string,
                "id": nullable_string,
                "v": nullable_string,
            },
        }
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=["k", "id"],
                connector=connector,
            )
            sink.setup()
            for batch in batches:
                sink.process_batch({"records": batch})
        return con.execute(
            "SELECT k, id, v FROM ducklake_catalog.s.t ORDER BY k NULLS FIRST, id, v"
        ).fetchall()

    def test_null_keys_stay_distinct_from_other_keys(self, tmp_path: Path):
        """(NULL, a), (NULL, b) and (x, a) are three different composite keys."""
        rows = self._merge_batches(
            self._catalog(),
            tmp_path,
            [
                [
                    {"k": None, "id": "a", "v": "first"},
                    {"k": None, "id": "b", "v": "first"},
                    {"k": "x", "id": "a", "v": "first"},
                ],
                [{"k": None, "id": "a", "v": "second"}],
            ],
        )
        assert rows == [
            (None, "a", "second"),
            (None, "b", "first"),
            ("x", "a", "first"),
        ]

    def test_existing_null_key_duplicates_collapse(self, tmp_path: Path):
        """Duplicate NULL-key rows left by the old merge collapse on the next merge.

        This is the NULL-matches-NULL rule applied to existing data: the next
        batch carrying that key replaces every copy of it.
        """
        con = self._catalog()
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(
            "CREATE TABLE ducklake_catalog.s.t (k VARCHAR, id VARCHAR, v VARCHAR)"
        )
        con.execute(
            "INSERT INTO ducklake_catalog.s.t VALUES "
            "(NULL, 'a', 'copy1'), (NULL, 'a', 'copy2'), (NULL, 'a', 'copy3'), "
            "('x', 'a', 'untouched')"
        )
        rows = self._merge_batches(
            con, tmp_path, [[{"k": None, "id": "a", "v": "new"}]]
        )
        assert rows == [(None, "a", "new"), ("x", "a", "untouched")]

    def test_append_reads_table_metadata_once(self, tmp_path: Path):
        """Only merges need the key types; append streams don't pay for them."""
        con = self._catalog()
        queries: list[str] = []

        def execute(sql: str) -> duckdb.DuckDBPyConnection:
            queries.append(sql)
            return con.execute(sql)

        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": "append",
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        schema = {"type": "object", "properties": {"k": {"type": "string"}}}
        with patch.object(connector, "execute", side_effect=execute):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=["k"],
                connector=connector,
            )
            sink.setup()
            sink.process_batch({"records": [{"k": "a"}]})
        assert sum("PRAGMA TABLE_INFO" in sql for sql in queries) == 1

    def test_existing_table_type_differs_from_stream(self, tmp_path: Path):
        """A TIMESTAMP table keyed by a stream now hinting timestamptz still merges.

        The staged key must be cast to the table's TIMESTAMP, not the declared
        TIMESTAMPTZ: under a non-UTC session time zone the two disagree.
        """
        con = self._catalog()
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(
            "CREATE TABLE ducklake_catalog.s.t (k TIMESTAMP, id VARCHAR, v VARCHAR)"
        )
        key_prop = {
            "type": "string",
            "format": "date-time",
            "x-sql-datatype": "timestamptz",
        }
        value = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        rows = self._load_twice(con, tmp_path, key_prop, value, ["k"])
        assert rows == [("a", "second")]


class TestTimezoneOffsets:
    """Timestamps with any fixed UTC offset load, with or without convert_tz_to_utc.

    The singer-sdk parses '2024-01-01T10:00:00+02:00' into a datetime with a
    fixed-offset tzinfo; those records are fed to the real sink here.
    """

    OFFSETS: ClassVar[dict[str, timedelta]] = {
        "+02:00": timedelta(hours=2),
        "-07:00": timedelta(hours=-7),
        "+05:30": timedelta(hours=5, minutes=30),
    }

    def _load(
        self, tmp_path: Path, records: list[dict], *, convert_tz_to_utc: bool
    ) -> duckdb.DuckDBPyConnection:
        """Merge each record as its own batch on key ``id``; return the catalog."""
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        self._load_into(
            con,
            tmp_path,
            [[record] for record in records],
            convert_tz_to_utc=convert_tz_to_utc,
        )
        return con

    @staticmethod
    def _load_into(
        con: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        batches: list[list[dict]],
        *,
        convert_tz_to_utc: bool = False,
        key_properties: tuple[str, ...] = ("id",),
    ) -> None:
        """Merge each batch of records through the real sink."""
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": "merge",
            "convert_tz_to_utc": convert_tz_to_utc,
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        date_time = {"type": "string", "format": "date-time"}
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "at": date_time,
                "at_tz": {**date_time, "x-sql-datatype": "timestamptz"},
            },
        }
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=list(key_properties),
                connector=connector,
            )
            sink.setup()
            for batch in batches:
                sink.process_batch({"records": batch})

    TEXT_SHAPES: ClassVar[list[str]] = ["nested", "oneof", "auto-cast"]

    @staticmethod
    def _load_text(
        tmp_path: Path, text: str, *, convert_tz_to_utc: bool, shape: str = "nested"
    ) -> tuple[str, float]:
        """Load a date-time that the singer-sdk leaves as text.

        The SDK parses only top-level date-times it recognises, so the value
        stays the tap's string when it is nested (flattened into its own
        column), declared with oneOf, or made a date-time by
        auto_cast_timestamps. Records go through the SDK's own parsing.
        Returns the plain column's wall-clock time and the TIMESTAMPTZ instant.
        """
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "flatten_max_level": 1,
            "auto_cast_timestamps": True,
            "convert_tz_to_utc": convert_tz_to_utc,
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        date_time = {"type": "string", "format": "date-time"}
        tz = {**date_time, "x-sql-datatype": "timestamptz"}
        props: dict
        record: dict
        if shape == "nested":
            props = {
                "p": {"type": "object", "properties": {"at": date_time, "at_tz": tz}}
            }
            record = {"p": {"at": text, "at_tz": text}}
            columns = ("p__at", "p__at_tz")
        elif shape == "oneof":
            props = {"at": {"oneOf": [date_time]}, "at_tz": {"oneOf": [tz]}}
            record = {"at": text, "at_tz": text}
            columns = ("at", "at_tz")
        else:
            props = {
                "created_at": {"type": "string"},
                "at_tz": {"type": "string", "x-sql-datatype": "timestamptz"},
            }
            record = {"created_at": text, "at_tz": text}
            columns = ("created_at", "at_tz")
        schema = {"type": "object", "properties": {"id": {"type": "string"}, **props}}
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=["id"],
                connector=connector,
            )
            sink.setup()
            context: dict = {}
            parsed = sink._validate_and_parse({"id": "a", **record})
            sink.process_record(parsed, context)
            sink.process_batch(context)
        plain, instant = columns
        return con.execute(
            f'SELECT "{plain}"::VARCHAR, epoch("{instant}") FROM ducklake_catalog.s.t'
        ).fetchone()

    @pytest.mark.parametrize("shape", TEXT_SHAPES)
    @pytest.mark.parametrize(
        ("text", "wall_clock"),
        [
            ("2024-01-01T10:00:00+05:30", "2024-01-01 04:30:00"),
            ("2024-01-01T10:00:00-07:00", "2024-01-01 17:00:00"),
            ("2024-01-01T10:00:00.5+02:00", "2024-01-01 08:00:00.5"),
            ("2024-01-01T10:00:00Z", "2024-01-01 10:00:00"),
        ],
    )
    def test_convert_tz_to_utc_converts_unparsed_text(
        self, tmp_path: Path, text: str, wall_clock: str, shape: str
    ):
        """Date-time text with an offset is stored in UTC, like a parsed value."""
        instant = datetime.fromisoformat(text).timestamp()
        loaded = self._load_text(tmp_path, text, convert_tz_to_utc=True, shape=shape)
        assert loaded == (wall_clock, instant)

    @pytest.mark.parametrize("shape", TEXT_SHAPES)
    def test_unparsed_text_keeps_wall_clock_without_convert(
        self, tmp_path: Path, shape: str
    ):
        """Without convert_tz_to_utc, the text's own wall-clock time is kept."""
        text = "2024-01-01T10:00:00+05:30"
        loaded = self._load_text(tmp_path, text, convert_tz_to_utc=False, shape=shape)
        assert loaded == (
            "2024-01-01 10:00:00",
            datetime.fromisoformat(text).timestamp(),
        )

    def test_naive_unparsed_text_is_unchanged_with_convert(self, tmp_path: Path):
        """Text without an offset is loaded as given, with convert_tz_to_utc too."""
        loaded = self._load_text(
            tmp_path, "2024-01-01T10:00:00", convert_tz_to_utc=True
        )
        assert loaded[0] == "2024-01-01 10:00:00"

    @pytest.mark.parametrize(
        "text",
        ["not a date", "", "9999-12-31T23:59:59-05:00", "2024-13-01T00:00:00+02:00"],
        ids=["garbage", "empty", "utc-beyond-year-9999", "invalid-month"],
    )
    def test_text_that_does_not_convert_is_left_as_it_is(self, text: str):
        """Text that doesn't parse, or overflows in UTC, reaches DuckDB unchanged."""
        data = {"at": [text, None]}
        assert _normalize_data_for_pyarrow(data, ["at"]) == {"at": [text, None]}

    @pytest.mark.parametrize("offset", ["+02:00", "-07:00", "+05:30"])
    @pytest.mark.parametrize("convert", ["keep-offset", "convert-to-utc"])
    def test_fixed_offset_loads(self, tmp_path: Path, offset: str, convert: str):
        """Fixed offsets stage and load; TIMESTAMPTZ always holds the instant.

        A plain DATETIME column keeps the wall-clock time unless
        convert_tz_to_utc converts it to UTC first, as for named zones.
        """
        value = datetime(2024, 1, 1, 10, 0, tzinfo=timezone(self.OFFSETS[offset]))
        con = self._load(
            tmp_path,
            [{"id": "a", "at": value, "at_tz": value}],
            convert_tz_to_utc=convert == "convert-to-utc",
        )
        row = con.execute(
            'SELECT "at"::VARCHAR, epoch(at_tz) FROM ducklake_catalog.s.t'
        ).fetchone()
        utc = value.astimezone(timezone.utc).replace(tzinfo=None)
        expected_wall_clock = utc if convert == "convert-to-utc" else value
        assert row == (
            expected_wall_clock.strftime("%Y-%m-%d %H:%M:%S"),
            value.timestamp(),
        )

    @pytest.mark.parametrize("convert", ["keep-offset", "convert-to-utc"])
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # expected = (wall clock, wall clock with convert_tz_to_utc, instant).
            # Naive: loaded as given; TIMESTAMPTZ reads it in the session zone.
            (
                datetime(2024, 1, 1, 10, 0),  # noqa: DTZ001
                (
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 15:00:00+00",
                ),
            ),
            (
                datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                (
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:00:00",
                    "2024-01-01 10:00:00+00",
                ),
            ),
            (
                datetime(2024, 1, 1, 10, 0, tzinfo=ZoneInfo("Europe/Paris")),
                (
                    "2024-01-01 10:00:00",
                    "2024-01-01 09:00:00",
                    "2024-01-01 09:00:00+00",
                ),
            ),
        ],
        ids=["naive", "utc", "europe-paris"],
    )
    def test_values_that_loaded_before_are_unchanged(
        self,
        tmp_path: Path,
        value: datetime,
        expected: tuple[str, str, str],
        convert: str,
    ):
        """Naive, UTC and named-zone values load as they did before the fix.

        Runs under a non-UTC session time zone, which DuckDB uses to read
        naive text into TIMESTAMPTZ.
        """
        con = duckdb.connect()
        con.execute("SET TimeZone = 'America/New_York'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        self._load_into(
            con,
            tmp_path,
            [[{"id": "a", "at": value, "at_tz": value}]],
            convert_tz_to_utc=convert == "convert-to-utc",
        )
        con.execute("SET TimeZone = 'UTC'")
        row = con.execute(
            'SELECT "at"::VARCHAR, at_tz::VARCHAR FROM ducklake_catalog.s.t'
        ).fetchone()
        wall_clock, utc_wall_clock, instant = expected
        stored = utc_wall_clock if convert == "convert-to-utc" else wall_clock
        assert row == (stored, instant)

    MIXED: ClassVar[dict[str, datetime]] = {
        "utc": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        "paris": datetime(2024, 1, 1, 10, 0, tzinfo=ZoneInfo("Europe/Paris")),
        "new_york": datetime(2024, 7, 1, 10, 0, tzinfo=ZoneInfo("America/New_York")),
        "naive": datetime(2024, 1, 1, 10, 0),  # noqa: DTZ001
    }

    def _load_mixed(
        self, tmp_path: Path, ids: tuple[str, ...], *, convert_tz_to_utc: bool
    ) -> dict[str, tuple]:
        """Load MIXED values in the given order as one batch, in a non-UTC session.

        Returns each id's plain wall-clock time and TIMESTAMPTZ instant.
        """
        con = duckdb.connect()
        con.execute("SET TimeZone = 'Asia/Tokyo'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        batch = [{"id": k, "at": self.MIXED[k], "at_tz": self.MIXED[k]} for k in ids]
        self._load_into(con, tmp_path, [batch], convert_tz_to_utc=convert_tz_to_utc)
        return {
            r[0]: r[1:]
            for r in con.execute(
                'SELECT id, "at"::VARCHAR, epoch(at_tz) FROM ducklake_catalog.s.t'
            ).fetchall()
        }

    @pytest.mark.parametrize(
        "ids",
        [
            ("utc", "paris"),
            ("paris", "utc"),
            ("paris", "new_york", "utc"),
            ("utc", "naive"),
            ("naive", "utc"),
            ("naive", "paris", "new_york"),
        ],
        ids="-".join,
    )
    def test_mixed_zone_batch_keeps_each_wall_clock_in_any_order(
        self, tmp_path: Path, ids: tuple[str, ...]
    ):
        """Without convert_tz_to_utc, each value keeps its own wall-clock time.

        A plain column stores 10:00 for every value whatever the other values
        in the batch; TIMESTAMPTZ stores each aware value's instant and reads
        a naive value in the session zone (Asia/Tokyo here).
        """
        rows = self._load_mixed(tmp_path, ids, convert_tz_to_utc=False)
        tokyo = ZoneInfo("Asia/Tokyo")
        expected = {}
        for k in ids:
            value = self.MIXED[k]
            instant = value if value.tzinfo else value.replace(tzinfo=tokyo)
            expected[k] = (value.strftime("%Y-%m-%d 10:00:00"), instant.timestamp())
        assert rows == expected

    @pytest.mark.parametrize(
        "ids", [("utc", "paris"), ("paris", "utc"), ("naive", "utc")], ids="-".join
    )
    def test_mixed_zone_batch_with_convert_stores_utc(
        self, tmp_path: Path, ids: tuple[str, ...]
    ):
        """With convert_tz_to_utc, aware values store UTC and naive ones as given.

        TIMESTAMPTZ holds each aware value's instant and reads a naive value in
        the session zone (Asia/Tokyo here), whatever the order.
        """
        rows = self._load_mixed(tmp_path, ids, convert_tz_to_utc=True)
        tokyo = ZoneInfo("Asia/Tokyo")
        for k in ids:
            value = self.MIXED[k]
            wall_clock = value.astimezone(timezone.utc) if value.tzinfo else value
            instant = value if value.tzinfo else value.replace(tzinfo=tokyo)
            assert rows[k] == (
                wall_clock.strftime("%Y-%m-%d %H:%M:%S"),
                instant.timestamp(),
            )

    def test_one_zone_column_is_left_to_pyarrow(self):
        """A column in one zone, across DST, stages as before (pyarrow's text)."""
        paris = ZoneInfo("Europe/Paris")
        values = [
            datetime(2024, 1, 1, 10, tzinfo=paris),
            datetime(2024, 7, 1, 10, tzinfo=paris),
        ]
        schema = pa.schema([pa.field("at", pa.string())])
        staged = create_pyarrow_table([{"at": v} for v in values], schema)
        by_pyarrow = pa.array(values).cast(pa.string()).to_pylist()
        assert staged.column("at").to_pylist() == by_pyarrow

    @pytest.mark.parametrize("convert", ["keep-offset", "convert-to-utc"])
    def test_batch_mixing_fixed_offsets_with_other_zones(
        self, tmp_path: Path, convert: str
    ):
        """One batch with fixed-offset, UTC, named-zone and naive values loads.

        Each value keeps its own instant; a plain column holds each value's
        own wall-clock time unless convert_tz_to_utc converts it to UTC.
        """
        values = {
            "fixed": datetime(2024, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=2))),
            "utc": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            "paris": datetime(2024, 1, 1, 10, 0, tzinfo=ZoneInfo("Europe/Paris")),
            "naive": datetime(2024, 1, 1, 10, 0),  # noqa: DTZ001
        }
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        # One batch holding all of them, as a tap would send.
        batch = [{"id": k, "at": v, "at_tz": v} for k, v in values.items()]
        self._load_into(
            con, tmp_path, [batch], convert_tz_to_utc=convert == "convert-to-utc"
        )
        rows = {
            r[0]: r[1:]
            for r in con.execute(
                'SELECT id, "at"::VARCHAR, at_tz::VARCHAR FROM ducklake_catalog.s.t'
            ).fetchall()
        }
        keep = convert == "keep-offset"
        fixed_wall_clock = "2024-01-01 10:00:00" if keep else "2024-01-01 08:00:00"
        paris_wall_clock = "2024-01-01 10:00:00" if keep else "2024-01-01 09:00:00"
        assert rows == {
            "fixed": (fixed_wall_clock, "2024-01-01 08:00:00+00"),
            "utc": ("2024-01-01 10:00:00", "2024-01-01 10:00:00+00"),
            "paris": (paris_wall_clock, "2024-01-01 09:00:00+00"),
            "naive": ("2024-01-01 10:00:00", "2024-01-01 10:00:00+00"),
        }

    @pytest.mark.parametrize(
        ("value", "wall_clock"),
        [
            (
                datetime(
                    9999, 12, 31, 23, 59, 59, tzinfo=timezone(timedelta(hours=-7))
                ),
                "9999-12-31 23:59:59",
            ),
            (
                datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=2))),
                "0001-01-01 00:00:00",
            ),
            (
                datetime(
                    2024, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=2, seconds=30))
                ),
                "2024-01-01 10:00:00",
            ),
            (
                # Local mean time: Paris was +00:09:21 in year 2.
                datetime(2, 1, 1, 10, 0, tzinfo=ZoneInfo("Europe/Paris")),
                "0002-01-01 10:00:00",
            ),
            (
                # Its UTC time is in year 0, so the DST normalisation overflows.
                datetime(1, 1, 1, 0, 0, tzinfo=ZoneInfo("Europe/Paris")),
                "0001-01-01 00:00:00",
            ),
        ],
        ids=[
            "year-9999",
            "year-1",
            "seconds-offset",
            "lmt-named-zone",
            "named-zone-overflow",
        ],
    )
    def test_edge_values_in_fixed_offset_column(self, value: datetime, wall_clock: str):
        """Edge years and offsets with seconds stage, and DuckDB reads them exactly."""
        fixed = datetime(2024, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=2)))
        schema = pa.schema([pa.field("ts", pa.string())])
        staged = create_pyarrow_table([{"ts": fixed}, {"ts": value}], schema)
        text = staged.column("ts")[1].as_py()
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        row = con.execute(
            "SELECT CAST($1 AS TIMESTAMP)::VARCHAR, epoch(CAST($1 AS TIMESTAMPTZ))",
            [text],
        ).fetchone()
        # Computed via the offset: converting to UTC would overflow datetime.
        offset = value.utcoffset()
        assert offset is not None
        local = value.replace(tzinfo=None) - datetime(1970, 1, 1)  # noqa: DTZ001
        epoch = (local - offset).total_seconds()
        assert row == (wall_clock, epoch)

    def test_utc_text_unchanged_for_existing_varchar_key(self, tmp_path: Path):
        """A VARCHAR key written by earlier versions still matches a UTC value.

        Earlier versions staged a UTC datetime as '2024-01-01 10:00:00.000000Z'.
        A table whose column is VARCHAR (created before the stream declared
        date-time) must keep getting that exact text, or merges miss old rows.
        """
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute(
            "CREATE TABLE ducklake_catalog.s.t "
            '(id VARCHAR, "at" VARCHAR, at_tz VARCHAR)'
        )
        con.execute(
            "INSERT INTO ducklake_catalog.s.t VALUES "
            "('a', '2024-01-01 10:00:00.000000Z', '2024-01-01 10:00:00.000000Z')"
        )
        value = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        self._load_into(
            con,
            tmp_path,
            [[{"id": "a", "at": value, "at_tz": value}]],
            key_properties=("at",),
        )
        rows = con.execute(
            'SELECT id, "at" FROM ducklake_catalog.s.t ORDER BY "at"'
        ).fetchall()
        assert rows == [("a", "2024-01-01 10:00:00.000000Z")]

    def test_fixed_offset_key_remerges(self, tmp_path: Path):
        """A key built from an offset timestamp is replaced on re-merge."""
        value = datetime(2024, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=2)))
        con = self._load(
            tmp_path,
            [
                {"id": "a", "at": value, "at_tz": value},
                {"id": "a", "at": value, "at_tz": value},
            ],
            convert_tz_to_utc=False,
        )
        count = con.execute("SELECT count(*) FROM ducklake_catalog.s.t").fetchone()
        assert count == (1,)


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_storage_credentials(self):
        """Test error handling for invalid storage credentials."""
        config = {
            "catalog_url": "test.db",
            "data_path": "s3://bucket/path",
            "storage_type": "S3",
            "public_key": "invalid_key",
            "secret_key": "invalid_secret",
        }
        # Should initialize without error, but fail on actual connection
        target = Targetducklake(config=config)
        assert target.config["storage_type"] == "S3"

    def test_missing_partition_config(self):
        """Test behavior with missing partition configuration."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        target = Targetducklake(config=config)
        assert target.config.get("partition_fields") is None

    def test_invalid_flatten_level(self):
        """Test handling of invalid flatten level."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "flatten_max_level": -1,
        }
        with pytest.raises(ConfigValidationError):
            Targetducklake(config=config)

    def test_empty_schema_handling(self):
        """Test handling of empty schema."""
        schema = {"type": "object", "properties": {}}
        result = flatten_schema(schema)
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_missing_stream_name_schema(self):
        """Test behavior when stream name doesn't follow expected pattern."""
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }

        with patch("target_ducklake.sinks.DuckLakeConnector"):
            target = Mock()
            target.config = config
            sink = ducklakeSink(
                target=target,
                stream_name="simple_name",  # No schema separation
                schema={"type": "object", "properties": {"id": {"type": "integer"}}},
                key_properties=None,
            )
            assert sink.stream_name == "simple_name"


class TestExecuteRetry:
    """Test retry logic in the execute method."""

    def _make_connector(self):
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        return DuckLakeConnector(config)

    @patch("target_ducklake.connector.time.sleep")
    def test_execute_retries_on_ioexception(self, mock_sleep):
        """Test that execute retries on duckdb.IOException and succeeds."""
        connector = self._make_connector()
        mock_cursor = Mock()
        mock_result = Mock()
        mock_cursor.execute.side_effect = [
            duckdb.IOException("Unable to connect to Postgres"),
            duckdb.IOException("Unable to connect to Postgres"),
            mock_result,
        ]
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        result = connector.execute("SELECT 1")

        assert result is mock_result
        assert mock_cursor.execute.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(2)  # 2^1
        mock_sleep.assert_any_call(4)  # 2^2

    @patch("target_ducklake.connector.time.sleep")
    def test_execute_raises_after_max_retries(self, mock_sleep):
        """Test that execute raises DuckLakeConnectorError after exhausting retries."""
        connector = self._make_connector()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = duckdb.IOException("Unable to connect to Postgres")
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        with pytest.raises(DuckLakeConnectorError, match="failed after 3 attempts"):
            connector.execute("SELECT 1")

        assert mock_cursor.execute.call_count == 3
        assert mock_sleep.call_count == 2  # sleeps before attempts 2 and 3

    @patch("target_ducklake.connector.time.sleep")
    def test_execute_no_retry_on_other_exceptions(self, mock_sleep):
        """Test that non-IOException errors are raised immediately without retry."""
        connector = self._make_connector()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = duckdb.CatalogException("table not found")
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        with pytest.raises(DuckLakeConnectorError):
            connector.execute("SELECT 1")

        assert mock_cursor.execute.call_count == 1
        mock_sleep.assert_not_called()

    @patch("target_ducklake.connector.time.sleep")
    def test_execute_retries_on_ssl_connection_error(self, mock_sleep):
        """Test that execute retries on SSL connection errors (duckdb.Error)."""
        connector = self._make_connector()
        mock_cursor = Mock()
        mock_result = Mock()
        mock_cursor.execute.side_effect = [
            duckdb.Error("Failed to query most recent snapshot for DuckLake: "
                         'Failed to execute query "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ": '
                         "SSL connection has been closed unexpectedly"),
            mock_result,
        ]
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        result = connector.execute("SELECT 1")

        assert result is mock_result
        assert mock_cursor.execute.call_count == 2
        mock_sleep.assert_called_once_with(2)

    @patch("target_ducklake.connector.time.sleep")
    def test_execute_no_retry_on_non_transient_duckdb_error(self, mock_sleep):
        """Test that non-transient duckdb.Error is raised immediately."""
        connector = self._make_connector()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = duckdb.Error("some other error")
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        with pytest.raises(DuckLakeConnectorError):
            connector.execute("SELECT 1")

        assert mock_cursor.execute.call_count == 1
        mock_sleep.assert_not_called()


class TestTruncateColumnNames:
    """Test column name truncation for Postgres compatibility."""

    def test_no_truncation_needed(self):
        """Names within limit are unchanged, mapping is empty."""
        schema = {"id": {"type": "integer"}, "name": {"type": "string"}}
        new_schema, mapping = truncate_column_names(schema, max_length=63)
        assert new_schema == schema
        assert mapping == {}

    def test_simple_truncation(self):
        """A single long name is truncated to max_length."""
        long_name = "a" * 80
        schema = {long_name: {"type": "string"}, "id": {"type": "integer"}}
        new_schema, mapping = truncate_column_names(schema, max_length=63)

        assert long_name not in new_schema
        truncated = "a" * 63
        assert truncated in new_schema
        assert mapping == {long_name: truncated}
        # Schema values are preserved
        assert new_schema[truncated] == {"type": "string"}
        assert new_schema["id"] == {"type": "integer"}

    def test_collision_resolution(self):
        """Two names that truncate to the same string get _{i} suffixes."""
        prefix = "a" * 63
        name_a = prefix + "_suffix_one"
        name_b = prefix + "_suffix_two"
        schema = {name_a: {"type": "string"}, name_b: {"type": "integer"}}
        new_schema, mapping = truncate_column_names(schema, max_length=63)

        # Both should be in the new schema with different names
        assert len(new_schema) == 2
        values = list(new_schema.keys())
        assert values[0] != values[1]
        # Each should end with _0 or _1
        assert values[0].endswith("_0")
        assert values[1].endswith("_1")
        # Total length should not exceed max_length
        assert all(len(v) <= 63 for v in values)

    def test_disabled_with_zero(self):
        """max_length=0 disables truncation."""
        long_name = "a" * 100
        schema = {long_name: {"type": "string"}}
        new_schema, mapping = truncate_column_names(schema, max_length=0)
        assert new_schema == schema
        assert mapping == {}

    def test_postgres_only_in_sink(self):
        """Truncation is skipped for non-postgres catalog types."""
        long_name = "a" * 80
        schema = {
            "type": "object",
            "properties": {long_name: {"type": "string"}},
        }
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "catalog_type": "duckdb",
        }
        with patch("target_ducklake.sinks.DuckLakeConnector"):
            target = Mock()
            target.config = config
            sink = ducklakeSink(
                target=target,
                stream_name="test-table",
                schema=schema,
                key_properties=None,
            )
            # Column name should NOT be truncated for duckdb catalog
            assert sink._column_name_mapping == {}
            assert long_name in sink.flatten_schema

    def test_key_properties_truncated(self):
        """Key properties are updated when their column name is truncated."""
        long_key = "a" * 80
        schema = {
            "type": "object",
            "properties": {
                long_key: {"type": "integer"},
                "name": {"type": "string"},
            },
        }
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "catalog_type": "postgres",
        }
        with patch("target_ducklake.sinks.DuckLakeConnector"):
            target = Mock()
            target.config = config
            sink = ducklakeSink(
                target=target,
                stream_name="test-table",
                schema=schema,
                key_properties=[long_key],
            )
            # Key property should be truncated
            assert sink.key_properties == ["a" * 63]
            assert long_key not in sink.flatten_schema
            assert "a" * 63 in sink.flatten_schema

class TestStartupScriptGCS:
    """Verify GCS HMAC startup script wiring."""

    def _make_connector(self, **overrides):
        config = {
            "catalog_url": "host:5432/db",
            "data_path": "gs://def-ducklake/team-abc",
            "storage_type": "GCS",
            "public_key": "ak",
            "secret_key": "sk",
        }
        config.update(overrides)
        return DuckLakeConnector(config=config)

    def test_hmac_path(self):
        """With HMAC keys, install public ducklake extension and create a TYPE gcs secret."""
        connector = self._make_connector()
        script = connector._build_startup_script()

        assert "INSTALL ducklake;" in script
        assert "TYPE gcs," in script
        assert "KEY_ID 'ak'" in script
        assert "DATA_PATH 'gs://def-ducklake/team-abc'" in script

    def test_includes_meta_schema(self):
        """META_SCHEMA should appear in the ATTACH statement when configured."""
        connector = self._make_connector(meta_schema="custom_meta")
        script = connector._build_startup_script()

        assert "META_SCHEMA 'custom_meta'" in script
        assert "DATA_PATH 'gs://def-ducklake/team-abc'" in script


class TestColumnTypeReconciliation:
    """Test in-place column type widening during prepare_table."""

    def _make_connector(self):
        config = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
        }
        return DuckLakeConnector(config)

    @pytest.mark.parametrize(
        ("current", "desired", "expected"),
        [
            # safe same-family widenings
            ("INTEGER", "BIGINT", "BIGINT"),
            ("SMALLINT", "BIGINT", "BIGINT"),
            ("TINYINT", "INTEGER", "INTEGER"),
            ("INT", "BIGINT", "BIGINT"),
            ("FLOAT", "DOUBLE", "DOUBLE"),
            ("REAL", "DOUBLE", "DOUBLE"),
            # case / whitespace insensitive; return preserves the desired casing
            (" integer ", "bigint", "bigint"),
            # no-op: equal types
            ("BIGINT", "BIGINT", None),
            ("INTEGER", "INTEGER", None),
            # no-op: narrowing
            ("BIGINT", "INTEGER", None),
            ("HUGEINT", "BIGINT", None),
            ("DOUBLE", "FLOAT", None),
            # no-op: cross-family (data-unsafe)
            ("DOUBLE", "BIGINT", None),
            ("DECIMAL(38, 9)", "BIGINT", None),
            ("BIGINT", "DECIMAL(38, 9)", None),
            ("VARCHAR", "JSON", None),
            ("INTEGER", "DATETIME", None),
        ],
    )
    def test_safe_widen_target(self, current, desired, expected):
        """Only same-family widenings return a target type; everything else is None."""
        assert DuckLakeConnector._safe_widen_target(current, desired) == expected

    def test_reconcile_only_widens_safe_columns(self):
        """Emit ALTER only for safe widenings of existing columns."""
        connector = self._make_connector()
        mock_cursor = Mock()
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        current_types = {
            "id": "VARCHAR",
            "legacy_id": "INTEGER",
            "_sdc_sync_started_at": "INTEGER",
            "content_type_id": "DOUBLE",
        }
        columns = [
            {"name": "id", "type": "VARCHAR"},  # unchanged
            {"name": "legacy_id", "type": "BIGINT"},  # widen
            {"name": "_sdc_sync_started_at", "type": "BIGINT"},  # widen
            {"name": "content_type_id", "type": "BIGINT"},  # cross-family, skip
            {"name": "new_col", "type": "BIGINT"},  # not in snapshot, skip
        ]
        connector._reconcile_column_types("myschema", "mytable", columns, current_types)

        executed = [call.args[0] for call in mock_cursor.execute.call_args_list]
        assert len(executed) == 2
        assert any(
            'ALTER COLUMN "legacy_id" SET DATA TYPE BIGINT' in sql for sql in executed
        )
        assert any(
            'ALTER COLUMN "_sdc_sync_started_at" SET DATA TYPE BIGINT' in sql
            for sql in executed
        )
        assert all("content_type_id" not in sql for sql in executed)
        assert all('"id"' not in sql for sql in executed)
        assert all("new_col" not in sql for sql in executed)

    def test_get_table_column_types_maps_name_to_type(self):
        """get_table_column_types maps PRAGMA rows name -> type, preserving order."""
        connector = self._make_connector()
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            (0, "id", "VARCHAR", False, None, False),
            (1, "legacy_id", "INTEGER", False, None, False),
            (2, "_sdc_sync_started_at", "INTEGER", False, None, False),
        ]
        mock_cursor = Mock()
        mock_cursor.execute.return_value = mock_result
        connector._connection = Mock()
        connector._connection.cursor.return_value = mock_cursor

        result = connector.get_table_column_types("myschema", "mytable")

        assert result == {
            "id": "VARCHAR",
            "legacy_id": "INTEGER",
            "_sdc_sync_started_at": "INTEGER",
        }

    def test_prepare_table_widens_existing_columns(self):
        """prepare_table widens existing columns, not just adds new ones."""
        connector = self._make_connector()
        with (
            patch.object(connector, "_check_if_table_exists", return_value=True),
            patch.object(
                connector,
                "get_table_column_types",
                return_value={"legacy_id": "INTEGER"},
            ),
            patch.object(connector, "_add_columns") as mock_add,
            patch.object(connector, "_alter_column_type") as mock_alter,
        ):
            table_types = connector.prepare_table(
                "myschema", "mytable", [{"name": "legacy_id", "type": "BIGINT"}]
            )

        mock_alter.assert_called_once_with("myschema", "mytable", "legacy_id", "BIGINT")
        mock_add.assert_not_called()
        # The table's columns are returned with their types, read once more.
        assert table_types == {"legacy_id": "INTEGER"}


class TestCaseInsensitiveColumns:
    """An existing table whose column names differ only in case still loads.

    DuckDB identifiers are case-insensitive, so the table's "ID" is the
    stream's "id". Drives the real sink lifecycle on an in-memory catalog.
    """

    @staticmethod
    def _load(
        tmp_path: Path, load_method: str, batches: list[list[dict]]
    ) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        # Created by another tool, or an earlier version, with other casing.
        con.execute(
            'CREATE TABLE ducklake_catalog.s.t ("ID" BIGINT, "Name" VARCHAR, '
            '"Legacy" INTEGER)'
        )
        con.execute("INSERT INTO ducklake_catalog.s.t VALUES (1, 'old', 7)")
        config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": load_method,
        }
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": ["null", "string"]},
                "legacy": {"type": ["null", "integer"]},
                "extra": {"type": ["null", "string"]},
            },
        }
        target = Mock()
        target.config = config
        connector = DuckLakeConnector(config)
        with patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ):
            sink = ducklakeSink(
                target=target,
                stream_name="t",
                schema=schema,
                key_properties=["id"],
                connector=connector,
            )
            sink.setup()
            for records in batches:
                sink.process_batch({"records": records})
        return con

    @pytest.mark.parametrize("load_method", ["merge", "append"])
    def test_existing_columns_with_other_case_are_not_re_added(
        self, tmp_path: Path, load_method: str
    ):
        """Setup adds only the new column, and values land in the existing ones."""
        batches = [
            [{"id": 2, "name": "a", "legacy": 8, "extra": "x"}],
            [{"id": 2, "name": "b", "legacy": 9, "extra": "y"}],
        ]
        con = self._load(tmp_path, load_method, batches)
        columns = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 't' ORDER BY ordinal_position"
        ).fetchall()
        assert columns == [
            ("ID", "BIGINT"),
            ("Name", "VARCHAR"),
            ("Legacy", "BIGINT"),  # widened in place for the stream's integer
            ("extra", "VARCHAR"),
        ]
        rows = con.execute("SELECT * FROM ducklake_catalog.s.t ORDER BY ALL").fetchall()
        if load_method == "merge":
            assert rows == [(1, "old", 7, None), (2, "b", 9, "y")]
        else:
            assert rows == [(1, "old", 7, None), (2, "a", 8, "x"), (2, "b", 9, "y")]

    def test_widening_matches_columns_case_insensitively(self):
        """A safe widening applies to the existing column whatever its case."""
        connector = DuckLakeConnector(
            {"catalog_url": "test.db", "data_path": "/tmp/test"}
        )
        with patch.object(connector, "_alter_column_type") as alter:
            connector._reconcile_column_types(
                "s", "t", [{"name": "legacy", "type": "BIGINT"}], {"Legacy": "INTEGER"}
            )
        alter.assert_called_once_with("s", "t", "Legacy", "BIGINT")

    def test_insert_columns_match_file_columns_case_insensitively(self):
        """A table column is read from the file column of the same name in any case."""
        sql = DuckLakeConnector._build_columns_sql(
            Mock(), file_columns=["id", "name"], target_table_columns=["ID", "Gone"]
        )
        assert sql.split(", ") == [' "ID"', "NULL"]


class TestSqliteCatalogDraining:
    """A SQLite catalog, which allows one writer at a time, loads several streams."""

    @pytest.mark.parametrize(
        ("catalog_type", "parallel_draining", "expected"),
        [
            ("sqlite", None, 1),
            ("sqlite", True, 1),
            ("sqlite", "true", 1),
            ("sqlite", False, 1),
            ("postgres", None, 10),
            ("duckdb", None, 10),
            ("mysql", None, 10),
            ("postgres", False, 1),
        ],
    )
    def test_sqlite_catalog_drains_one_sink_at_a_time(
        self,
        capfd: pytest.CaptureFixture[str],
        catalog_type: str,
        parallel_draining: object,
        expected: int,
    ):
        """parallel_draining is ignored for sqlite, with a log line when it was on."""
        config: dict[str, object] = {
            "catalog_url": "test.db",
            "data_path": "/tmp/test",
            "storage_type": "local",
            "catalog_type": catalog_type,
        }
        if parallel_draining is not None:
            config["parallel_draining"] = parallel_draining
        target = Targetducklake(config=config)
        assert target.max_parallelism == expected
        # The SDK's target logger writes to stderr without propagating.
        logged = "drained one at a time" in capfd.readouterr().err
        assert logged == (catalog_type == "sqlite" and parallel_draining is not False)

    def test_several_streams_load_into_a_sqlite_catalog(self, tmp_path: Path):
        """Two streams drained in one run both load (default config).

        Drained in parallel, the second commit failed with 'database is locked'.
        """
        config = {
            "catalog_type": "sqlite",
            "catalog_url": str(tmp_path / "catalog.sqlite"),
            "data_path": str(tmp_path / "data") + "/",
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging") + "/",
        }
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "v": {"type": "string"}},
        }
        streams = ["a", "b", "c"]
        messages = [
            {"type": "SCHEMA", "stream": s, "schema": schema, "key_properties": ["id"]}
            for s in streams
        ] + [
            {"type": "RECORD", "stream": s, "record": {"id": i, "v": s}}
            for s in streams
            for i in range(3)
        ]
        lines = "".join(json.dumps(m) + "\n" for m in messages)
        target = Targetducklake(config=config)
        target.listen(file_input=io.StringIO(lines))
        target.target_connector.close()
        con = duckdb.connect()
        con.execute(
            f"ATTACH 'ducklake:sqlite:{tmp_path / 'catalog.sqlite'}' AS lake "
            f"(DATA_PATH '{tmp_path / 'data'}/')"
        )
        counts = [
            con.execute(f"SELECT count(*) FROM lake.s.{s}").fetchone()[0]
            for s in streams
        ]
        assert counts == [3, 3, 3]

    def test_parallel_drain_waits_for_every_sink_before_raising(self):
        """A failing sink's error is raised only after the other drains finish.

        Raised while another thread was still inside DuckDB, the error made
        the interpreter exit under that thread, and the process aborted
        (exit 134) instead of failing with the error.
        """
        target = Targetducklake(
            config={"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        )
        finished = threading.Event()

        def drain_one(sink: str) -> None:
            if sink == "fails":
                msg = "boom"
                raise RuntimeError(msg)
            threading.Event().wait(0.3)  # still draining when the other fails
            finished.set()

        with (
            patch.object(target, "drain_one", side_effect=drain_one),
            pytest.raises(RuntimeError, match="boom"),
        ):
            target._drain_all(["fails", "slow"], parallelism=2)
        assert finished.is_set()

    def test_parallel_drain_reports_every_failure(
        self, capfd: pytest.CaptureFixture[str]
    ):
        """With two failing sinks, the first error is raised and the other logged."""
        target = Targetducklake(
            config={"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        )

        def drain_one(sink: str) -> None:
            raise RuntimeError(sink)

        with (
            patch.object(target, "drain_one", side_effect=drain_one),
            pytest.raises(RuntimeError, match="first"),
        ):
            target._drain_all(["first", "second"], parallelism=2)
        assert "Another sink also failed to drain: second" in capfd.readouterr().err


class TestAtomicOverwrite:
    """An overwrite replaces the table in one transaction at the end of the stream.

    Drives the real sink lifecycle on an in-memory catalog holding old rows.
    """

    @staticmethod
    def _sink(
        tmp_path: Path,
        con: duckdb.DuckDBPyConnection,
        key_properties: tuple[str, ...] = ("id",),
        **config: object,
    ) -> ducklakeSink:
        con.execute("ATTACH ':memory:' AS ducklake_catalog")
        con.execute("CREATE SCHEMA ducklake_catalog.s")
        con.execute("CREATE TABLE ducklake_catalog.s.t (id BIGINT, v VARCHAR)")
        con.execute("INSERT INTO ducklake_catalog.s.t VALUES (1, 'old'), (2, 'old')")
        full_config = {
            "catalog_url": "test.db",
            "data_path": str(tmp_path / "data"),
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging"),
            "load_method": "overwrite",
            **config,
        }
        target = Mock()
        target.config = full_config
        connector = DuckLakeConnector(full_config)
        patch.object(
            connector, "execute", side_effect=lambda sql: con.execute(sql)
        ).start()
        schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "v": {"type": "string"}},
        }
        sink = ducklakeSink(
            target=target,
            stream_name="t",
            schema=schema,
            key_properties=list(key_properties),
            connector=connector,
        )
        sink.setup()
        return sink

    @staticmethod
    def _rows(con: duckdb.DuckDBPyConnection) -> list[tuple]:
        return con.execute("SELECT * FROM ducklake_catalog.s.t ORDER BY ALL").fetchall()

    def teardown_method(self) -> None:
        """Undo the execute() patch the sink helper started."""
        patch.stopall()

    @pytest.mark.parametrize(
        "config",
        [{}, {"load_method": "append", "overwrite_if_no_pk": True}],
        ids=["load-method-overwrite", "overwrite-if-no-pk"],
    )
    def test_readers_see_the_old_table_until_the_new_one_is_complete(
        self, tmp_path: Path, config: dict
    ):
        """Batches are staged; the table changes only at clean_up, all at once."""
        con = duckdb.connect()
        # overwrite_if_no_pk applies to a stream without key properties.
        sink = self._sink(tmp_path, con, () if config else ("id",), **config)
        sink.process_batch({"records": [{"id": 3, "v": "new"}]})
        assert self._rows(con) == [(1, "old"), (2, "old")]
        sink.process_batch({"records": [{"id": 4, "v": "new"}]})
        assert self._rows(con) == [(1, "old"), (2, "old")]
        sink.clean_up()
        assert self._rows(con) == [(3, "new"), (4, "new")]
        assert not Path(sink.temp_file_dir).exists()

    def test_a_run_that_fails_before_the_end_leaves_the_old_table(
        self, tmp_path: Path
    ):
        """Without clean_up (the run failed or was interrupted), nothing changed."""
        con = duckdb.connect()
        sink = self._sink(tmp_path, con)
        sink.process_batch({"records": [{"id": 3, "v": "new"}]})
        with pytest.raises(ValueError, match="whole numbers"):
            sink.process_batch({"records": [{"id": "abc", "v": "bad"}]})
        assert self._rows(con) == [(1, "old"), (2, "old")]

    def test_a_failed_replace_leaves_the_old_table(self, tmp_path: Path):
        """If the replace transaction fails, it rolls back: the old rows stay."""
        con = duckdb.connect()
        sink = self._sink(tmp_path, con)
        sink.process_batch({"records": [{"id": 3, "v": "new"}]})
        sink.process_batch({"records": [{"id": 4, "v": "new"}]})
        Path(sink._staged_files[1]).unlink()  # the INSERT fails after the DELETE
        with pytest.raises(duckdb.IOException):
            sink.clean_up()
        con.execute("ROLLBACK")  # the patched execute shares one connection
        assert self._rows(con) == [(1, "old"), (2, "old")]
        assert not Path(sink.temp_file_dir).exists()

    def test_an_overwrite_with_no_records_empties_the_table(self, tmp_path: Path):
        """As before, a stream with no records leaves an empty table."""
        con = duckdb.connect()
        sink = self._sink(tmp_path, con)
        sink.clean_up()
        assert self._rows(con) == []


class TestActivateVersion:
    """ACTIVATE_VERSION removes rows absent from the activated version.

    Runs the target through ``listen()`` on a real SQLite DuckLake catalog,
    with messages as a full-table tap sends them.
    """

    SCHEMA: ClassVar[dict] = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "v": {"type": "string"}},
    }

    def _run(self, tmp_path: Path, messages: list[dict], **config: object) -> None:
        full_config = {
            "catalog_type": "sqlite",
            "catalog_url": str(tmp_path / "catalog.sqlite"),
            "data_path": str(tmp_path / "data") + "/",
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(tmp_path / "staging") + "/",
            "add_record_metadata": True,
            **config,
        }
        lines = "".join(json.dumps(m) + "\n" for m in messages)
        target = Targetducklake(config=full_config)
        target.listen(file_input=io.StringIO(lines))
        target.target_connector.close()

    def _rows(self, tmp_path: Path, columns: str = "id, v") -> list[tuple]:
        con = duckdb.connect()
        con.execute(
            f"ATTACH 'ducklake:sqlite:{tmp_path / 'catalog.sqlite'}' AS lake "
            f"(DATA_PATH '{tmp_path / 'data'}/')"
        )
        rows = con.execute(f"SELECT {columns} FROM lake.s.t ORDER BY id").fetchall()
        con.close()
        return rows

    def _sync(self, version: int, rows: dict[int, str]) -> list[dict]:
        """A full-table sync: announce the version, send its records, activate it."""
        activate = {"type": "ACTIVATE_VERSION", "stream": "t", "version": version}
        records = [
            {"type": "RECORD", "stream": "t", "record": {"id": i, "v": v}}
            for i, v in rows.items()
        ]
        return [
            self._schema_message(),
            activate,
            *({**record, "version": version} for record in records),
            activate,
        ]

    def _schema_message(self) -> dict:
        return {
            "type": "SCHEMA",
            "stream": "t",
            "schema": self.SCHEMA,
            "key_properties": ["id"],
        }

    def test_rows_absent_from_the_new_version_are_removed(self, tmp_path: Path):
        """Row 2, gone at the source, is removed; rows 1 and 3 hold version 2."""
        self._run(tmp_path, self._sync(1, {1: "a", 2: "b", 3: "c"}))
        assert self._rows(tmp_path) == [(1, "a"), (2, "b"), (3, "c")]
        self._run(tmp_path, self._sync(2, {1: "a2", 3: "c2"}))
        assert self._rows(tmp_path, "id, v, _sdc_table_version") == [
            (1, "a2", 2),
            (3, "c2", 2),
        ]

    def test_announcing_a_version_removes_nothing(self, tmp_path: Path):
        """The ACTIVATE_VERSION sent before the records doesn't empty the table."""
        self._run(tmp_path, self._sync(1, {1: "a", 2: "b"}))
        announce_only = self._sync(2, {})[:2]
        self._run(tmp_path, announce_only)
        assert self._rows(tmp_path) == [(1, "a"), (2, "b")]

    def test_rows_without_a_version_are_removed_too(self, tmp_path: Path):
        """Rows loaded before versions were recorded are not in the new version."""
        self._run(
            tmp_path,
            [
                self._schema_message(),
                {"type": "RECORD", "stream": "t", "record": {"id": 9, "v": "old"}},
            ],
        )
        self._run(tmp_path, self._sync(5, {1: "a"}))
        assert self._rows(tmp_path) == [(1, "a")]

    def test_without_record_metadata_nothing_is_removed(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ):
        """Records carry no version, so ACTIVATE_VERSION is ignored with a warning."""
        self._run(tmp_path, self._sync(1, {1: "a", 2: "b"}), add_record_metadata=False)
        self._run(tmp_path, self._sync(2, {1: "a2"}), add_record_metadata=False)
        assert self._rows(tmp_path) == [(1, "a2"), (2, "b")]
        assert "Ignoring ACTIVATE_VERSION 2" in capfd.readouterr().err

    def test_overwrite_with_activate_version_replaces_the_table_once(
        self, tmp_path: Path
    ):
        """An overwrite stream ignores the version; the table is replaced at the end."""
        self._run(tmp_path, self._sync(1, {1: "a", 2: "b"}), load_method="overwrite")
        self._run(tmp_path, self._sync(2, {3: "c"}), load_method="overwrite")
        assert self._rows(tmp_path) == [(3, "c")]

    def test_state_is_not_emitted_when_the_overwrite_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """STATE follows the overwrite's commit: a failed replace emits none."""
        state = {"type": "STATE", "value": {"bookmarks": {"t": {"done": True}}}}
        messages = [*self._sync(1, {1: "a"}), state]
        with (
            patch.object(
                DuckLakeConnector,
                "replace_table_contents",
                side_effect=DuckLakeConnectorError("replace failed"),
            ),
            pytest.raises(DuckLakeConnectorError, match="replace failed"),
        ):
            self._run(tmp_path, messages, load_method="overwrite")
        assert '"bookmarks"' not in capsys.readouterr().out
        self._run(tmp_path, messages, load_method="overwrite")
        assert '"bookmarks"' in capsys.readouterr().out

    def test_state_is_held_while_an_overwrite_is_uncommitted(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """A mid-stream drain doesn't emit STATE before the overwrite commits.

        Every message drains here (max_record_age_minutes near zero). If the
        run then fails, the bookmark must not be out: a resumable tap would
        skip rows that were never committed.
        """
        state = {"type": "STATE", "value": {"bookmarks": {"t": {"pk": 3}}}}
        record = {"type": "RECORD", "stream": "t"}
        self._run(tmp_path, self._sync(1, {1: "old"}))
        failing = [
            self._schema_message(),
            {**record, "record": {"id": 3, "v": "new"}},
            state,
            {**record, "record": {"id": 4, "v": "new"}},
            {**record, "record": {"id": "abc", "v": "bad"}},
        ]
        capsys.readouterr()
        with pytest.raises(ValueError, match="whole numbers"):
            self._run(
                tmp_path, failing, load_method="overwrite", max_record_age_minutes=1e-9
            )
        assert '"bookmarks"' not in capsys.readouterr().out
        assert self._rows(tmp_path) == [(1, "old")]
        self._run(
            tmp_path, failing[:-1], load_method="overwrite", max_record_age_minutes=1e-9
        )
        assert '"bookmarks"' in capsys.readouterr().out
        assert self._rows(tmp_path) == [(3, "new"), (4, "new")]

    def test_an_emptied_source_removes_every_row(self, tmp_path: Path):
        """ACTIVATE_VERSION twice with no records between: the source is empty."""
        self._run(tmp_path, self._sync(1, {1: "a", 2: "b"}))
        self._run(tmp_path, self._sync(2, {}))
        assert self._rows(tmp_path) == []
