"""Comprehensive unit tests for target-ducklake components."""

from __future__ import annotations

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
from singer_sdk.exceptions import ConfigValidationError

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
    _field_type_to_pyarrow_field,
    create_pyarrow_table,
    flatten_schema_to_pyarrow_schema,
    number_precision_and_scale,
)
from target_ducklake.sinks import ducklakeSink
from target_ducklake.target import Targetducklake


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


class TestParquetUtils:
    """Test parquet utilities functionality."""

    def test_field_type_to_pyarrow_field(self):
        """Test PyArrow field conversion."""
        # Test string field
        field = _field_type_to_pyarrow_field(
            "name", {"type": "string"}, required_fields=["name"]
        )
        assert field.name == "name"
        assert field.type == pa.string()
        assert not field.nullable

        # Test nullable field
        field = _field_type_to_pyarrow_field(
            "optional_field", {"type": "string"}, required_fields=["other_field"]
        )
        assert field.nullable

        # Test integer field
        field = _field_type_to_pyarrow_field(
            "id", {"type": "integer"}, required_fields=["id"]
        )
        assert field.type == pa.int64()

    def test_flatten_schema_to_pyarrow_schema(self):
        """Test converting flattened schema to PyArrow schema."""
        flattened_schema = {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
        }
        schema = flatten_schema_to_pyarrow_schema(flattened_schema)
        assert isinstance(schema, pa.Schema)
        assert len(schema) == 3
        assert schema.field("id").type == pa.int64()
        assert schema.field("name").type == pa.string()
        assert schema.field("email").type == pa.string()


class TestDecimalPrecision:
    """Numbers are staged as decimals so they keep their exact digits."""

    # 29 integer + 9 decimal digits: fills DECIMAL(38, 9), far beyond float64.
    BIG = Decimal("12345678901234567890123456789.123456789")

    @pytest.mark.parametrize(
        ("jsonschema", "expected"),
        [
            ({"type": ["null", "number"]}, (38, 9)),
            ({"type": "number", "precision": 10, "scale": 2}, (10, 2)),
            ({"type": "number", "precision": 50}, (38, 9)),
            ({"type": "number", "precision": 5}, (5, 5)),
        ],
    )
    def test_number_precision_and_scale(self, jsonschema, expected):
        """Precision caps at 38; scale defaults to 9, never above precision."""
        assert number_precision_and_scale(jsonschema) == expected

    def test_number_field_is_decimal(self):
        """Number columns are staged as decimal128, not float64."""
        field = _field_type_to_pyarrow_field(
            "amount", {"type": ["null", "number"]}, required_fields=[]
        )
        assert field.type == pa.decimal128(38, 9)

    def test_parquet_type_matches_ducklake_column_type(self):
        """The staged parquet type and the DuckLake DDL type always agree."""
        jsonschema = {"type": "number", "precision": 12, "scale": 4}
        field = _field_type_to_pyarrow_field("amount", jsonschema, required_fields=[])
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
        schema = flatten_schema_to_pyarrow_schema({"amount": {"type": "number"}})
        table = create_pyarrow_table([{"amount": self.BIG}], schema)
        path = tmp_path / "batch.parquet"
        pq.write_table(table, path)

        con = duckdb.connect()
        con.execute("CREATE TABLE t (amount DECIMAL(38, 9))")
        con.execute(f"INSERT INTO t SELECT amount FROM '{path}'")
        assert con.execute("SELECT amount FROM t").fetchone()[0] == self.BIG


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
        pyarrow_schema = flatten_schema_to_pyarrow_schema(flat_schema)

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
            ("number", "decimal(50,40)", "DECIMAL(38, 38)"),
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
        schema = flatten_schema_to_pyarrow_schema(
            {
                "d": {"type": "number", "x-sql-datatype": "numeric(12,4)"},
                "f": {"type": "number", "x-sql-datatype": "double precision"},
                "n": {"type": "number"},
            }
        )
        assert schema.field("d").type == pa.decimal128(12, 4)
        assert schema.field("f").type == pa.float64()
        assert schema.field("n").type == pa.decimal128(38, 9)

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
        table = create_pyarrow_table([record], flatten_schema_to_pyarrow_schema(props))
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
        pyarrow_schema = flatten_schema_to_pyarrow_schema(props)
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
            patch.object(connector, "get_table_columns", return_value=["legacy_id"]),
        ):
            connector.prepare_table(
                "myschema", "mytable", [{"name": "legacy_id", "type": "BIGINT"}]
            )

        mock_alter.assert_called_once_with("myschema", "mytable", "legacy_id", "BIGINT")
        mock_add.assert_not_called()
