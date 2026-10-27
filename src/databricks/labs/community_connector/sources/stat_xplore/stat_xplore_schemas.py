"""Spark schemas and static metadata for the Stat-Xplore connector.

Schemas are static (a pure function of the table kind and ``output``) so the
Delta target never drifts with the data. Dataset-specific shape lives inside
the ``dimensions`` array and the raw JSON columns, not in the column list.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STAT_XPLORE_TABLE_SCHEMA_ITEMS = "schema_items"
STAT_XPLORE_TABLE_INFO = "info"
STAT_XPLORE_TABLE_RATE_LIMIT = "rate_limit"
STAT_XPLORE_BUILTIN_TABLES = (
    STAT_XPLORE_TABLE_SCHEMA_ITEMS,
    STAT_XPLORE_TABLE_INFO,
    STAT_XPLORE_TABLE_RATE_LIMIT,
)

STAT_XPLORE_SCHEMA_ITEMS_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("parent_id", StringType(), False),
    StructField("label", StringType(), True),
    StructField("type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("depth", IntegerType(), False),
    StructField("is_expanded", BooleanType(), False),
    StructField("child_count", IntegerType(), True),
    StructField("schema_root", StringType(), False),
    StructField("raw_json", StringType(), False),
    StructField("retrieved_at", TimestampType(), False),
])

STAT_XPLORE_INFO_SCHEMA = StructType([
    StructField("locale", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("raw_json", StringType(), False),
    StructField("retrieved_at", TimestampType(), False),
])

STAT_XPLORE_RATE_LIMIT_SCHEMA = StructType([
    StructField("scope", StringType(), False),
    StructField("limit", LongType(), True),
    StructField("remaining", LongType(), True),
    StructField("reset_epoch_ms", LongType(), True),
    StructField("raw_json", StringType(), False),
    StructField("retrieved_at", TimestampType(), False),
])

_SX_DIMENSION_STRUCT = StructType([
    StructField("field_uri", StringType(), False),
    StructField("field_label", StringType(), True),
    StructField("item_index", IntegerType(), False),
    StructField("item_type", StringType(), True),
    StructField("item_uris", ArrayType(StringType()), False),
    StructField("item_labels", ArrayType(StringType()), False),
    StructField("annotation_keys", ArrayType(StringType()), False),
])

STAT_XPLORE_CELLS_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("query_hash", StringType(), False),
    StructField("database_id", StringType(), False),
    StructField("measure_uri", StringType(), False),
    StructField("measure_label", StringType(), True),
    StructField("dimensions", ArrayType(_SX_DIMENSION_STRUCT), False),
    StructField("dimension_key", StringType(), False),
    StructField("period_field_uri", StringType(), True),
    StructField("period_uri", StringType(), True),
    StructField("period_code", StringType(), True),
    StructField("period_label", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("value_raw", StringType(), False),
    StructField("value_status", StringType(), False),
    StructField("annotation_keys", ArrayType(StringType()), False),
    StructField("annotations_json", StringType(), False),
    StructField("cube_metadata_json", StringType(), False),
    StructField("request_json", StringType(), False),
    StructField("response_sha256", StringType(), False),
    StructField("retrieved_at", TimestampType(), False),
])

STAT_XPLORE_RESPONSES_SCHEMA = StructType([
    StructField("response_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("query_hash", StringType(), False),
    StructField("database_id", StringType(), False),
    StructField("measure_uris", ArrayType(StringType()), False),
    StructField("period_field_uri", StringType(), True),
    StructField("period_uris", ArrayType(StringType()), False),
    StructField("request_json", StringType(), False),
    StructField("response_json", StringType(), False),
    StructField("response_sha256", StringType(), False),
    StructField("cell_count", LongType(), False),
    StructField("numeric_cell_count", LongType(), False),
    StructField("missing_cell_count", LongType(), False),
    StructField("non_numeric_cell_count", LongType(), False),
    StructField("retrieved_at", TimestampType(), False),
])

STAT_XPLORE_BUILTIN_SCHEMAS = {
    STAT_XPLORE_TABLE_SCHEMA_ITEMS: STAT_XPLORE_SCHEMA_ITEMS_SCHEMA,
    STAT_XPLORE_TABLE_INFO: STAT_XPLORE_INFO_SCHEMA,
    STAT_XPLORE_TABLE_RATE_LIMIT: STAT_XPLORE_RATE_LIMIT_SCHEMA,
}

STAT_XPLORE_BUILTIN_METADATA = {
    STAT_XPLORE_TABLE_SCHEMA_ITEMS: {"primary_keys": ["id", "parent_id"], "ingestion_type": "snapshot"},
    STAT_XPLORE_TABLE_INFO: {"primary_keys": ["locale"], "ingestion_type": "snapshot"},
    STAT_XPLORE_TABLE_RATE_LIMIT: {"primary_keys": ["scope"], "ingestion_type": "snapshot"},
}

STAT_XPLORE_SCHEMA_OPTION_KEYS = (
    "schema_root",
    "schema_expand_types",
    "schema_max_depth",
    "schema_max_requests",
)
