# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Constants for the Data Quality Framework.

The PyDeequ datatype map is lazily loaded to avoid forcing a pydeequ
dependency at core import time.
"""
import functools

DQ_ENGINE_NAME = "engine"
DQ_RULE_NAME = "name"
DQ_DATASET = "dataset"
# Foreign key and unique constraint config keys
FK_CONSTRAINTS = "foreign_key_constraints"
UNIQUE_CONSTRAINTS = "unique_constraints"
REF_DB = "ref_db"
REF_TABLE = "ref_table"
REF_COLUMN = "ref_column"
SCHEMA = "schema"
DATABASE_KEY = "database"
TABLE_KEY = "table"
SRC_COLUMN = "src_column"
DQ_REPOSITORY_METRICS = "metrics"
DQ_REPOSITORY_VERIFICATIONS = "verifications"
CATALOG_TYPE_KEY = "catalog_type"
CATALOG_TYPE_UNITY = "unity"
CATALOG_TYPE_DELTA = "delta"
CATALOG_TYPE_GLUE = "glue"
CATALOG_TYPE_HIVE = "hive"
CATALOG_TYPE_SPARK = "spark"
DQ_SINGLE_CHECK_MODE = "single_check_mode"
FK_THRESHOLD_COUNT_KEY = "threshold_list_count"
FK_THRESHOLD_COUNT_VALUE = 1024
FK_USE_LIST_KEY = "use_list_check"
# Threshold for switching from list-based (isContainedIn) to join-based FK validation.
# List-based validation builds an IN clause with all reference values, which is
# efficient for small reference tables but becomes memory-intensive for large ones.
# Above this threshold, the framework uses a left outer join instead.
DQ_METRICS_RESULT_SUCCESS_KEY = "success"
OVERRIDE_CONFIG_PATTERN_KEY = "pattern"
OVERRIDE_KEY = "overrides"
OVERRIDE_CONFIG_REPLACE_KEY = "replace"
NOT_NULL_COLUMNS_KEY = "not_null_columns"


@functools.lru_cache(maxsize=1)
def get_pydeequ_datatype_map():
    """Lazily build and cache the PyDeequ datatype map.

    Uses ``lru_cache`` so the map is built once on first access and
    the pydeequ import is deferred until actually needed.
    """
    from pydeequ.checks import ConstrainableDataTypes

    return {
        "IntegerType": ConstrainableDataTypes.Integral,
        "LongType": ConstrainableDataTypes.Integral,
        "StringType": ConstrainableDataTypes.String,
        "CharType": ConstrainableDataTypes.String,
        "VarcharType": ConstrainableDataTypes.String,
        "DoubleType": ConstrainableDataTypes.Fractional,
        "FloatType": ConstrainableDataTypes.Fractional,
        "BooleanType": ConstrainableDataTypes.Boolean,
        "DecimalType": ConstrainableDataTypes.Numeric,
    }


# Datatype mapping constants for schema validation
PYDEEQU_DATATYPE_MAP = property(
    lambda self: get_pydeequ_datatype_map()
)

CAST_SPARK_SQL_DATATYPE_MAP = {
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "TimestampNTZType": "TIMESTAMPNTZ",
    "BinaryType": "BINARY",
    "ShortType": "SHORT",
    "ByteType": "BYTE",
}

TYPEOF_DATATYPE_MAP = {
    "IntegerType": "int",
    "LongType": "bigint",
    "StringType": "string",
    "CharType": "string",
    "VarcharType": "string",
    "DoubleType": "double",
    "FloatType": "float",
    "BooleanType": "boolean",
    "DecimalType": "decimal",
    "DateType": "date",
    "TimestampType": "timestamp",
}
