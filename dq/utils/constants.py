# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Constants for the Data Quality Framework.

The PyDeequ datatype map is lazily loaded to avoid forcing a pydeequ
dependency at core import time.
"""

DQ_ENGINE_NAME = "engine"
DQ_RULE_NAME = "name"
SCHEMA_VALIDATION_FK_CONSTRAINTS = "foreign_key_constraints"
SCHEMA_VALIDATION_UNQK_CONSTRAINTS = "unique_constraints"
SCHEMA_VALIDATION_REF_DB = "ref_db"
SCHEMA_VALIDATION_REF_TABLE = "ref_table"
SCHEMA_VALIDATION_REF_COLUMN = "ref_column"
SCHEMA_VALIDATION_SCHEMA = "schema"
SCHEMA_VALIDATION_DATABASE_KEY = "database"
SCHEMA_VALIDATION_TABLE_KEY = "table"
SCHEMA_VALIDATION_SRC_COLUMN = "src_column"
DQ_REPOSITORY_METRICS = "metrics"
DQ_REPOSITORY_VERIFICATIONS = "verifications"
SCHEMA_VALIDATION_CATALOG_TYPE_KEY = "catalog_type"
SCHEMA_VALIDATION_CATALOG_TYPE_UNITY = "unity"
SCHEMA_VALIDATION_CATALOG_TYPE_DELTA = "delta"
SCHEMA_VALIDATION_CATALOG_TYPE_GLUE = "glue"
SCHEMA_VALIDATION_CATALOG_TYPE_HIVE = "hive"
SCHEMA_VALIDATION_CATALOG_TYPE_SPARK = "spark"
DQ_SINGLE_CHECK_MODE = "single_check_mode"
SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_KEY = "threshold_list_count"
SCHEMA_VALIDATION_CHECK_FK_THRESHOLD_COUNT_VALUE = 1024
SCHEMA_VALIDATION_CHECK_FK_USE_LIST_KEY = "use_list_check"
DQ_METRICS_RESULT_SUCCESS_KEY = "success"
SCHEMA_VALIDATION_OVERRIDE_CONFIG_PATTERN_KEY = "pattern"
SCHEMA_VALIDATION_OVERRIDE_KEY = "overrides"
SCHEMA_VALIDATION_OVERRIDE_CONFIG_REPLACE_KEY = "replace"
SCHEMA_VALIDATION_NOT_NULL_COLUMNS_KEY = "not_null_columns"


def _get_pydeequ_datatype_map():
    """Lazily build the PyDeequ datatype map to avoid import-time dependency."""
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


# Lazy-loaded cache
_PYDEEQU_DATATYPE_MAP_CACHE = None


def get_pydeequ_datatype_map():
    """Get the PyDeequ datatype map (cached after first call)."""
    global _PYDEEQU_DATATYPE_MAP_CACHE
    if _PYDEEQU_DATATYPE_MAP_CACHE is None:
        _PYDEEQU_DATATYPE_MAP_CACHE = _get_pydeequ_datatype_map()
    return _PYDEEQU_DATATYPE_MAP_CACHE


# Fixed typo: SCHEVA -> SCHEMA (backward compat aliases below)
SCHEMA_VALIDATION_PYDEEQU_DATATYPE_MAP = property(
    lambda self: get_pydeequ_datatype_map()
)

SCHEMA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP = {
    "DateType": "DATE",
    "TimestampType": "TIMESTAMP",
    "TimestampNTZType": "TIMESTAMPNTZ",
    "BinaryType": "BINARY",
    "ShortType": "SHORT",
    "ByteType": "BYTE",
}

SCHEMA_VALIDATION_TYPEOF_DATATYPE_MAP = {
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

# Backward compatibility aliases (typo versions)
SCHEVA_VALIDATION_PYDEEQU_DATATYPE_MAP = None  # Replaced by get_pydeequ_datatype_map()
SCHEVA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP = (
    SCHEMA_VALIDATION_CASTSPARKSQL_DATATYPE_MAP
)
