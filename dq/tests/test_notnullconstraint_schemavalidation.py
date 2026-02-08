# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for NOT NULL constraint validation in schema validation."""

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from dq.tests.test_helpers import (
    apply_schema_validation,
    assert_all_metrics_success,
    create_schema_config,
)


@pytest.mark.spark
def test_schemavalidation_with_nullvalue_and_nullable_columns_success(spark):
    """Test schema validation succeeds when nullable columns contain null values."""
    data = [("John", 25, "2021-01-01"), ("Doe", None, "2021-01-02"), ("Jane", 40, None)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)

    config_str = create_schema_config(
        name="null values in nullable columns",
        single_check_mode=True,
    )
    metrics = apply_schema_validation(spark, df, config_str)

    assert_all_metrics_success(metrics)


@pytest.mark.spark
def test_schemavalidation_without_nullvalue_and_non_nullable_columns_success(spark):
    """Test schema validation succeeds when non-nullable columns contain no null values."""
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 40, "2024-05-05"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)

    config_str = create_schema_config(
        name="no null values in non-nullable columns",
        single_check_mode=True,
    )
    metrics = apply_schema_validation(spark, df, config_str)

    assert_all_metrics_success(metrics)
