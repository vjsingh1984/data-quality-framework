# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for UNIQUE constraint validation in schema validation."""

import json

import pytest
from pyhocon import ConfigFactory
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dq.tests.test_helpers import (
    apply_schema_validation,
    assert_all_metrics_success,
    assert_any_metric_failure,
)


@pytest.mark.spark
def test_unique_constraint_single_key_single_check_mode_success(spark):
    """Test unique constraint on single column succeeds with unique values."""
    schema_config = ConfigFactory.parse_string(
        """{
        name = "unique constraint single key success"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[name]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    metrics = apply_schema_validation(spark, df, schema_config)
    assert_all_metrics_success(metrics)


@pytest.mark.spark
def test_unique_constraint_single_key_single_check_mode_failure(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[age]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert False == process_schemavalidation_failure(
        spark, df, schema_config
    ), "Atleast one metric should have failed."


@pytest.mark.spark
def test_unique_constraint_composite_key_single_check_mode_success(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[name,age]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert True == process_schemavalidation_success(
        spark, df, schema_config
    ), "Atleast one metric failed."


@pytest.mark.spark
def test_unique_constraint_composite_key_single_check_mode_failure(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[age,signup_date]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert False == process_schemavalidation_failure(
        spark, df, schema_config
    ), "Atleast one metric should have failed."


@pytest.mark.spark
def test_unique_constraint_single_key_multiple_check_mode_success(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[name]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert True == process_schemavalidation_success(
        spark, df, schema_config
    ), "Atleast one metric failed."


@pytest.mark.spark
def test_unique_constraint_single_key_multiple_check_mode_failure(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[age]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert False == process_schemavalidation_failure(
        spark, df, schema_config
    ), "Atleast one metric failed."


@pytest.mark.spark
def test_unique_constraint_composite_key_multiple_check_mode_success(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[name,age]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert True == process_schemavalidation_success(
        spark, df, schema_config
    ), "Atleast one metric failed."


@pytest.mark.spark
def test_unique_constraint_composite_key_multiple_check_mode_failure(spark):
    schema_config = ConfigFactory.parse_string(
        """{
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            unique_constraints=[[age,signup_date]]
        }
    }"""
    )
    data = [
        ("John", 25, "2021-01-01"),
        ("Doe", 30, "2021-01-02"),
        ("Jane", 30, "2021-01-02"),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("signup_date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    assert False == process_schemavalidation_failure(
        spark, df, schema_config
    ), "Atleast one metric should have failed."
