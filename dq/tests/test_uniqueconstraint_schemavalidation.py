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

from dq.engine.schemavalidation.schemavalidation_engine import (
    SchemaValidationEngine,
)
from dq.tests.test_helpers import (
    apply_schema_validation,
    assert_all_metrics_success,
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
    ), "At least one metric should have failed."


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
    ), "At least one metric failed."


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
    ), "At least one metric should have failed."


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
    ), "At least one metric failed."


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
    ), "At least one metric failed."


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
    ), "At least one metric failed."


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
    ), "At least one metric should have failed."


def process_schemavalidation_success(spark, df, config):
    # Initialize Schema Validation Engine with single_check_mode = False for granular reporting
    schema_validation_engine = SchemaValidationEngine(config)
    df.createOrReplaceTempView("temp_data_table")
    # Apply Schema Validation including multi-column unique constraints
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overall_success = True
    for metric in summary_metrics:
        print(json.dumps(metric, indent=2))
        # assert metric['success'] == True , f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overall_success = False
    if not overall_success:
        print("dataframe\n")
        df.show()
        print("schema\n")
        df.printSchema()
        print("summarymetric\n")
        print(json.dumps(summary_metrics, indent=2))
    return overall_success


def process_schemavalidation_failure(spark, df, config):
    # Initialize Schema Validation Engine with single_check_mode = False for granular reporting
    schema_validation_engine = SchemaValidationEngine(config)
    df.createOrReplaceTempView("temp_data_table")
    # Apply Schema Validation including multi-column unique constraints
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overall_success = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overall_success = False

    return overall_success
