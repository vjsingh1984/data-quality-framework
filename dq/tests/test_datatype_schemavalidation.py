# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json

import pytest
from pyhocon import ConfigFactory
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dq.engine.schemavalidation.schemavalidation_engine import SchemaValidationEngine


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_stringtype_with_yyyymmdd_without_override_failure(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD without override"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert (
        False == overallsuccess
    ), "Schemavalidation should fail as signupo_date will be interpreted as IntType and StringType check constraint will fail"


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_stringtype_with_yyyymmdd_with_override_pattern_replace_true_success(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD and Override with replace true(default)"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            overrides = [
                { 
                    column = signup_date
                    pattern = "^20[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|(1|2)[0-9]|3[01])$"
                }
            ]
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Schemavalidation should have passed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_stringtype_with_yyyymmdd_with_override_pattern_replace_false_failure(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD and Override with replace true(default)"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = temp_data_table
            overrides = [
                { column = signup_date
                  pattern = "^20[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|(1|2)[0-9]|3[01])$"
                  replace = False
                }
            ]
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert False == overallsuccess, "Schemavalidation should have failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_varchartype10_success(spark):
    data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as VARCHAR(10))"))
    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for varchartype10_success"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_shortType_success(spark):
    data = [
        ("John's name is greater than allowed 10 characters", 25),
        ("Doe", 30),
        ("Jane", 40),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df.createOrReplaceTempView("temp_data_table")
    df.withColumn("age", expr("CAST(age as SHORT)"))
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_chartype10_success(spark):
    data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as CHAR(10))"))

    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for chartype10_success"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_true_booleanType_success(spark):
    data = [
        ("John's name is greater than allowed 10 characters", False),
        ("Doe", False),
        ("Jane", True),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("isteenager", BooleanType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as CHAR(10))"))

    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for chartype10_failure"
        engine = schemavalidation
        single_check_mode = True
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_stringtype_with_yyyymmdd_without_override_failure(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD without override"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = temp_data_table
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert (
        False == overallsuccess
    ), "Schemavalidation should fail as signupo_date will be interpreted as IntType and StringType check constraint will fail"


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_stringtype_with_yyyymmdd_with_override_pattern_replace_true_success(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD and Override with replace true(default)"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = temp_data_table
            overrides = [
                { column = signup_date
                  pattern = "^20[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|(1|2)[0-9]|3[01])$"
                }
            ]
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Schemavalidation should have passed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_stringtype_with_yyyymmdd_with_override_pattern_replace_false_failure(
    spark,
):
    data = [
        ("John", "20230101"),
        ("Joe", "20230303"),
        ("Jane", "20230401"),
        ("James", "20230903"),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("signup_date", StringType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("temp_data_table")
    schema_config = ConfigFactory.parse_string(
        """
    {
        name="SchemaValidation for StringType with YYYYMMDD and Override with replace true(default)"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = temp_data_table
            overrides = [
                { column = signup_date
                  pattern = "^20[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|(1|2)[0-9]|3[01])$"
                  replace = False
                }
            ]
        }
    }
    """
    )
    # Apply Schema Validation including multi-column unique and foreign key constraints
    schema_validation_engine = SchemaValidationEngine(schema_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert False == overallsuccess, "Schemavalidation should have failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_varchartype10_success(spark):
    data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as VARCHAR(10))"))
    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for varchartype10_success"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_shortType_success(spark):
    data = [
        ("John's name is greater than allowed 10 characters", 25),
        ("Doe", 30),
        ("Jane", 40),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df.createOrReplaceTempView("temp_data_table")
    df.withColumn("age", expr("CAST(age as SHORT)"))
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for varchartype10_failure"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_chartype10_success(spark):
    data = [("John", 25), ("Doe", 30), ("Jane", 40)]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as CHAR(10))"))

    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for chartype10_success"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."


@pytest.mark.spark
def test_schemavalidation_single_check_mode_false_booleanType_success(spark):
    data = [
        ("John's name is greater than allowed 10 characters", False),
        ("Doe", False),
        ("Jane", True),
    ]
    data_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("isteenager", BooleanType(), False),
        ]
    )
    df = spark.createDataFrame(data=data, schema=data_schema)
    df = df.withColumn("name", expr("CAST( name as CHAR(10))"))

    df.createOrReplaceTempView("temp_data_table")
    scemavalidation_config = ConfigFactory.parse_string(
        """
    {
        name = "schemavalidation for chartype10_failure"
        engine = schemavalidation
        single_check_mode = False
        schema = {
            catalog_type = spark
            table = "temp_data_table"
        }
    }
    """
    )
    schema_validation_engine = SchemaValidationEngine(scemavalidation_config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    overallsuccess = True
    for metric in summary_metrics:
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overallsuccess = False

    assert True == overallsuccess, "Atleast one metric failed."
