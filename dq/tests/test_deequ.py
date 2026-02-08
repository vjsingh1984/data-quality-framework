# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json

import pytest
from pyhocon import ConfigFactory

from dq.engine.deequ.deequ_engine import DeequEngine


@pytest.fixture
def sample_success_dataframe(spark):
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", 4, "cathy@example.com"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_failure_dataframe(spark):
    data = [
        ("Alice", -2, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", None, "not-a-valid-email"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def deequ_single_check_config():
    config_str = """
    deequ { 
        name = "myrule1"
        engine = "deequ"
        single_check_mode = True
        checks = [
            {
                constraint = "containsEmail"
                column = "email"
                assertion = "lambda i : i == 1"
            },
            {
                constraint = "isPositive"
                column = "age"
            },
            {
                alias = "check-datatype"
                constraint = "hasDataType"
                column = "age"
                description = "Check if age is integer type"
                hint = "Check failed as age may not be integer type"
                datatype = Integral
                assertion = "lambda i : i == 1"
            },
	        {
		        constraint = "hasCompleteness",
                column = "name",
                assertion = "lambda i : i == 1"
                hint = "Check failed as name may not be complete"
                description = "Check if name is complete"
                
            }
        ]
    }

    """
    return ConfigFactory.parse_string(config_str).get("deequ", {})


@pytest.fixture
def deequ_multi_check_config():
    config_str = """
    deequ { 
        name = "myrule1"
        engine = "deequ"
        single_check_mode = False
        checks = [
            {
                constraint = "containsEmail"
                column = "email"
                assertion = "lambda i : i == 1"
            },
            {
                constraint = "isPositive"
                column = "age"
            },
            {
                alias = "check-datatype"
                constraint = "hasDataType"
                column = "age"
                description = "Check if age is integer type"
                hint = "Check failed as age may not be integer type"
                datatype = Integral
                assertion = "lambda i : i == 1"
            },
	        {
		        constraint = "hasCompleteness",
                column = "name",
                assertion = "lambda i : i == 1"
                hint = "Check failed as name may not be complete"
                description = "Check if name is complete"
                
            }
        ]
    }

    """
    return ConfigFactory.parse_string(config_str).get("deequ", {})


@pytest.mark.spark
def test_single_check_deequ_engine_success(
    sample_success_dataframe, deequ_single_check_config
):
    engine = DeequEngine(deequ_single_check_config)
    summary_metrics = engine.apply(sample_success_dataframe, repository=None)
    overall_success = True
    for metric in summary_metrics:
        assert metric["success"] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overall_success = False

    assert True == overall_success, "At least one metric failed."


@pytest.mark.spark
def test_multi_check_deequ_engine_success(
    sample_success_dataframe, deequ_multi_check_config
):
    engine = DeequEngine(deequ_multi_check_config)
    summary_metrics = engine.apply(sample_success_dataframe, repository=None)
    overall_success = True
    for metric in summary_metrics:
        assert metric["success"] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overall_success = False

    assert True == overall_success, "At least one metric failed."


@pytest.mark.spark
def test_single_check_deequ_engine_failure(
    sample_failure_dataframe, deequ_single_check_config
):
    engine = DeequEngine(deequ_single_check_config)
    summary_metrics = engine.apply(sample_failure_dataframe, repository=None)
    overall_success = True
    for metric in summary_metrics:
        if not (metric["success"]):
            overall_success = False

    assert False == overall_success, "At least one metric should have failed."


@pytest.mark.spark
def test_multi_check_deequ_engine_failure(
    sample_failure_dataframe, deequ_multi_check_config
):
    engine = DeequEngine(deequ_multi_check_config)
    summary_metrics = engine.apply(sample_failure_dataframe, repository=None)
    overall_success = True
    for metric in summary_metrics:
        if not (metric["success"]):
            overall_success = False

    assert False == overall_success, "At least one metric should have failed."
