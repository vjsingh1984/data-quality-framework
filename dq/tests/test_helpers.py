# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Shared test helper functions for reducing duplication across test files."""

import json
from typing import Any, Dict, List

from pyhocon import ConfigTree
from pyspark.sql import DataFrame


def apply_schema_validation(
    spark, df: DataFrame, config: ConfigTree
) -> List[Dict[str, Any]]:
    """Apply schema validation and return metrics.

    Args:
        spark: SparkSession instance.
        df: DataFrame to validate.
        config: Configuration tree.

    Returns:
        List of metric dictionaries.
    """
    from dq.engine.schemavalidation.schemavalidation_engine import (
        SchemaValidationEngine,
    )

    # Create temp view with consistent name
    df.createOrReplaceTempView("temp_data_table")

    # Initialize and run schema validation engine
    schema_validation_engine = SchemaValidationEngine(config)
    summary_metrics = schema_validation_engine.apply(df, repository=None)
    return summary_metrics


def assert_all_metrics_success(metrics: List[Dict[str, Any]]) -> None:
    """Assert that all metrics in the list indicate success.

    Args:
        metrics: List of metric dictionaries.

    Raises:
        AssertionError: If any metric failed.
    """
    overall_success = True
    for metric in metrics:
        if not metric["success"]:
            print("Error in : " + json.dumps(metric, default=str))
            overall_success = False

    assert (
        overall_success
    ), f"Some metrics failed: {[m for m in metrics if not m['success']]}"


def assert_any_metric_failure(metrics: List[Dict[str, Any]]) -> None:
    """Assert that at least one metric in the list indicates failure.

    Args:
        metrics: List of metric dictionaries.

    Raises:
        AssertionError: If all metrics succeeded.
    """
    failed_metrics = [m for m in metrics if not m["success"]]
    assert (
        len(failed_metrics) > 0
    ), "Expected at least one metric to fail, but all succeeded"


def parse_schema_config(config_str: str) -> ConfigTree:
    """Parse a schema validation configuration string.

    Args:
        config_str: HOCON configuration string.

    Returns:
        Parsed configuration tree.
    """
    from pyhocon import ConfigFactory

    return ConfigFactory.parse_string(config_str)


def create_schema_config(
    name: str,
    table: str = "temp_data_table",
    catalog_type: str = "spark",
    single_check_mode: bool = True,
    additional_config: str = "",
) -> str:
    """Build a schema validation configuration string.

    Args:
        name: Test name for the validation rule.
        table: Table name to validate.
        catalog_type: Catalog type (spark, unity, glue, hive).
        single_check_mode: Whether to use single check mode.
        additional_config: Additional HOCON config to append.

    Returns:
        HOCON configuration string.
    """
    base_config = f"""{{
        name = "{name}"
        engine = schemavalidation
        single_check_mode = {str(single_check_mode).lower()}
        schema = {{
            catalog_type = {catalog_type}
            table = {table}
        }}
    """

    if additional_config:
        base_config = base_config.rstrip("}") + ",\n" + additional_config + "\n}"

    return base_config
