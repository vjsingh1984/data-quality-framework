# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import pathlib

import pytest

# Import custom constraints to auto-register them
import dq.engine.custom.constraints  # noqa: F401

os.environ.setdefault("SPARK_VERSION", "3.5")

try:
    import pydeequ
    from pyhocon import ConfigFactory
    from pyspark.sql import SparkSession

    _HAS_SPARK = True
except ImportError:
    _HAS_SPARK = False
    # Skip test files that import pyspark directly (they all live in this dir)
    _this_dir = pathlib.Path(__file__).parent
    collect_ignore = [str(f) for f in _this_dir.glob("test_*.py")]


@pytest.fixture
def spark(scope="module"):
    if not _HAS_SPARK:
        pytest.skip("pyspark/pydeequ not installed")
    path_list = "lib/deequ-2.0.7-spark-3.5.jar".split("/")
    spark = (
        SparkSession.builder.master("local")
        .appName("test-dqframework")
        .config("spark.jars", os.path.join(*path_list))
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .getOrCreate()
    )
    print(pydeequ.deequ_maven_coord, pydeequ.f2j_maven_coord)
    yield spark


@pytest.fixture
def sample_dataframe(spark):
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", None, "cathy@example.com"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def multi_column_dataframe(spark):
    data = [
        (
            "2024-10-23",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "B",
            "Tier_1",
            0.25,
            0.40319,
            0.50663,
            0.20348,
            0.49419,
            0.21469,
            0.77583,
            0.13068,
            0.51413,
            0.73420,
            0.53060,
        ),
        (
            "2024-10-24",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "A",
            "Tier_1",
            0.25,
            0.06611,
            0.47947,
            0.51981,
            0.49819,
            0.45441,
            0.59403,
            0.73713,
            0.30807,
            0.06705,
            0.70729,
        ),
        (
            "2024-10-25",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "BB",
            "Tier_1",
            0.4,
            0.55653,
            0.22908,
            0.72953,
            0.44179,
            0.36171,
            0.52886,
            0.40993,
            0.58167,
            0.70007,
            0.15098,
        ),
    ]
    columns = [
        "Date",
        "Batch",
        "Region",
        "Sectors",
        "AvRating",
        "Tier",
        "Recovery",
        "Metric6M",
        "Metric1Y",
        "Metric2Y",
        "Metric3Y",
        "Metric5Y",
        "Metric7Y",
        "Metric10Y",
        "Metric15Y",
        "Metric20Y",
        "Metric30Y",
    ]

    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_dataframe_group(spark):
    data = [
        ("Alice", 34, "NY", "US"),
        ("Bob", 45, "NY", "US"),
        ("Catherine", 45, "NY", "US"),
    ]
    columns = ["name", "age", "state", "country"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def custom_config():
    config_str = """
    sync { 
        name = "myrule1"
        engine = "custom"
        checks = [
        {
            constraint_name = "DistinctnessByGroup-check"
            constraint = "DistinctnessByGroup" 
            columns = ["name", "age"]
            group_by = ["state", "country"]
            min =  3
            level = "Error"
            }
        ]
    }
    """
    return ConfigFactory.parse_string(config_str).get("sync", {})


@pytest.fixture
def custom_config_rate_of_change():
    config_str = """
    sync { 
        name = "myrule1"
        engine = "custom"
        checks = [
        {
            constraint_name = "rate_of_change_check"
            constraint = "RateOfChange"
            columns = ["Recovery", "Metric6M", "Metric1Y", "Metric2Y", "Metric3Y", "Metric5Y", "Metric7Y", "Metric10Y", "Metric15Y", "Metric20Y", "Metric30Y"]
            group_by = ["Batch", "Region", "Sectors", "AvRating", "Tier"]
            sort_by = "Date"
            max =  20
            level = "Error"
        },
        {
            constraint_name = "stale_value_check"
            constraint = "DistinctnessByGroup"
            columns = [ "Metric6M", "Metric1Y", "Metric2Y", "Metric3Y", "Metric5Y", "Metric7Y", "Metric10Y", "Metric15Y", "Metric20Y", "Metric30Y"]
            group_by = ["Batch", "Region", "Sectors", "AvRating", "Tier"]
            min =  2
            level = "Error"
        }
        ]
    }
    """
    return ConfigFactory.parse_string(config_str).get("sync", {})


@pytest.fixture
def custom_config_lookup_based_column():
    config_str = """
    sync { 
        name = "reflookup"
        engine = "custom"
        checks = [
        {
            constraint_name = "ref_table_lookup"
            constraint = "LookupColumnList"
            ignore_columns = ["Date", "u", "source"]
            ref_table = "ref_db.lookup_table"
            ref_columns ="item_id"
            level = "Warning"
        }
        ]
    }
    """
    return ConfigFactory.parse_string(config_str).get("sync", {})


@pytest.fixture
def custom_config_wide_col_negative_values():
    config_str = """
    sync { 
        name = "NonNegativeCheckforWidetables"
        engine = "custom"
        checks = [
        {
            constraint_name = "Negative_values"
            constraint = "NegativeValuesCheck" 
            ignore_columns = ["Date", "u", "source"]
            level = "Warning"
        }
        ]
    }
    """
    return ConfigFactory.parse_string(config_str).get("sync", {})


@pytest.fixture
def numeric_dataframe_with_negatives(spark):
    data = [
        (20240105, 58229.9219, 28396.1404, 60950.7453, 18751.7745, 92411.2502),
        (20240106, 12923.4537, -12054.2225, -91415.808, 72485.9162, 34810.6636),
    ]

    columns = ["date", "col_a", "col_b", "col_c", "col_d", "col_e"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def numeric_dataframe(spark):
    data = [
        (20240105, 58229.9219, 28396.1404, 60950.7453, 18751.7745, 92411.2502),
        (20240106, 12923.4537, 12054.2225, 91415.808, 72485.9162, 34810.6636),
    ]

    columns = ["date", "col_a", "col_b", "col_c", "col_d", "col_e"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def multi_column_dataframe_rate_of_change(spark):
    data = [
        (
            "2024-10-23",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "B",
            "Tier_1",
            0.25,
            0.40319,
            0.50663,
            0.20348,
            0.49419,
            0.21469,
            0.77583,
            0.13068,
            0.51413,
            0.73420,
            0.53060,
        ),
        (
            "2024-10-24",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "B",
            "Tier_1",
            0.35,
            0.06611,
            0.47947,
            0.51981,
            0.49819,
            0.45441,
            0.59403,
            0.73713,
            0.30807,
            0.06705,
            0.70729,
        ),
        (
            "2024-10-25",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "B",
            "Tier_1",
            0.90,
            0.55653,
            0.22908,
            0.72953,
            0.44179,
            0.36171,
            0.52886,
            0.40993,
            0.58167,
            0.70007,
            0.15098,
        ),
        (
            "2024-10-23",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "BB",
            "Tier_1",
            0.25,
            0.10319,
            0.50663,
            0.20348,
            0.49419,
            0.21469,
            0.77583,
            0.13068,
            0.51413,
            0.73420,
            0.53060,
        ),
        (
            "2024-10-24",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "BB",
            "Tier_1",
            0.25,
            0.001611,
            0.47947,
            0.51981,
            0.49819,
            0.45441,
            0.59403,
            0.73713,
            0.30807,
            0.06705,
            0.70729,
        ),
        (
            "2024-10-25",
            "Batch_1",
            "Region_A",
            "Sector_1",
            "BB",
            "Tier_1",
            0.4,
            0.99653,
            0.22908,
            0.72953,
            0.44179,
            0.36171,
            0.52886,
            0.40993,
            0.58167,
            0.70007,
            0.15098,
        ),
    ]
    columns = [
        "Date",
        "Batch",
        "Region",
        "Sectors",
        "AvRating",
        "Tier",
        "Recovery",
        "Metric6M",
        "Metric1Y",
        "Metric2Y",
        "Metric3Y",
        "Metric5Y",
        "Metric7Y",
        "Metric10Y",
        "Metric15Y",
        "Metric20Y",
        "Metric30Y",
    ]

    return spark.createDataFrame(data, columns)
