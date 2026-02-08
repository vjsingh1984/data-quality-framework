# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark integration tests for the Drules engine."""
import pytest
from pyhocon import ConfigFactory

from dq.engine.drules.drules_engine import DrulesEngine
from dq.exceptions import ConfigurationError


@pytest.fixture
def drules_dataframe(spark):
    """DataFrame with mixed data for drules tests."""
    data = [
        ("Alice", 34, "alice@example.com", 75000, "2020-01-15", "2024-06-30"),
        ("Bob", 45, "bob@example.com", 95000, "2018-03-01", "2025-12-31"),
        ("Catherine", 29, "cathy@example.com", 62000, "2021-07-10", "2026-01-15"),
    ]
    columns = ["name", "age", "email", "salary", "start_date", "end_date"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def drules_dataframe_dirty(spark):
    """DataFrame with nulls and invalid data for failure tests."""
    data = [
        ("Alice", 34, "alice@example.com", 75000, "2020-01-15", "2024-06-30"),
        (None, 200, "not-an-email", -5000, "2025-01-01", "2020-01-01"),
        ("Bob", None, None, 95000, None, "2025-12-31"),
    ]
    columns = ["name", "age", "email", "salary", "start_date", "end_date"]
    return spark.createDataFrame(data, columns)


def _make_config(config_str):
    return ConfigFactory.parse_string(config_str).get("drules", {})


class TestColumnThreshold:
    def test_column_threshold_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "column_threshold"
                        constraint_name = "age_range"
                        column = "age"
                        min = 0
                        max = 120
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert len(metrics) == 1
        assert metrics[0]["success"] is True

    def test_column_threshold_failure(self, drules_dataframe_dirty):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "column_threshold"
                        constraint_name = "age_range"
                        column = "age"
                        min = 0
                        max = 100
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe_dirty)
        assert len(metrics) == 1
        assert metrics[0]["success"] is False


class TestNullCheck:
    def test_null_check_not_null_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "null_check"
                        constraint_name = "name_not_null"
                        column = "name"
                        mode = "not_null"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert metrics[0]["success"] is True

    def test_null_check_not_null_failure(self, drules_dataframe_dirty):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "null_check"
                        constraint_name = "name_not_null"
                        column = "name"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe_dirty)
        assert metrics[0]["success"] is False


class TestRegex:
    def test_regex_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "regex"
                        constraint_name = "email_format"
                        column = "email"
                        pattern = "^[a-z]+@example\\\\.com$"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert metrics[0]["success"] is True

    def test_regex_failure(self, drules_dataframe_dirty):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "regex"
                        constraint_name = "email_format"
                        column = "email"
                        pattern = "^[a-z]+@example\\\\.com$"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe_dirty)
        assert metrics[0]["success"] is False


class TestCrossColumn:
    def test_cross_column_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "cross_column"
                        constraint_name = "start_before_end"
                        left_column = "start_date"
                        right_column = "end_date"
                        operator = "<"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert metrics[0]["success"] is True

    def test_cross_column_failure(self, drules_dataframe_dirty):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "cross_column"
                        constraint_name = "start_before_end"
                        left_column = "start_date"
                        right_column = "end_date"
                        operator = "<"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe_dirty)
        assert metrics[0]["success"] is False


class TestCustomSQL:
    def test_custom_sql_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "custom_sql"
                        constraint_name = "row_count_check"
                        sql_expression = "SELECT COUNT(*) FROM {table}"
                        expected_operator = ">"
                        expected_value = 0
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert metrics[0]["success"] is True

    def test_custom_sql_failure(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "custom_sql"
                        constraint_name = "impossible_count"
                        sql_expression = "SELECT COUNT(*) FROM {table} WHERE age > 1000"
                        expected_operator = ">"
                        expected_value = 0
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert metrics[0]["success"] is False

    def test_custom_sql_injection_rejected(self):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "custom_sql"
                        sql_expression = "SELECT 1; DROP TABLE users"
                    }
                ]
            }
        """
        )
        with pytest.raises(ConfigurationError, match="prohibited"):
            DrulesEngine(config)


class TestMultiRule:
    def test_multi_rule_success(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "null_check"
                        constraint_name = "name_not_null"
                        column = "name"
                    }
                    {
                        rule_type = "column_threshold"
                        constraint_name = "age_range"
                        column = "age"
                        min = 0
                        max = 120
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        assert len(metrics) == 2
        assert all(m["success"] for m in metrics)

    def test_multi_rule_partial_failure(self, drules_dataframe_dirty):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "null_check"
                        constraint_name = "name_not_null"
                        column = "name"
                    }
                    {
                        rule_type = "column_threshold"
                        constraint_name = "salary_positive"
                        column = "salary"
                        min = 0
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe_dirty)
        assert len(metrics) == 2
        assert any(not m["success"] for m in metrics)


class TestResultFormat:
    def test_result_format(self, drules_dataframe):
        config = _make_config(
            """
            drules {
                checks = [
                    {
                        rule_type = "null_check"
                        constraint_name = "name_not_null"
                        column = "name"
                    }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        metrics = engine.apply(drules_dataframe)
        for m in metrics:
            assert "check" in m
            assert "success" in m
            assert "details" in m
            assert isinstance(m["success"], bool)


class TestEngineDiscovery:
    def test_engine_discovery(self):
        from dq.engine.engine_loader import EngineLoader

        config = _make_config("drules { checks = [] }")
        loader = EngineLoader()
        engine = loader.load_engine("drules", config)
        assert isinstance(engine, DrulesEngine)
