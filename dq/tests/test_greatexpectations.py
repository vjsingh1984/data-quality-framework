# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Spark integration tests for the Great Expectations engine."""
import pytest

ge = pytest.importorskip("great_expectations")

from pyhocon import ConfigFactory

from dq.engine.greatexpectations.greatexpectations_engine import (
    GreatExpectationsEngine,
)
from dq.exceptions import ConfigurationError


@pytest.fixture
def ge_dataframe(spark):
    """DataFrame with clean data for GE tests."""
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Bob", 45, "bob@example.com"),
        ("Catherine", 49, "cathy@example.com"),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def ge_dataframe_dirty(spark):
    """DataFrame with nulls and duplicates for failure tests."""
    data = [
        ("Alice", 34, "alice@example.com"),
        ("Alice", None, "not-an-email"),
        (None, 200, None),
    ]
    columns = ["name", "age", "email"]
    return spark.createDataFrame(data, columns)


def _make_config(config_str):
    return ConfigFactory.parse_string(config_str).get("ge", {})


class TestGENotNull:
    def test_ge_not_null_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert len(metrics) == 1
        assert metrics[0]["success"] is True

    def test_ge_not_null_failure(self, ge_dataframe_dirty):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe_dirty)
        assert len(metrics) == 1
        assert metrics[0]["success"] is False


class TestGEUnique:
    def test_ge_unique_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_be_unique", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert len(metrics) == 1
        assert metrics[0]["success"] is True

    def test_ge_unique_failure(self, ge_dataframe_dirty):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_be_unique", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe_dirty)
        assert len(metrics) == 1
        assert metrics[0]["success"] is False


class TestGEBetween:
    def test_ge_between_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    {
                        type = "expect_column_values_to_be_between"
                        column = "age"
                        kwargs { min_value = 0, max_value = 120 }
                    }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert metrics[0]["success"] is True

    def test_ge_between_failure(self, ge_dataframe_dirty):
        config = _make_config(
            """
            ge {
                expectations = [
                    {
                        type = "expect_column_values_to_be_between"
                        column = "age"
                        kwargs { min_value = 0, max_value = 100 }
                    }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe_dirty)
        assert metrics[0]["success"] is False


class TestGEColumnExists:
    def test_ge_column_exists_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_to_exist", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert metrics[0]["success"] is True

    def test_ge_column_exists_failure(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_to_exist", column = "nonexistent" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert metrics[0]["success"] is False


class TestGERegex:
    def test_ge_regex_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    {
                        type = "expect_column_values_to_match_regex"
                        column = "email"
                        kwargs { regex = "^[a-z]+@example\\\\.com$" }
                    }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert metrics[0]["success"] is True


class TestGEMultiExpectation:
    def test_ge_multi_expectation_success(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "name" }
                    { type = "expect_column_values_to_be_unique", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        assert len(metrics) == 2
        assert all(m["success"] for m in metrics)

    def test_ge_multi_expectation_failure(self, ge_dataframe_dirty):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "name" }
                    { type = "expect_column_values_to_be_unique", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe_dirty)
        assert len(metrics) == 2
        assert any(not m["success"] for m in metrics)


class TestGEResultFormat:
    def test_ge_result_format(self, ge_dataframe):
        config = _make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "name" }
                ]
            }
        """
        )
        engine = GreatExpectationsEngine(config)
        metrics = engine.apply(ge_dataframe)
        for m in metrics:
            assert "check" in m
            assert "success" in m
            assert "details" in m
            assert isinstance(m["success"], bool)


class TestGEInvalidConfig:
    def test_ge_invalid_config_raises(self):
        config = _make_config('ge { name = "bad" }')
        with pytest.raises(ConfigurationError):
            GreatExpectationsEngine(config)
