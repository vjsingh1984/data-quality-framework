# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Great Expectations engine — no Spark required."""
from unittest.mock import MagicMock

import pytest
from pyhocon import ConfigFactory

from dq.exceptions import ConfigurationError


class TestGEConfigValidation:
    """Test configuration validation in GreatexpectationsEngine."""

    def _make_config(self, config_str):
        return ConfigFactory.parse_string(config_str).get("ge", {})

    def test_missing_expectations_key_raises(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config('ge { name = "test" }')
        with pytest.raises(ConfigurationError, match="expectations"):
            GreatexpectationsEngine(config)

    def test_empty_expectations_list_ok(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config("ge { expectations = [] }")
        engine = GreatexpectationsEngine(config)
        assert engine is not None

    def test_checks_alias_accepted(self):
        """'checks' key should work as alias for 'expectations'."""
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config(
            """
            ge {
                checks = [
                    { type = "expect_column_values_to_not_be_null", column = "id" }
                ]
            }
        """
        )
        engine = GreatexpectationsEngine(config)
        assert engine is not None

    def test_missing_type_raises(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config(
            """
            ge {
                expectations = [
                    { column = "id" }
                ]
            }
        """
        )
        with pytest.raises(ConfigurationError, match="type"):
            GreatexpectationsEngine(config)

    def test_unknown_type_raises(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config(
            """
            ge {
                expectations = [
                    { type = "expect_nonsense_xyz", column = "id" }
                ]
            }
        """
        )
        with pytest.raises(ConfigurationError, match="Unsupported"):
            GreatexpectationsEngine(config)

    @pytest.mark.parametrize(
        "exp_type",
        [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_unique",
            "expect_column_values_to_be_between",
            "expect_column_to_exist",
            "expect_column_values_to_match_regex",
            "expect_table_row_count_to_be_between",
            "expect_column_mean_to_be_between",
            "expect_column_values_to_be_in_set",
        ],
    )
    def test_valid_types_accepted(self, exp_type):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        # Table-level expectations don't need column
        table_level = {"expect_table_row_count_to_be_between"}
        col_part = "" if exp_type in table_level else ', column = "id"'
        config = self._make_config(
            f"""
            ge {{
                expectations = [
                    {{ type = "{exp_type}"{col_part} }}
                ]
            }}
        """
        )
        engine = GreatexpectationsEngine(config)
        assert engine is not None

    def test_column_required_for_column_level(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        config = self._make_config(
            """
            ge {
                expectations = [
                    { type = "expect_column_values_to_not_be_null" }
                ]
            }
        """
        )
        with pytest.raises(ConfigurationError, match="column"):
            GreatexpectationsEngine(config)


class TestGEMetricExtraction:
    """Test metric extraction from GE validation output."""

    def _get_engine_class(self):
        from dq.engine.greatexpectations.greatexpectations_engine import (
            GreatexpectationsEngine,
        )

        return GreatexpectationsEngine

    def _make_mock_result(
        self, success, exp_type="expect_column_values_to_not_be_null", observed=None
    ):
        result = MagicMock()
        result.success = success
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = exp_type
        result.result = {"observed_value": observed} if observed else {}
        return result

    def test_success_format(self):
        cls = self._get_engine_class()
        engine = cls.__new__(cls)
        validation = MagicMock()
        validation.results = [self._make_mock_result(True)]

        metrics = engine._extract_metrics_from_validation_output(validation)
        assert len(metrics) == 1
        assert metrics[0]["success"] is True
        assert "check" in metrics[0]
        assert "details" in metrics[0]

    def test_failure_format(self):
        cls = self._get_engine_class()
        engine = cls.__new__(cls)
        validation = MagicMock()
        validation.results = [self._make_mock_result(False)]

        metrics = engine._extract_metrics_from_validation_output(validation)
        assert metrics[0]["success"] is False

    def test_empty_results(self):
        cls = self._get_engine_class()
        engine = cls.__new__(cls)
        validation = MagicMock()
        validation.results = []

        metrics = engine._extract_metrics_from_validation_output(validation)
        assert metrics == []

    def test_details_include_observed_value(self):
        cls = self._get_engine_class()
        engine = cls.__new__(cls)
        validation = MagicMock()
        validation.results = [self._make_mock_result(True, observed=42)]

        metrics = engine._extract_metrics_from_validation_output(validation)
        assert metrics[0]["details"]["observed_value"] == 42


class TestGECheckUnit:
    """Test GreatexpectationsCheck dispatches correctly."""

    def test_correct_method_called(self):
        from dq.engine.greatexpectations.greatexpectations_check import (
            GreatexpectationsCheck,
        )

        config = [{"type": "expect_column_values_to_not_be_null", "column": "name"}]
        check = GreatexpectationsCheck(config)

        ge_df = MagicMock()
        ge_df.expect_column_values_to_not_be_null = MagicMock()
        check.apply_checks(ge_df)
        ge_df.expect_column_values_to_not_be_null.assert_called_once_with("name")

    def test_kwargs_forwarded(self):
        from dq.engine.greatexpectations.greatexpectations_check import (
            GreatexpectationsCheck,
        )

        config = [
            {
                "type": "expect_column_values_to_be_between",
                "column": "age",
                "kwargs": {"min_value": 0, "max_value": 120},
            }
        ]
        check = GreatexpectationsCheck(config)

        ge_df = MagicMock()
        ge_df.expect_column_values_to_be_between = MagicMock()
        check.apply_checks(ge_df)
        ge_df.expect_column_values_to_be_between.assert_called_once_with(
            "age", min_value=0, max_value=120
        )

    def test_table_level_expectations_no_column(self):
        from dq.engine.greatexpectations.greatexpectations_check import (
            GreatexpectationsCheck,
        )

        config = [
            {
                "type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": 1000},
            }
        ]
        check = GreatexpectationsCheck(config)

        ge_df = MagicMock()
        ge_df.expect_table_row_count_to_be_between = MagicMock()
        check.apply_checks(ge_df)
        ge_df.expect_table_row_count_to_be_between.assert_called_once_with(
            min_value=1, max_value=1000
        )

    def test_unknown_expectation_raises(self):
        from dq.engine.greatexpectations.greatexpectations_check import (
            GreatexpectationsCheck,
        )

        config = [{"type": "expect_nonsense_xyz", "column": "id"}]
        check = GreatexpectationsCheck(config)

        ge_df = MagicMock(spec=[])  # no attributes
        with pytest.raises(ConfigurationError, match="not found"):
            check.apply_checks(ge_df)
