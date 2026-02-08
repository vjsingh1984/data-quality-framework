# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.engine.greatexpectations.greatexpectations_check import GreatexpectationsCheck
from dq.exceptions import ConfigurationError, EngineExecutionError
from dq.utils import constants

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Supported GE expectation types
SUPPORTED_EXPECTATIONS = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_between",
    "expect_column_to_exist",
    "expect_column_values_to_match_regex",
    "expect_table_row_count_to_be_between",
    "expect_column_mean_to_be_between",
    "expect_column_values_to_be_in_set",
}

# Table-level expectations that don't require a column parameter
TABLE_LEVEL_EXPECTATIONS = {
    "expect_table_row_count_to_be_between",
}


class GreatexpectationsEngine(DQEngine):
    """Engine that validates DataFrames using Great Expectations.

    Wraps a GE ``SparkDFDataset`` and dynamically applies expectations
    defined in the HOCON configuration.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)

    def _validate_config(self) -> None:
        """Validate expectations configuration at init time (fail-fast)."""
        # Accept 'checks' as alias for 'expectations'
        expectations = self._config.get(
            "expectations", self._config.get("checks", None)
        )
        if expectations is None:
            raise ConfigurationError(
                "Great Expectations engine requires 'expectations' (or 'checks') key "
                "in configuration."
            )

        for i, exp in enumerate(expectations):
            exp_type = exp.get("type", None)
            if not exp_type:
                raise ConfigurationError(
                    f"Expectation at index {i} is missing required 'type' key."
                )
            if exp_type not in SUPPORTED_EXPECTATIONS:
                raise ConfigurationError(
                    f"Unsupported expectation type '{exp_type}'. "
                    f"Supported: {sorted(SUPPORTED_EXPECTATIONS)}"
                )
            if exp_type not in TABLE_LEVEL_EXPECTATIONS:
                column = exp.get("column", None)
                if not column:
                    raise ConfigurationError(
                        f"Expectation '{exp_type}' requires a 'column' parameter."
                    )

    def apply(self, df: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply Great Expectations checks to the given DataFrame.

        Args:
            df: Spark DataFrame to validate.
            repository: Optional repository config (unused).

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        try:
            import great_expectations as ge
        except ImportError:
            raise EngineExecutionError(
                "great-expectations is not installed. "
                "Install with: poetry install -E ge"
            )

        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        try:
            ge_df = ge.dataset.SparkDFDataset(df)
        except Exception as e:
            raise EngineExecutionError(f"Failed to create SparkDFDataset: {e}") from e

        # Accept 'checks' as alias for 'expectations'
        expectations = self._config.get("expectations", self._config.get("checks", []))
        ge_checks = GreatexpectationsCheck(expectations)

        try:
            ge_checks.apply_checks(ge_df)
            validation_output = ge_df.validate()
        except Exception as e:
            raise EngineExecutionError(
                f"Great Expectations validation failed: {e}"
            ) from e

        metrics = self._extract_metrics_from_validation_output(validation_output)
        return metrics

    def _extract_metrics_from_validation_output(
        self, validation_output
    ) -> List[Dict[str, Any]]:
        """Extract metric dictionaries from GE validation output."""
        summarymetrics = []
        for exp_result in validation_output.results:
            check_type = exp_result.expectation_config.expectation_type

            # Convert GE result to dict if it isn't already
            details = exp_result.result
            if hasattr(details, "to_dict"):
                details = details.to_dict()
            elif not isinstance(details, dict):
                details = {"result": str(details)}

            metric = self._create_metric(
                check=check_type,
                success=exp_result.success,
                details=details,
                constraint=check_type,
            )
            summarymetrics.append(metric.to_dict())
        return summarymetrics
