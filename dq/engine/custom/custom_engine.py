# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Import constraints package to trigger auto-registration
import dq.engine.custom.constraints  # noqa: F401
from dq.engine.custom.constraint_registry import ConstraintRegistry
from dq.engine.dq_engine import DQEngine
from dq.utils import constants, repository_utils

logger = logging.getLogger(__name__)


class CustomEngine(DQEngine):
    """Engine providing custom business-rule constraints.

    Dispatches to constraint implementations registered in the
    ``ConstraintRegistry``. Built-in constraints:

    * ``DistinctnessByGroup`` -- validates distinct counts within groups
    * ``RateOfChange`` -- detects sudden value changes between consecutive rows
    * ``LookupBasedOnColumnNameList`` -- checks column names against a reference table
    * ``WideTablesNegativeValuesCheck`` -- finds negative values across wide tables

    External constraints can be added via
    ``ConstraintRegistry.register(name, cls)``.
    """

    def __init__(self, config, dqts: Optional[int] = None):
        self._config = config
        super().__init__(config, dqts)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply custom constraint checks to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.
                Deprecated: Use ``repository_writer`` in constructor instead.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        if repository is not None:
            warnings.warn(
                "The 'repository' parameter is deprecated. "
                "Use 'repository_writer' in the engine constructor instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        custom_checks = self._config.get("checks", {})
        spark_session = dataframe.sparkSession
        _metrics_results = []
        _verification_results = []

        for check_config in custom_checks:
            constraint_name = check_config.get("constraint", None)
            constraint_class = ConstraintRegistry.get(constraint_name)
            constraint_instance = constraint_class()
            _results, _check_verification = constraint_instance.evaluate(
                dataframe, check_config, spark_session
            )
            _metrics_results += _results
            _verification_results += _check_verification

        df_metrics_results = spark_session.createDataFrame(
            _metrics_results, ["entity", "instance", "name", "value"]
        )
        df_check_verification_results = spark_session.createDataFrame(
            _verification_results,
            [
                "check",
                "check_level",
                "check_status",
                "constraint",
                "constraint_status",
                "constraint_message",
            ],
        )

        if repository:
            from pydeequ.repository import ResultKey

            current_milli_time = ResultKey.current_milli_time()
            repository_utils.save_to_repository(
                repository,
                df_metrics_results,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository_utils.save_to_repository(
                repository,
                df_check_verification_results,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )

        summary_metrics = []
        for check in df_metrics_results.collect():
            summary_metrics.append(
                {
                    "check": check["name"],
                    "success": check["value"] == 1,
                    "details": check,
                }
            )

        return summary_metrics
