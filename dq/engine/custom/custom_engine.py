# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Import constraints package to trigger auto-registration
import dq.engine.custom.constraints  # noqa: F401
from dq.engine.custom.constraint_registry import ConstraintRegistry
from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError
from dq.utils import constants, repository_utils

logger = logging.getLogger(__name__)


class CustomEngine(DQEngine):
    """Engine providing custom business-rule constraints.

    Dispatches to constraint implementations registered in the
    ``ConstraintRegistry``. Built-in constraints:

    * ``DistinctnessByGroup`` -- validates distinct counts within groups
    * ``RateOfChange`` -- detects sudden value changes between consecutive rows
    * ``LookupColumnList`` -- checks column names against a reference table
    * ``WideTablesNegativeValuesCheck`` -- finds negative values across wide tables

    External constraints can be added via
    ``ConstraintRegistry.register(name, cls)``.
    """

    def __init__(self, config):
        self._config = config
        self._spark_session = None  # Will be set in apply()
        super().__init__(config)

    def _validate_config(self) -> None:
        """Validate CustomEngine configuration at init time."""
        checks = self._config.get("checks", [])
        if not checks:
            raise ConfigurationError(
                "CustomEngine requires 'checks' in configuration with at least one check."
            )

        for i, check in enumerate(checks):
            try:
                constraint_name = check.get("constraint")
            except Exception:
                constraint_name = None

            if not constraint_name:
                raise ConfigurationError(
                    f"Check at index {i} is missing required 'constraint' key."
                )
            if not ConstraintRegistry.is_registered(constraint_name):
                raise ConfigurationError(
                    f"Unknown constraint '{constraint_name}' at index {i}. "
                    f"Available: {', '.join(ConstraintRegistry.list_constraints())}"
                )

    def before_apply(self, dataframe: DataFrame) -> None:
        """Hook called before apply() - cache reference DataFrames if needed."""
        super().before_apply(dataframe)

        # Cache spark session for use in apply()
        self._spark_session = dataframe.sparkSession

        # Pre-load and cache reference DataFrames for LookupColumnList constraints
        self._cache_reference_dataframes(dataframe)

    def after_apply(self, dataframe: DataFrame, metrics: List[Dict[str, Any]]) -> None:
        """Hook called after apply() - cleanup cached DataFrames."""
        super().after_apply(dataframe, metrics)

        # Unpersist all cached DataFrames
        for cache_key, cached_df in self._cache.items():
            if hasattr(cached_df, "unpersist"):
                cached_df.unpersist()
        self._cache.clear()

    def _cache_reference_dataframes(self, dataframe: DataFrame) -> None:
        """Pre-load and cache reference DataFrames for LookupColumnList constraints."""
        checks = self._config.get("checks", [])

        for check_config in checks:
            constraint_name = check_config.get("constraint")
            if constraint_name == "LookupColumnList":
                ref_table = check_config.get("ref_table")
                if ref_table and ref_table not in self._cache:
                    try:
                        ref_df = self._spark_session.table(ref_table)
                        self._cache[f"ref_{ref_table}"] = ref_df.cache()
                        logger.debug(
                            "Cached reference table '%s' for LookupColumnList",
                            ref_table,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to cache reference table '%s': %s", ref_table, e
                        )

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

        # Call lifecycle hooks
        self.before_apply(dataframe)

        try:
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

            metrics_dataframe = spark_session.createDataFrame(
                _metrics_results, ["entity", "instance", "name", "value"]
            )
            verifications_dataframe = spark_session.createDataFrame(
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
                    metrics_dataframe,
                    constants.DQ_REPOSITORY_METRICS,
                    current_milli_time,
                )
                repository_utils.save_to_repository(
                    repository,
                    verifications_dataframe,
                    constants.DQ_REPOSITORY_VERIFICATIONS,
                    current_milli_time,
                )

            summary_metrics = []
            for check in metrics_dataframe.collect():
                check_name = check["name"]
                # Extract constraint from check name if possible
                constraint = check_name

                metric = self._create_metric(
                    check=check_name,
                    success=check["value"] == 1,
                    details=check,
                    constraint=constraint,
                )
                summary_metrics.append(metric.to_dict())

            return summary_metrics
        finally:
            # Always call after_apply, even if an exception occurred
            self.after_apply(dataframe, [])
