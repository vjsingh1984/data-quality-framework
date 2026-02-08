# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ProfilerEngine(DQEngine):
    """Engine that performs data profiling and statistical analysis.

    Generates comprehensive data profiles including:
    - Statistical summaries (count, mean, stddev, min, max, percentiles)
    - Data type analysis
    - Null value analysis
    - Unique value analysis
    - Distribution analysis
    - Correlation analysis (optional)

    The profiling results can be used to:
    - Understand data distributions
    - Identify data quality issues proactively
    - Suggest appropriate validation rules
    - Export profile reports for documentation

    Usage::

        dqframework {
            profiler {
                enabled = true
                profile_type = "comprehensive"  # or "basic", "advanced"
                include_correlation = false  # Optional: compute correlations
                max_unique_values = 100  # Limit unique value analysis
                export_format = "json"  # or "html", "markdown"
                suggest_rules = true  # Generate rule suggestions based on profile
            }
        }
    """

    def __init__(self, config: ConfigTree):
        self._spark_session = None
        super().__init__(config)

    def _validate_config(self) -> None:
        """Validate ProfilerEngine configuration at init time."""
        # ProfilerEngine doesn't require specific config
        # It can run with defaults
        pass

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run profiling analysis on the DataFrame.

        Args:
            dataframe: Spark DataFrame to profile.
            repository: Optional repository config for persisting profiles.
                Deprecated: Use repository_writer in constructor instead.

        Returns:
            List of metric dicts with profiling results.
        """
        if repository is not None:
            warnings.warn(
                "The 'repository' parameter is deprecated. "
                "Use 'repository_writer' in the engine constructor instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        rule_name = self._config.get("rule_name", "Profiler")
        engine_name = self._config.get("engine_name", "profiler")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        self._spark_session = dataframe.sparkSession

        profile_type = self._config.get("profile_type", "basic")
        include_correlation = self._config.get("include_correlation", False)
        max_unique_values = self._config.get("max_unique_values", 100)

        # Run profiling based on profile type
        summary_metrics = self._run_profiling(
            dataframe,
            profile_type=profile_type,
            include_correlation=include_correlation,
            max_unique_values=max_unique_values,
        )

        return summary_metrics

    def _run_profiling(
        self,
        df: DataFrame,
        profile_type: str,
        include_correlation: bool,
        max_unique_values: int,
    ) -> List[Dict[str, Any]]:
        """Run profiling analysis and return metrics.

        Args:
            df: DataFrame to profile.
            profile_type: Type of profiling ("basic", "comprehensive", "advanced").
            include_correlation: Whether to compute correlation matrix.
            max_unique_values: Maximum unique values to analyze per column.

        Returns:
            List of profiling metric dicts.
        """
        metrics = []

        # Import profiling check functionality
        from dq.engine.profiler.profiler_check import ProfilerCheck

        profiler_check = ProfilerCheck(
            profile_type=profile_type,
            include_correlation=include_correlation,
            max_unique_values=max_unique_values,
        )

        # Get profiling results
        profile_results = profiler_check.profile_dataframe(df)

        # Convert to metric format
        for metric_name, metric_value in profile_results.items():
            metric = self._create_metric(
                check=f"Profile.{metric_name}",
                success=True,
                details={"profile_result": metric_value},
                constraint="Profile",
            )
            metrics.append(metric.to_dict())

        logger.info(
            "Profiling completed: %d metrics generated",
            len(metrics),
        )

        return metrics
