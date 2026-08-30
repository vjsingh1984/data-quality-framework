# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from pathlib import Path
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
                export_path = "/path/to/profile_report.json"  # Optional: export results
                export_format = "json"  # or "html", "markdown"
                suggest_rules = true  # Generate rule suggestions based on profile
            }
        }
    """

    def __init__(self, config: ConfigTree):
        self._spark_session: Any = None  # Will be set to SparkSession in apply()
        self._last_profile_result: Any = None  # Will be set to ProfileResult in apply()
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

        # Get DataFrame name from config
        dataframe_name = self._config.get("dataframe_name", "unknown")

        # Run profiling based on profile type
        summary_metrics = self._run_profiling(
            dataframe,
            dataframe_name=dataframe_name,
            profile_type=profile_type,
            include_correlation=include_correlation,
            max_unique_values=max_unique_values,
        )

        # Export results if export_path is configured
        export_path = self._config.get("export_path", None)
        if export_path:
            self._export_profile(summary_metrics, export_path)

        return summary_metrics

    def _run_profiling(
        self,
        df: DataFrame,
        dataframe_name: str,
        profile_type: str,
        include_correlation: bool,
        max_unique_values: int,
    ) -> List[Dict[str, Any]]:
        """Run profiling analysis and return metrics.

        Args:
            df: DataFrame to profile.
            dataframe_name: Name of the DataFrame being profiled.
            profile_type: Type of profiling ("basic", "comprehensive", "advanced").
            include_correlation: Whether to compute correlation matrix.
            max_unique_values: Maximum unique values to analyze per column.

        Returns:
            List of profiling metric dicts.
        """
        # Import profiling check functionality
        from dq.engine.profiler.profiler_check import ProfilerCheck
        from dq.engine.profiler.profiler_results import ProfileMetrics

        profiler_check = ProfilerCheck(
            profile_type=profile_type,
            include_correlation=include_correlation,
            max_unique_values=max_unique_values,
        )

        # Get profiling results
        profile_result = profiler_check.profile_dataframe(df, dataframe_name)

        # Store the profile result for export
        self._last_profile_result = profile_result

        # Create single metric with complete profile
        profile_metric = ProfileMetrics(
            check="Profile",
            success=True,
            details={"profile_result": profile_result.to_dict()},
            constraint="Profile",
            dataframe_name=dataframe_name,
            timestamp=profile_result.timestamp,
        )

        logger.info(
            "Profiling completed for DataFrame '%s': %d columns profiled",
            dataframe_name,
            profile_result.general.column_count,
        )

        return [profile_metric.to_dict()]

    def _export_profile(self, metrics: List[Dict[str, Any]], export_path: str) -> None:
        """Export profiling results to a file.

        Args:
            metrics: Profiling metrics to export.
            export_path: Path to export file.
        """
        from dq.engine.profiler.profiler_exporter import ProfileExporter
        from dq.engine.profiler.profiler_results import ProfileResult

        # Extract profile result from metrics
        profile_dict = metrics[0]["details"]["profile_result"]
        profile_result = ProfileResult.from_dict(profile_dict)

        # Determine export format from file extension
        export_format = self._config.get("export_format")
        if export_format is None:
            # Infer from file extension
            path = Path(export_path)
            suffix = path.suffix.lower()
            if suffix == ".json":
                export_format = "json"
            elif suffix == ".html":
                export_format = "html"
            elif suffix in (".md", ".markdown"):
                export_format = "markdown"
            else:
                export_format = "json"

        # Export the profile
        exporter = ProfileExporter()
        exporter.export(profile_result, export_path, format=export_format)

        logger.info("Profile exported to %s (format: %s)", export_path, export_format)
