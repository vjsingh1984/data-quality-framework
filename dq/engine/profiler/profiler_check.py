# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Import Spark functions for runtime use
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class ProfilerCheck:
    """Performs statistical profiling and data analysis on DataFrames.

    Generates comprehensive data profiles including statistics,
    distributions, and quality metrics that can be used to understand
    data characteristics and suggest appropriate validation rules.

    Supports three profiling levels:
    - **basic**: Row/column counts, null analysis, basic statistics
    - **comprehensive**: Everything in basic + unique values, percentiles, patterns
    - **advanced**: Everything in comprehensive + correlations, histograms, outliers
    """

    def __init__(
        self,
        profile_type: str = "basic",
        include_correlation: bool = False,
        max_unique_values: int = 100,
    ):
        """Initialize the profiler.

        Args:
            profile_type: Type of profiling ("basic", "comprehensive", "advanced").
            include_correlation: Whether to include correlation analysis.
            max_unique_values: Maximum unique values to analyze per column.
        """
        self._profile_type = profile_type
        self._include_correlation = include_correlation
        self._max_unique_values = max_unique_values

    def profile_dataframe(self, dataframe: DataFrame) -> Dict[str, Any]:
        """Profile a DataFrame and return analysis results.

        Args:
            dataframe: Spark DataFrame to profile.

        Returns:
            Dictionary containing profiling metrics organized by category:
            - general: Overall statistics (row_count, column_count, etc.)
            - columns: Per-column statistics and analysis
            - correlations: Correlation matrix (if requested)
            - suggestions: Rule suggestions based on profile (if requested)
        """
        profile = {
            "general": self._profile_general(dataframe),
            "columns": self._profile_columns(dataframe),
        }

        # Add correlations for advanced or explicitly requested profiling
        if self._profile_type in ("advanced",) or self._include_correlation:
            profile["correlations"] = self._profile_correlations(dataframe)

        # Add advanced features for advanced profile type
        if self._profile_type == "advanced":
            profile["distributions"] = self._profile_distributions(dataframe)
            profile["outliers"] = self._detect_outliers(dataframe)

        # Generate rule suggestions
        profile["suggestions"] = self._generate_suggestions(dataframe, profile)

        return profile

    def _profile_general(self, dataframe: DataFrame) -> Dict[str, Any]:
        """Generate general dataset statistics.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary with general statistics.
        """
        row_count = dataframe.count()
        column_count = len(dataframe.columns)

        # Get schema information
        schema = dataframe.schema
        column_names = schema.names
        column_types = [str(field.dataType) for field in schema.fields]

        return {
            "row_count": row_count,
            "column_count": column_count,
            "columns": column_names,
            "schema": {name: dtype for name, dtype in zip(column_names, column_types)},
            "size_bytes": row_count
            * sum(self._estimate_size(dtype) for dtype in column_types),
        }

    def _profile_columns(self, dataframe: DataFrame) -> Dict[str, Dict[str, Any]]:
        """Generate per-column profiling statistics.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary mapping column names to their profiles.
        """
        columns_profile = {}

        for field in dataframe.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)

            # Get null count and percentage
            null_count = dataframe.where(F.col(col_name).isNull()).count()
            total_count = dataframe.count()
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

            column_profile = {
                "data_type": col_type,
                "nullable": field.nullable,
                "null_count": null_count,
                "null_percentage": null_percentage,
            }

            # Add type-specific statistics
            if self._is_numeric_type(col_type):
                column_profile.update(self._profile_numeric_column(dataframe, col_name))
            elif self._is_string_type(col_type):
                column_profile.update(self._profile_string_column(dataframe, col_name))
            elif self._is_date_type(col_type):
                column_profile.update(self._profile_date_column(dataframe, col_name))

            # Add unique/distinct analysis for comprehensive/advanced
            if self._profile_type in ("comprehensive", "advanced"):
                column_profile.update(self._profile_unique_values(dataframe, col_name))

            columns_profile[col_name] = column_profile

        return columns_profile

    def _profile_numeric_column(
        self, dataframe: DataFrame, column_name: str
    ) -> Dict[str, Any]:
        """Profile a numeric column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the numeric column.

        Returns:
            Dictionary with numeric statistics.
        """
        # Import Spark functions
        from pyspark.sql import functions as F

        # Get basic statistics
        stats = dataframe.select(
            F.min(column_name).alias("min"),
            F.max(column_name).alias("max"),
            F.mean(column_name).alias("mean"),
            F.stddev_pop(column_name).alias("stddev"),
        ).first()

        numeric_stats = {
            "min": stats["min"] if stats else None,
            "max": stats["max"] if stats else None,
            "mean": stats["mean"] if stats else None,
            "stddev": stats["stddev"] if stats else None,
        }

        # Add percentiles for comprehensive/advanced
        if self._profile_type in ("comprehensive", "advanced"):
            percentiles = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
            exprs = [
                F.percentile_approx(column_name, p).alias(f"p{int(p*100)}")
                for p in percentiles
            ]
            percentile_results = dataframe.select(exprs).first()

            for i, p in enumerate(percentiles):
                if percentile_results:
                    numeric_stats[f"p{int(p*100)}"] = percentile_results[i]

        return numeric_stats

    def _profile_string_column(
        self, dataframe: DataFrame, column_name: str
    ) -> Dict[str, Any]:
        """Profile a string column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the string column.

        Returns:
            Dictionary with string statistics.
        """
        from pyspark.sql import functions as F

        # Get length statistics
        length_stats = dataframe.select(
            F.min(F.length(column_name)).alias("min_length"),
            F.max(F.length(column_name)).alias("max_length"),
            F.mean(F.length(column_name)).alias("avg_length"),
        ).first()

        string_stats = {
            "min_length": length_stats["min_length"] if length_stats else None,
            "max_length": length_stats["max_length"] if length_stats else None,
            "avg_length": length_stats["avg_length"] if length_stats else None,
        }

        # Add pattern detection for comprehensive/advanced
        if self._profile_type in ("comprehensive", "advanced"):
            string_stats["patterns"] = self._detect_patterns(dataframe, column_name)

        return string_stats

    def _profile_date_column(
        self, dataframe: DataFrame, column_name: str
    ) -> Dict[str, Any]:
        """Profile a date/timestamp column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the date column.

        Returns:
            Dictionary with date statistics.
        """
        from pyspark.sql import functions as F

        # Get date range
        date_stats = dataframe.select(
            F.min(column_name).alias("min_date"),
            F.max(column_name).alias("max_date"),
        ).first()

        return {
            "min_date": str(date_stats["min_date"]) if date_stats["min_date"] else None,
            "max_date": str(date_stats["max_date"]) if date_stats["max_date"] else None,
        }

    def _profile_unique_values(
        self, dataframe: DataFrame, column_name: str
    ) -> Dict[str, Any]:
        """Profile unique values for a column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the column.

        Returns:
            Dictionary with unique value statistics.
        """
        # Get distinct count
        distinct_count = dataframe.select(column_name).distinct().count()
        total_count = dataframe.count()

        # Get sample unique values (limited by max_unique_values)
        unique_values = (
            dataframe.select(column_name)
            .distinct()
            .limit(self._max_unique_values)
            .rdd.map(lambda row: row[0])
            .collect()
        )

        return {
            "distinct_count": distinct_count,
            "unique_percentage": (
                (distinct_count / total_count * 100) if total_count > 0 else 0
            ),
            "sample_values": unique_values[: self._max_unique_values],
        }

    def _profile_correlations(self, dataframe: DataFrame) -> Dict[str, float]:
        """Compute correlation matrix for numeric columns.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary mapping column pairs to correlation coefficients.
        """
        from pyspark.ml.stat import Correlation

        # Get numeric columns
        numeric_cols = [
            field.name
            for field in dataframe.schema.fields
            if self._is_numeric_type(str(field.dataType))
        ]

        if len(numeric_cols) < 2:
            return {}

        correlations = {}

        # Compute pairwise correlations
        for i, col1 in enumerate(numeric_cols):
            for col2 in numeric_cols[i + 1 :]:
                try:
                    corr = Correlation.corr(dataframe, col1, col2)
                    if corr:
                        correlations[f"{col1}_{col2}"] = corr
                except Exception as e:
                    logger.debug(
                        "Failed to compute correlation between %s and %s: %s",
                        col1,
                        col2,
                        e,
                    )

        return correlations

    def _profile_distributions(self, dataframe: DataFrame) -> Dict[str, Any]:
        """Analyze data distributions for numeric columns.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary with distribution analysis results.
        """
        # This is a placeholder for distribution analysis
        # In a full implementation, this would compute histograms, skewness, kurtosis, etc.
        return {}

    def _detect_outliers(self, dataframe: DataFrame) -> Dict[str, Any]:
        """Detect outliers in numeric columns using statistical methods.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary with outlier detection results.
        """
        # This is a placeholder for outlier detection
        # In a full implementation, this would use Z-score, IQR, or other methods
        return {}

    def _detect_patterns(
        self, dataframe: DataFrame, column_name: str
    ) -> Dict[str, int]:
        """Detect common patterns in string columns (emails, URLs, dates, etc.).

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the string column.

        Returns:
            Dictionary mapping pattern names to match counts.
        """

        # Pattern definitions
        patterns = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "url": r"^https?://[^\s]+$",
            "date_iso": r"^\d{4}-\d{2}-\d{2}$",
            "datetime_iso": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        }

        pattern_counts = {}

        for pattern_name, pattern_regex in patterns.items():
            try:
                # Count matches
                count = dataframe.filter(
                    F.col(column_name).rlike(pattern_regex)
                ).count()
                pattern_counts[pattern_name] = count
            except Exception:
                pattern_counts[pattern_name] = 0

        return pattern_counts

    def _generate_suggestions(
        self, dataframe: DataFrame, profile: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Suggest validation rules based on profile data.

        Args:
            dataframe: The profiled DataFrame.
            profile: Profiling results.

        Returns:
            List of rule suggestion dictionaries.
        """
        suggestions = []

        columns_profile = profile.get("columns", {})

        for column_name, column_profile in columns_profile.items():
            # Suggest completeness checks for columns with high null percentage
            null_pct = column_profile.get("null_percentage", 0)
            if null_pct > 5:
                suggestions.append(
                    {
                        "column": column_name,
                        "issue": "high_null_percentage",
                        "suggestion": f"Completeness check recommended ({null_pct:.1f}% null)",
                        "constraint": "Completeness",
                        "severity": "warning" if null_pct < 20 else "error",
                    }
                )

            # Suggest uniqueness checks for low distinct percentage
            if "distinct_count" in column_profile:
                total_count = profile["general"]["row_count"]
                distinct_pct = column_profile["distinct_count"]
                uniqueness_pct = (
                    (distinct_pct / total_count * 100) if total_count > 0 else 0
                )

                if uniqueness_pct < 90 and column_profile.get("data_type") in (
                    "StringType",
                    "IntegerType",
                ):
                    suggestions.append(
                        {
                            "column": column_name,
                            "issue": "low_uniqueness",
                            "suggestion": f"Uniqueness check recommended ({uniqueness_pct:.1f}% unique)",
                            "constraint": "Uniqueness",
                            "severity": "info",
                        }
                    )

            # Suggest range checks for numeric columns with min/max
            if "min" in column_profile and "max" in column_profile:
                min_val = column_profile["min"]
                max_val = column_profile["max"]
                if min_val is not None and max_val is not None:
                    suggestions.append(
                        {
                            "column": column_name,
                            "issue": "numeric_range_identified",
                            "suggestion": f"Range check suggested: [{min_val}, {max_val}]",
                            "constraint": "RangeCheck",
                            "severity": "info",
                        }
                    )

            # Suggest pattern checks for string columns with detected patterns
            if "patterns" in column_profile:
                patterns = column_profile.get("patterns", {})
                if patterns.get("email", 0) > len(dataframe) * 0.5:
                    suggestions.append(
                        {
                            "column": column_name,
                            "issue": "email_format_detected",
                            "suggestion": "Email format pattern check recommended",
                            "constraint": "PatternMatch",
                            "severity": "info",
                        }
                    )

                if patterns.get("url", 0) > len(dataframe) * 0.5:
                    suggestions.append(
                        {
                            "column": column_name,
                            "issue": "url_format_detected",
                            "suggestion": "URL format pattern check recommended",
                            "constraint": "PatternMatch",
                            "severity": "info",
                        }
                    )

        return suggestions

    def _is_numeric_type(self, dtype: str) -> bool:
        """Check if a data type is numeric."""
        numeric_types = [
            "ByteType",
            "ShortType",
            "IntegerType",
            "LongType",
            "FloatType",
            "DoubleType",
            "DecimalType",
        ]
        return any(nt in dtype for nt in numeric_types)

    def _is_string_type(self, dtype: str) -> bool:
        """Check if a data type is string-based."""
        return "StringType" in dtype or "VarcharType" in dtype

    def _is_date_type(self, dtype: str) -> bool:
        """Check if a data type is date/timestamp."""
        date_types = ["DateType", "TimestampType"]
        return any(dt in dtype for dt in date_types)

    def _estimate_size(self, dtype: str) -> int:
        """Estimate the byte size for a data type.

        Args:
            dtype: Spark data type string.

        Returns:
            Estimated size in bytes.
        """
        # Basic size estimates (these are approximations)
        if "ByteType" in dtype:
            return 1
        elif "ShortType" in dtype:
            return 2
        elif "IntegerType" in dtype:
            return 4
        elif "LongType" in dtype:
            return 8
        elif "FloatType" in dtype:
            return 4
        elif "DoubleType" in dtype:
            return 8
        elif "DecimalType" in dtype:
            return 16  # Default precision
        elif "StringType" in dtype or "VarcharType" in dtype:
            return 20  # Average string length (approximation)
        elif "DateType" in dtype or "TimestampType" in dtype:
            return 8
        elif "BooleanType" in dtype:
            return 1
        else:
            return 20  # Default for unknown types
