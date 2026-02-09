# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Import Spark functions for runtime use
from pyspark.sql import functions as F

from dq.engine.profiler.profiler_results import (
    ColumnProfile,
    DateStatistics,
    GeneralStatistics,
    NumericStatistics,
    ProfileResult,
    RuleSuggestion,
    StringStatistics,
    UniqueValueStatistics,
)

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

    def profile_dataframe(
        self, dataframe: DataFrame, dataframe_name: str = "unknown"
    ) -> ProfileResult:
        """Profile a DataFrame and return analysis results.

        Args:
            dataframe: Spark DataFrame to profile.
            dataframe_name: Name of the DataFrame being profiled.

        Returns:
            ProfileResult containing profiling metrics organized by category.
        """
        general_stats = self._profile_general(dataframe)
        columns_profile = self._profile_columns(dataframe)

        correlations = None
        # Add correlations for advanced or explicitly requested profiling
        if self._profile_type in ("advanced",) or self._include_correlation:
            correlations = self._profile_correlations(dataframe)

        # Generate rule suggestions
        profile_dict = {
            "general": general_stats,
            "columns": columns_profile,
        }
        suggestions = self._generate_suggestions(dataframe, profile_dict)

        return ProfileResult(
            timestamp=datetime.now(),
            dataframe_name=dataframe_name,
            general=general_stats,
            columns=columns_profile,
            correlations=correlations,
            suggestions=suggestions,
            profile_type=self._profile_type,
        )

    def _profile_general(self, dataframe: DataFrame) -> GeneralStatistics:
        """Generate general dataset statistics.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            GeneralStatistics object with general statistics.
        """
        row_count = dataframe.count()
        column_count = len(dataframe.columns)

        # Get schema information
        schema = dataframe.schema
        column_names = list(schema.names)
        column_types = [str(field.dataType) for field in schema.fields]

        return GeneralStatistics(
            row_count=row_count,
            column_count=column_count,
            columns=column_names,
            schema={name: dtype for name, dtype in zip(column_names, column_types)},
            size_bytes=row_count
            * sum(self._estimate_size(dtype) for dtype in column_types),
        )

    def _profile_columns(self, dataframe: DataFrame) -> Dict[str, ColumnProfile]:
        """Generate per-column profiling statistics.

        Args:
            dataframe: DataFrame to analyze.

        Returns:
            Dictionary mapping column names to ColumnProfile objects.
        """
        columns_profile: Dict[str, ColumnProfile] = {}
        total_count = dataframe.count()

        for field in dataframe.schema.fields:
            col_name = field.name
            col_type = str(field.dataType)

            # Get null count and percentage
            null_count = dataframe.where(F.col(col_name).isNull()).count()
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

            numeric_stats: Optional[NumericStatistics] = None
            string_stats: Optional[StringStatistics] = None
            date_stats: Optional[DateStatistics] = None
            unique_value_stats: Optional[UniqueValueStatistics] = None

            # Add type-specific statistics
            if self._is_numeric_type(col_type):
                numeric_stats = self._profile_numeric_column(dataframe, col_name)
            elif self._is_string_type(col_type):
                string_stats = self._profile_string_column(dataframe, col_name)
            elif self._is_date_type(col_type):
                date_stats = self._profile_date_column(dataframe, col_name)

            # Add unique/distinct analysis for comprehensive/advanced
            if self._profile_type in ("comprehensive", "advanced"):
                unique_value_stats = self._profile_unique_values(dataframe, col_name)

            columns_profile[col_name] = ColumnProfile(
                name=col_name,
                data_type=col_type,
                nullable=field.nullable,
                null_count=null_count,
                null_percentage=null_percentage,
                numeric_stats=numeric_stats,
                string_stats=string_stats,
                date_stats=date_stats,
                unique_value_stats=unique_value_stats,
            )

        return columns_profile

    def _profile_numeric_column(
        self, dataframe: DataFrame, column_name: str
    ) -> NumericStatistics:
        """Profile a numeric column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the numeric column.

        Returns:
            NumericStatistics object with numeric statistics.
        """
        # Get basic statistics
        stats = dataframe.select(
            F.min(column_name).alias("min"),
            F.max(column_name).alias("max"),
            F.mean(column_name).alias("mean"),
            F.stddev_pop(column_name).alias("stddev"),
        ).first()

        percentiles: Dict[str, Optional[float]] = {}

        # Add percentiles for comprehensive/advanced
        if self._profile_type in ("comprehensive", "advanced"):
            percentile_values = [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
            exprs = [
                F.percentile_approx(column_name, p).alias(f"p{int(p*100)}")
                for p in percentile_values
            ]
            percentile_results = dataframe.select(exprs).first()

            for i, p in enumerate(percentile_values):
                if percentile_results:
                    percentiles[f"p{int(p*100)}"] = percentile_results[i]

        return NumericStatistics(
            min=stats["min"] if stats else None,
            max=stats["max"] if stats else None,
            mean=stats["mean"] if stats else None,
            stddev=stats["stddev"] if stats else None,
            percentiles=percentiles,
        )

    def _profile_string_column(
        self, dataframe: DataFrame, column_name: str
    ) -> StringStatistics:
        """Profile a string column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the string column.

        Returns:
            StringStatistics object with string statistics.
        """
        # Get length statistics
        length_stats = dataframe.select(
            F.min(F.length(column_name)).alias("min_length"),
            F.max(F.length(column_name)).alias("max_length"),
            F.mean(F.length(column_name)).alias("avg_length"),
        ).first()

        patterns: Dict[str, int] = {}

        # Add pattern detection for comprehensive/advanced
        if self._profile_type in ("comprehensive", "advanced"):
            patterns = self._detect_patterns(dataframe, column_name)

        return StringStatistics(
            min_length=length_stats["min_length"] if length_stats else None,
            max_length=length_stats["max_length"] if length_stats else None,
            avg_length=length_stats["avg_length"] if length_stats else None,
            patterns=patterns,
        )

    def _profile_date_column(
        self, dataframe: DataFrame, column_name: str
    ) -> DateStatistics:
        """Profile a date/timestamp column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the date column.

        Returns:
            DateStatistics object with date statistics.
        """
        # Get date range
        date_stats = dataframe.select(
            F.min(column_name).alias("min_date"),
            F.max(column_name).alias("max_date"),
        ).first()

        return DateStatistics(
            min_date=str(date_stats["min_date"]) if date_stats["min_date"] else None,
            max_date=str(date_stats["max_date"]) if date_stats["max_date"] else None,
        )

    def _profile_unique_values(
        self, dataframe: DataFrame, column_name: str
    ) -> UniqueValueStatistics:
        """Profile unique values for a column.

        Args:
            dataframe: DataFrame containing the column.
            column_name: Name of the column.

        Returns:
            UniqueValueStatistics object with unique value statistics.
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

        return UniqueValueStatistics(
            distinct_count=distinct_count,
            unique_percentage=(
                (distinct_count / total_count * 100) if total_count > 0 else 0
            ),
            sample_values=unique_values[: self._max_unique_values],
        )

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
    ) -> List[RuleSuggestion]:
        """Suggest validation rules based on profile data.

        Args:
            dataframe: The profiled DataFrame.
            profile: Profiling results.

        Returns:
            List of RuleSuggestion objects.
        """
        suggestions: List[RuleSuggestion] = []

        columns_profile = profile.get("columns", {})

        for column_name, column_profile in columns_profile.items():
            # Suggest completeness checks for columns with high null percentage
            null_pct = column_profile.null_percentage
            if null_pct > 5:
                suggestions.append(
                    RuleSuggestion(
                        column=column_name,
                        issue="high_null_percentage",
                        suggestion=f"Completeness check recommended ({null_pct:.1f}% null)",
                        constraint="Completeness",
                        severity="warning" if null_pct < 20 else "error",
                    )
                )

            # Suggest uniqueness checks for low distinct percentage
            if column_profile.unique_value_stats is not None:
                total_count = profile["general"].row_count
                distinct_count = column_profile.unique_value_stats.distinct_count
                uniqueness_pct = (
                    (distinct_count / total_count * 100) if total_count > 0 else 0
                )

                if uniqueness_pct < 90 and column_profile.data_type in (
                    "StringType",
                    "IntegerType",
                ):
                    suggestions.append(
                        RuleSuggestion(
                            column=column_name,
                            issue="low_uniqueness",
                            suggestion=f"Uniqueness check recommended ({uniqueness_pct:.1f}% unique)",
                            constraint="Uniqueness",
                            severity="info",
                        )
                    )

            # Suggest range checks for numeric columns with min/max
            if column_profile.numeric_stats is not None:
                min_val = column_profile.numeric_stats.min
                max_val = column_profile.numeric_stats.max
                if min_val is not None and max_val is not None:
                    suggestions.append(
                        RuleSuggestion(
                            column=column_name,
                            issue="numeric_range_identified",
                            suggestion=f"Range check suggested: [{min_val}, {max_val}]",
                            constraint="RangeCheck",
                            severity="info",
                        )
                    )

            # Suggest pattern checks for string columns with detected patterns
            if column_profile.string_stats is not None:
                patterns = column_profile.string_stats.patterns
                row_count = profile["general"].row_count
                if patterns.get("email", 0) > row_count * 0.5:
                    suggestions.append(
                        RuleSuggestion(
                            column=column_name,
                            issue="email_format_detected",
                            suggestion="Email format pattern check recommended",
                            constraint="PatternMatch",
                            severity="info",
                        )
                    )

                if patterns.get("url", 0) > row_count * 0.5:
                    suggestions.append(
                        RuleSuggestion(
                            column=column_name,
                            issue="url_format_detected",
                            suggestion="URL format pattern check recommended",
                            constraint="PatternMatch",
                            severity="info",
                        )
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
