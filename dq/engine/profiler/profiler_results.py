# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class GeneralStatistics:
    """General dataset statistics from profiling.

    Attributes:
        row_count: Total number of rows in the dataset.
        column_count: Total number of columns.
        columns: List of column names.
        schema: Mapping of column names to data types.
        size_bytes: Estimated size in bytes.
    """

    row_count: int
    column_count: int
    columns: List[str]
    schema: Dict[str, str]
    size_bytes: int


@dataclass
class NumericStatistics:
    """Statistics for numeric columns.

    Attributes:
        min: Minimum value.
        max: Maximum value.
        mean: Mean (average) value.
        stddev: Standard deviation.
        percentiles: Dictionary of percentile values (e.g., p25, p50, p75).
    """

    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    stddev: Optional[float]
    percentiles: Dict[str, Optional[float]] = field(default_factory=dict)


@dataclass
class StringStatistics:
    """Statistics for string columns.

    Attributes:
        min_length: Minimum string length.
        max_length: Maximum string length.
        avg_length: Average string length.
        patterns: Dictionary of pattern match counts (emails, URLs, etc.).
    """

    min_length: Optional[int]
    max_length: Optional[int]
    avg_length: Optional[float]
    patterns: Dict[str, int] = field(default_factory=dict)


@dataclass
class DateStatistics:
    """Statistics for date/timestamp columns.

    Attributes:
        min_date: Minimum date value as ISO string.
        max_date: Maximum date value as ISO string.
    """

    min_date: Optional[str]
    max_date: Optional[str]


@dataclass
class UniqueValueStatistics:
    """Statistics about unique values in a column.

    Attributes:
        distinct_count: Number of distinct values.
        unique_percentage: Percentage of unique values.
        sample_values: Sample of unique values (limited by max_unique_values).
    """

    distinct_count: int
    unique_percentage: float
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class ColumnProfile:
    """Complete profile for a single column.

    Attributes:
        name: Column name.
        data_type: Spark data type.
        nullable: Whether the column is nullable.
        null_count: Number of null values.
        null_percentage: Percentage of null values.
        numeric_stats: Numeric statistics (if applicable).
        string_stats: String statistics (if applicable).
        date_stats: Date statistics (if applicable).
        unique_value_stats: Unique value statistics (for comprehensive/advanced).
    """

    name: str
    data_type: str
    nullable: bool
    null_count: int
    null_percentage: float
    numeric_stats: Optional[NumericStatistics] = None
    string_stats: Optional[StringStatistics] = None
    date_stats: Optional[DateStatistics] = None
    unique_value_stats: Optional[UniqueValueStatistics] = None


@dataclass
class RuleSuggestion:
    """Suggested validation rule based on profiling.

    Attributes:
        column: Column name the suggestion applies to.
        issue: Type of issue detected.
        suggestion: Human-readable suggestion description.
        constraint: Recommended constraint type.
        severity: Severity level (info, warning, error).
    """

    column: str
    issue: str
    suggestion: str
    constraint: str
    severity: str


@dataclass
class ProfileResult:
    """Complete profiling result for a DataFrame.

    Attributes:
        timestamp: When the profile was generated.
        dataframe_name: Name of the DataFrame profiled.
        general: General dataset statistics.
        columns: Dictionary mapping column names to column profiles.
        correlations: Correlation matrix (if computed).
        suggestions: List of rule suggestions.
        profile_type: Type of profiling performed (basic, comprehensive, advanced).
    """

    timestamp: datetime
    dataframe_name: str
    general: GeneralStatistics
    columns: Dict[str, ColumnProfile]
    correlations: Optional[Dict[str, float]] = None
    suggestions: List[RuleSuggestion] = field(default_factory=list)
    profile_type: str = "basic"

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile result to dictionary format.

        Returns:
            Dictionary representation of the profile result.
        """
        result: Dict[str, Any] = {
            "timestamp": self.timestamp.isoformat(),
            "dataframe_name": self.dataframe_name,
            "profile_type": self.profile_type,
            "general": {
                "row_count": self.general.row_count,
                "column_count": self.general.column_count,
                "columns": self.general.columns,
                "schema": self.general.schema,
                "size_bytes": self.general.size_bytes,
            },
            "columns": {},
        }

        # Add column profiles
        for col_name, col_profile in self.columns.items():
            col_dict: Dict[str, Any] = {
                "data_type": col_profile.data_type,
                "nullable": col_profile.nullable,
                "null_count": col_profile.null_count,
                "null_percentage": col_profile.null_percentage,
            }

            if col_profile.numeric_stats:
                col_dict["numeric_stats"] = {
                    "min": col_profile.numeric_stats.min,
                    "max": col_profile.numeric_stats.max,
                    "mean": col_profile.numeric_stats.mean,
                    "stddev": col_profile.numeric_stats.stddev,
                    "percentiles": col_profile.numeric_stats.percentiles,
                }

            if col_profile.string_stats:
                col_dict["string_stats"] = {
                    "min_length": col_profile.string_stats.min_length,
                    "max_length": col_profile.string_stats.max_length,
                    "avg_length": col_profile.string_stats.avg_length,
                    "patterns": col_profile.string_stats.patterns,
                }

            if col_profile.date_stats:
                col_dict["date_stats"] = {
                    "min_date": col_profile.date_stats.min_date,
                    "max_date": col_profile.date_stats.max_date,
                }

            if col_profile.unique_value_stats:
                col_dict["unique_value_stats"] = {
                    "distinct_count": col_profile.unique_value_stats.distinct_count,
                    "unique_percentage": col_profile.unique_value_stats.unique_percentage,
                    "sample_values": col_profile.unique_value_stats.sample_values,
                }

            result["columns"][col_name] = col_dict

        # Add correlations if present
        if self.correlations:
            result["correlations"] = self.correlations

        # Add suggestions
        result["suggestions"] = [
            {
                "column": s.column,
                "issue": s.issue,
                "suggestion": s.suggestion,
                "constraint": s.constraint,
                "severity": s.severity,
            }
            for s in self.suggestions
        ]

        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProfileResult":
        """Create ProfileResult from dictionary.

        Args:
            data: Dictionary representation of profile result.

        Returns:
            ProfileResult instance.
        """
        general = GeneralStatistics(
            row_count=data["general"]["row_count"],
            column_count=data["general"]["column_count"],
            columns=data["general"]["columns"],
            schema=data["general"]["schema"],
            size_bytes=data["general"]["size_bytes"],
        )

        columns = {}
        for col_name, col_data in data.get("columns", {}).items():
            numeric_stats = None
            if "numeric_stats" in col_data:
                ns = col_data["numeric_stats"]
                numeric_stats = NumericStatistics(
                    min=ns["min"],
                    max=ns["max"],
                    mean=ns["mean"],
                    stddev=ns["stddev"],
                    percentiles=ns.get("percentiles", {}),
                )

            string_stats = None
            if "string_stats" in col_data:
                ss = col_data["string_stats"]
                string_stats = StringStatistics(
                    min_length=ss["min_length"],
                    max_length=ss["max_length"],
                    avg_length=ss["avg_length"],
                    patterns=ss.get("patterns", {}),
                )

            date_stats = None
            if "date_stats" in col_data:
                ds = col_data["date_stats"]
                date_stats = DateStatistics(
                    min_date=ds["min_date"],
                    max_date=ds["max_date"],
                )

            unique_value_stats = None
            if "unique_value_stats" in col_data:
                uvs = col_data["unique_value_stats"]
                unique_value_stats = UniqueValueStatistics(
                    distinct_count=uvs["distinct_count"],
                    unique_percentage=uvs["unique_percentage"],
                    sample_values=uvs.get("sample_values", []),
                )

            columns[col_name] = ColumnProfile(
                name=col_name,
                data_type=col_data["data_type"],
                nullable=col_data["nullable"],
                null_count=col_data["null_count"],
                null_percentage=col_data["null_percentage"],
                numeric_stats=numeric_stats,
                string_stats=string_stats,
                date_stats=date_stats,
                unique_value_stats=unique_value_stats,
            )

        suggestions = [
            RuleSuggestion(
                column=s["column"],
                issue=s["issue"],
                suggestion=s["suggestion"],
                constraint=s["constraint"],
                severity=s["severity"],
            )
            for s in data.get("suggestions", [])
        ]

        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            dataframe_name=data["dataframe_name"],
            profile_type=data.get("profile_type", "basic"),
            general=general,
            columns=columns,
            correlations=data.get("correlations"),
            suggestions=suggestions,
        )


@dataclass
class ProfileMetrics:
    """Metrics format returned by ProfilerEngine.apply().

    This wraps ProfileResult to match the standard engine output format.

    Attributes:
        check: Check name (e.g., "Profile.general", "Profile.columns.id").
        success: Always True for profiling.
        details: Contains profile_result key with ProfileResult data.
        constraint: Constraint type ("Profile").
        dataframe_name: Name of the DataFrame.
        timestamp: When the profile was generated.
    """

    check: str
    success: bool
    details: Dict[str, Any]
    constraint: str
    dataframe_name: str
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to standard metric dictionary format.

        Returns:
            Dictionary with check, success, details, constraint, and timestamp.
        """
        return {
            "check": self.check,
            "success": self.success,
            "details": self.details,
            "constraint": self.constraint,
            "dataframe_name": self.dataframe_name,
            "timestamp": self.timestamp.isoformat(),
        }
