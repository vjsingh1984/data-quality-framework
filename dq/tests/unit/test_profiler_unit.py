# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for ProfilerEngine."""
import json
from datetime import datetime

import pytest
from pyhocon import ConfigFactory

from dq.engine.profiler.profiler_engine import ProfilerEngine
from dq.engine.profiler.profiler_exporter import ProfileExporter
from dq.engine.profiler.profiler_results import (
    ColumnProfile,
    GeneralStatistics,
    NumericStatistics,
    ProfileResult,
    RuleSuggestion,
    StringStatistics,
)


class TestProfilerEngineConfigValidation:
    """Tests for ProfilerEngine configuration validation."""

    def test_profiler_engine_accepts_empty_config(self):
        """Test that ProfilerEngine accepts empty configuration."""
        config = ConfigFactory.parse_string("{}")
        engine = ProfilerEngine(config)
        assert engine is not None

    def test_profiler_engine_accepts_profile_type(self):
        """Test that ProfilerEngine accepts profile_type configuration."""
        config = ConfigFactory.parse_string(
            """
            {
                profile_type = "basic"
            }
            """
        )
        engine = ProfilerEngine(config)
        assert engine is not None

    def test_profiler_engine_accepts_max_unique_values(self):
        """Test that ProfilerEngine accepts max_unique_values configuration."""
        config = ConfigFactory.parse_string(
            """
            {
                max_unique_values = 50
            }
            """
        )
        engine = ProfilerEngine(config)
        assert engine is not None

    def test_profiler_engine_accepts_include_correlation(self):
        """Test that ProfilerEngine accepts include_correlation configuration."""
        config = ConfigFactory.parse_string(
            """
            {
                include_correlation = false
            }
            """
        )
        engine = ProfilerEngine(config)
        assert engine is not None


class TestProfileResultModels:
    """Tests for profiling result data models."""

    def test_general_statistics(self):
        """Test GeneralStatistics dataclass."""
        stats = GeneralStatistics(
            row_count=1000,
            column_count=5,
            columns=["id", "name", "age", "email", "created_at"],
            schema={"id": "IntegerType", "name": "StringType"},
            size_bytes=50000,
        )
        assert stats.row_count == 1000
        assert stats.column_count == 5
        assert len(stats.columns) == 5
        assert stats.size_bytes == 50000

    def test_numeric_statistics(self):
        """Test NumericStatistics dataclass."""
        stats = NumericStatistics(
            min=0.0,
            max=100.0,
            mean=50.0,
            stddev=30.0,
            percentiles={"p25": 25.0, "p50": 50.0, "p75": 75.0},
        )
        assert stats.min == 0.0
        assert stats.max == 100.0
        assert stats.mean == 50.0
        assert len(stats.percentiles) == 3

    def test_string_statistics(self):
        """Test StringStatistics dataclass."""
        stats = StringStatistics(
            min_length=1,
            max_length=100,
            avg_length=25.5,
            patterns={"email": 50, "url": 10},
        )
        assert stats.min_length == 1
        assert stats.max_length == 100
        assert stats.avg_length == 25.5
        assert len(stats.patterns) == 2

    def test_column_profile(self):
        """Test ColumnProfile dataclass."""
        profile = ColumnProfile(
            name="email",
            data_type="StringType",
            nullable=True,
            null_count=5,
            null_percentage=0.5,
            string_stats=StringStatistics(
                min_length=5, max_length=100, avg_length=25.0
            ),
        )
        assert profile.name == "email"
        assert profile.nullable is True
        assert profile.null_percentage == 0.5
        assert profile.string_stats is not None

    def test_rule_suggestion(self):
        """Test RuleSuggestion dataclass."""
        suggestion = RuleSuggestion(
            column="email",
            issue="high_null_percentage",
            suggestion="Completeness check recommended (5.0% null)",
            constraint="Completeness",
            severity="warning",
        )
        assert suggestion.column == "email"
        assert suggestion.severity == "warning"

    def test_profile_result_to_dict(self):
        """Test ProfileResult serialization to dict."""
        profile = ProfileResult(
            timestamp=datetime.now(),
            dataframe_name="test_table",
            profile_type="basic",
            general=GeneralStatistics(
                row_count=100,
                column_count=2,
                columns=["id", "name"],
                schema={"id": "IntegerType", "name": "StringType"},
                size_bytes=5000,
            ),
            columns={
                "id": ColumnProfile(
                    name="id",
                    data_type="IntegerType",
                    nullable=False,
                    null_count=0,
                    null_percentage=0.0,
                ),
                "name": ColumnProfile(
                    name="name",
                    data_type="StringType",
                    nullable=True,
                    null_count=5,
                    null_percentage=5.0,
                ),
            },
            suggestions=[
                RuleSuggestion(
                    column="name",
                    issue="high_null_percentage",
                    suggestion="Completeness check recommended",
                    constraint="Completeness",
                    severity="warning",
                )
            ],
        )

        result_dict = profile.to_dict()
        assert result_dict["dataframe_name"] == "test_table"
        assert result_dict["profile_type"] == "basic"
        assert result_dict["general"]["row_count"] == 100
        assert len(result_dict["columns"]) == 2
        assert len(result_dict["suggestions"]) == 1

    def test_profile_result_from_dict(self):
        """Test ProfileResult deserialization from dict."""
        profile_dict = {
            "timestamp": "2024-01-01T12:00:00",
            "dataframe_name": "test_table",
            "profile_type": "basic",
            "general": {
                "row_count": 100,
                "column_count": 1,
                "columns": ["id"],
                "schema": {"id": "IntegerType"},
                "size_bytes": 5000,
            },
            "columns": {
                "id": {
                    "name": "id",
                    "data_type": "IntegerType",
                    "nullable": False,
                    "null_count": 0,
                    "null_percentage": 0.0,
                }
            },
            "suggestions": [],
        }

        profile = ProfileResult.from_dict(profile_dict)
        assert profile.dataframe_name == "test_table"
        assert profile.general.row_count == 100
        assert len(profile.columns) == 1


class TestProfileExporter:
    """Tests for ProfileExporter functionality."""

    def test_exporter_to_json(self):
        """Test JSON export format."""
        profile = ProfileResult(
            timestamp=datetime.now(),
            dataframe_name="test_table",
            profile_type="basic",
            general=GeneralStatistics(
                row_count=100,
                column_count=1,
                columns=["id"],
                schema={"id": "IntegerType"},
                size_bytes=5000,
            ),
            columns={},
            suggestions=[],
        )

        exporter = ProfileExporter()
        json_str = exporter.to_json(profile)

        # Verify valid JSON
        data = json.loads(json_str)
        assert data["dataframe_name"] == "test_table"
        assert data["general"]["row_count"] == 100

    def test_exporter_to_markdown(self):
        """Test Markdown export format."""
        profile = ProfileResult(
            timestamp=datetime.now(),
            dataframe_name="test_table",
            profile_type="basic",
            general=GeneralStatistics(
                row_count=100,
                column_count=1,
                columns=["id"],
                schema={"id": "IntegerType"},
                size_bytes=5000,
            ),
            columns={},
            suggestions=[
                RuleSuggestion(
                    column="id",
                    issue="test_issue",
                    suggestion="Test suggestion",
                    constraint="TestConstraint",
                    severity="info",
                )
            ],
        )

        exporter = ProfileExporter()
        md_str = exporter.to_markdown(profile)

        # Verify markdown structure
        assert "# Data Profile: test_table" in md_str
        assert "## General Statistics" in md_str
        assert "- **Row Count:** 100" in md_str
        assert "## Rule Suggestions" in md_str

    def test_exporter_to_html(self):
        """Test HTML export format."""
        profile = ProfileResult(
            timestamp=datetime.now(),
            dataframe_name="test_table",
            profile_type="basic",
            general=GeneralStatistics(
                row_count=100,
                column_count=1,
                columns=["id"],
                schema={"id": "IntegerType"},
                size_bytes=5000,
            ),
            columns={},
            suggestions=[],
        )

        exporter = ProfileExporter()
        html_str = exporter.to_html(profile)

        # Verify HTML structure
        assert "<!DOCTYPE html>" in html_str
        assert "<title>Data Profile: test_table</title>" in html_str
        assert "Row Count" in html_str

    def test_exporter_format_bytes(self):
        """Test byte size formatting."""
        exporter = ProfileExporter()

        assert exporter._format_bytes(500) == "500.0 B"
        assert "KB" in exporter._format_bytes(2048)
        assert "MB" in exporter._format_bytes(2 * 1024 * 1024)
        assert "GB" in exporter._format_bytes(2 * 1024 * 1024 * 1024)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
