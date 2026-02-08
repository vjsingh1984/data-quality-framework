# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for native schema validation (D4)."""
from unittest.mock import MagicMock

from dq.validation.result import SchemaValidationSummary, ValidationResult
from dq.validation.schema_validator import NativeSchemaValidator


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_to_dict(self):
        """Test converting ValidationResult to dict."""
        result = ValidationResult(
            check_name="TestCheck",
            constraint="TestConstraint",
            success=True,
            details={"key": "value"},
            assertion="Test assertion",
        )

        expected = {
            "check": "TestCheck",
            "constraint": "TestConstraint",
            "success": True,
            "details": {"key": "value"},
            "assertion": "Test assertion",
        }

        assert result.to_dict() == expected


class TestSchemaValidationSummary:
    """Tests for SchemaValidationSummary."""

    def test_empty_summary(self):
        """Test empty summary properties."""
        summary = SchemaValidationSummary()

        assert summary.total_checks == 0
        assert summary.passed_checks == 0
        assert summary.failed_checks == 0

    def test_add_result(self):
        """Test adding results to summary."""
        summary = SchemaValidationSummary()

        result1 = ValidationResult("Check1", "Constraint1", True)
        result2 = ValidationResult("Check2", "Constraint2", False)

        summary.add_result(result1)
        summary.add_result(result2)

        assert summary.total_checks == 2
        assert summary.passed_checks == 1
        assert summary.failed_checks == 1

    def test_to_metric_dicts(self):
        """Test converting summary to metric dicts."""
        summary = SchemaValidationSummary()

        result = ValidationResult(
            check_name="DatatypeCheck.id",
            constraint="Datatype",
            success=True,
            details={"column": "id"},
        )
        summary.add_result(result)

        metrics = summary.to_metric_dicts(
            engine_name="schemavalidation",
            dataset="test_table",
            timestamp_ms=12345,
        )

        assert len(metrics) == 1
        assert metrics[0]["check"] == "DatatypeCheck.id"
        assert metrics[0]["constraint"] == "Datatype"
        assert metrics[0]["success"] is True
        assert metrics[0]["engine"] == "schemavalidation"
        assert metrics[0]["dataset"] == "test_table"
        assert metrics[0]["timestamp_ms"] == 12345


class TestNativeSchemaValidator:
    """Tests for NativeSchemaValidator."""

    def test_normalize_type(self):
        """Test type normalization."""
        mock_spark = MagicMock()
        validator = NativeSchemaValidator({}, mock_spark)

        # Test common type mappings (Spark types have "Type" suffix)
        assert validator._normalize_type("IntegerType") == "int"
        assert validator._normalize_type("LongType") == "bigint"
        assert validator._normalize_type("StringType") == "string"
        assert validator._normalize_type("BooleanType") == "boolean"

        # Test type with parameters (keeps "decimal" prefix)
        normalized = validator._normalize_type("Decimal(10,2)")
        assert normalized == "decimal"

    def test_requires_schema_config(self):
        """Test that validator handles empty schema config."""
        mock_spark = MagicMock()

        config = {"tables": []}

        validator = NativeSchemaValidator(config, mock_spark)
        summary = validator.validate(MagicMock())

        # Empty schema should return empty summary
        assert summary.total_checks == 0

    def test_validates_datatypes(self):
        """Test datatype validation."""
        mock_spark = MagicMock()

        # Create a mock DataFrame with schema
        mock_df = MagicMock()
        mock_df.schema = MagicMock()
        mock_df.schema.__contains__ = lambda self, col: col in ["id", "name"]
        mock_df.schema.__getitem__ = lambda self, col: MagicMock(
            dataType=MockMock(type_str="IntegerType" if col == "id" else "StringType")
        )
        mock_df.count.return_value = 0

        config = {
            "tables": [
                {
                    "name": "test_table",
                    "columns": [
                        {"name": "id", "type": "int"},
                        {"name": "name", "type": "string"},
                    ],
                }
            ]
        }

        validator = NativeSchemaValidator(config, mock_spark)
        summary = validator.validate(mock_df)

        # Should have 2 datatype checks
        datatype_checks = [r for r in summary.results if r.constraint == "Datatype"]
        assert len(datatype_checks) == 2

    def test_validates_nullable(self):
        """Test nullable constraint validation."""
        mock_spark = MagicMock()

        # Create a mock DataFrame
        mock_df = MagicMock()
        mock_df.schema = MagicMock()
        mock_df.schema.__contains__ = lambda self, col: col in ["id"]
        mock_df.schema.__getitem__ = lambda self, col: MagicMock(
            nullable=True, dataType=MockMock(type_str="IntegerType")
        )
        mock_df.select.return_value.filter.return_value.count.return_value = 0
        mock_df.count.return_value = 100

        config = {
            "tables": [
                {
                    "name": "test_table",
                    "columns": [{"name": "id", "type": "int", "nullable": False}],
                }
            ]
        }

        validator = NativeSchemaValidator(config, mock_spark)
        summary = validator.validate(mock_df)

        # Should have nullable check
        nullable_checks = [r for r in summary.results if r.constraint == "NotNull"]
        assert len(nullable_checks) >= 1

    def test_validates_unique(self):
        """Test unique constraint validation."""
        mock_spark = MagicMock()

        # Create a mock DataFrame
        mock_df = MagicMock()
        mock_df.schema = MagicMock()
        mock_df.schema.__contains__ = lambda self, col: col in ["id"]
        mock_df.schema.__getitem__ = lambda self, col: MagicMock(
            nullable=True, dataType=MockMock(type_str="IntegerType")
        )
        mock_df.count.return_value = 100
        mock_df.select.return_value.distinct.return_value.count.return_value = 100

        config = {
            "tables": [
                {"name": "test_table", "columns": [{"name": "id", "unique": True}]}
            ]
        }

        validator = NativeSchemaValidator(config, mock_spark)
        summary = validator.validate(mock_df)

        # Should have unique check
        unique_checks = [r for r in summary.results if r.constraint == "Unique"]
        assert len(unique_checks) >= 1


class MockMock:
    """Mock for dataType toString."""

    def __init__(self, type_str):
        self.type_str = type_str

    def __str__(self):
        return self.type_str
