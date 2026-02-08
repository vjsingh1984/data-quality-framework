# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Engine-agnostic schema validation using native Spark methods."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from dq.validation.result import SchemaValidationSummary, ValidationResult

logger = logging.getLogger(__name__)


class NativeSchemaValidator:
    """Schema validation using Spark native methods (no PyDeequ required).

    Validates dataframe schemas against expected constraints using Spark's
    built-in schema introspection and DataFrame operations. This provides
    an alternative to PyDeequ for environments where Deequ is not available.

    Supported constraints:
    - Datatype validation: Checks column data types match expected types
    - Nullable validation: Ensures columns are/not nullable as expected
    - Unique constraints: Validates columns contain unique values
    """

    def __init__(self, schema_config: Dict[str, Any], spark_session):
        """Initialize the validator.

        Args:
            schema_config: Schema configuration with tables and constraints.
            spark_session: Active SparkSession.
        """
        self._schema_config = schema_config
        self._spark_session = spark_session

    def validate(self, df: DataFrame) -> SchemaValidationSummary:
        """Validate the DataFrame against schema constraints.

        Args:
            df: DataFrame to validate.

        Returns:
            SchemaValidationSummary with all validation results.
        """
        summary = SchemaValidationSummary()
        table_schemas = self._schema_config.get("tables", [])

        if not table_schemas:
            logger.warning("No table schemas configured for validation")
            return summary

        # Assume first table schema applies to the DataFrame
        table_schema = table_schemas[0]

        # Validate datatypes
        self._validate_datatypes(df, table_schema, summary)

        # Validate nullable constraints
        self._validate_nullable(df, table_schema, summary)

        # Validate unique constraints
        self._validate_unique(df, table_schema, summary)

        # Note: Foreign key validation requires joins, kept separate for now
        # as it's more complex and may benefit from Deequ's optimization

        return summary

    def _validate_datatypes(
        self,
        df: DataFrame,
        table_schema: Dict[str, Any],
        summary: SchemaValidationSummary,
    ) -> None:
        """Validate column datatypes.

        Args:
            df: DataFrame to validate.
            table_schema: Table schema configuration.
            summary: Summary to add results to.
        """
        columns = table_schema.get("columns", [])
        df_schema = df.schema

        for col_config in columns:
            column_name = col_config.get("name")
            if not column_name:
                continue

            if column_name not in df_schema:
                summary.add_result(
                    ValidationResult(
                        check_name=f"DatatypeCheck.{column_name}",
                        constraint="Datatype",
                        success=False,
                        details={
                            "error": f"Column '{column_name}' not found in DataFrame"
                        },
                        assertion=f"Column '{column_name}' should exist",
                    )
                )
                continue

            actual_type = str(df_schema[column_name].dataType)
            expected_type = col_config.get("type")

            if expected_type:
                # Normalize type names for comparison
                normalized_actual = self._normalize_type(actual_type)
                normalized_expected = self._normalize_type(expected_type)

                success = normalized_actual == normalized_expected

                summary.add_result(
                    ValidationResult(
                        check_name=f"DatatypeCheck.{column_name}",
                        constraint="Datatype",
                        success=success,
                        details={
                            "column": column_name,
                            "expected_type": expected_type,
                            "actual_type": actual_type,
                        },
                        assertion=f"Column '{column_name}' should be {expected_type}",
                    )
                )

    def _validate_nullable(
        self,
        df: DataFrame,
        table_schema: Dict[str, Any],
        summary: SchemaValidationSummary,
    ) -> None:
        """Validate nullable constraints.

        Args:
            df: DataFrame to validate.
            table_schema: Table schema configuration.
            summary: Summary to add results to.
        """
        columns = table_schema.get("columns", [])

        for col_config in columns:
            column_name = col_config.get("name")
            if not column_name or column_name not in df.schema:
                continue

            nullable = col_config.get("nullable")
            if nullable is None:
                continue

            # Check schema first (faster)
            schema_nullable = df.schema[column_name].nullable

            if nullable and not schema_nullable:
                # Schema says not nullable but config expects nullable - OK
                continue
            elif not nullable and schema_nullable:
                # Schema allows nulls but config doesn't - need to check data
                null_count = (
                    df.select(column_name).filter(f"{column_name} IS NULL").count()
                )

                success = null_count == 0
                summary.add_result(
                    ValidationResult(
                        check_name=f"NullableCheck.{column_name}",
                        constraint="NotNull",
                        success=success,
                        details={
                            "column": column_name,
                            "null_count": null_count,
                            "total_rows": df.count(),
                        },
                        assertion=f"Column '{column_name}' should not contain null values",
                    )
                )

    def _validate_unique(
        self,
        df: DataFrame,
        table_schema: Dict[str, Any],
        summary: SchemaValidationSummary,
    ) -> None:
        """Validate unique constraints.

        Args:
            df: DataFrame to validate.
            table_schema: Table schema configuration.
            summary: Summary to add results to.
        """
        columns = table_schema.get("columns", [])

        for col_config in columns:
            column_name = col_config.get("name")
            if not column_name or column_name not in df.schema:
                continue

            unique = col_config.get("unique", False)
            if not unique:
                continue

            # Check uniqueness by comparing total count with distinct count
            total_count = df.count()
            distinct_count = df.select(column_name).distinct().count()

            success = total_count == distinct_count
            duplicate_count = total_count - distinct_count

            summary.add_result(
                ValidationResult(
                    check_name=f"UniqueCheck.{column_name}",
                    constraint="Unique",
                    success=success,
                    details={
                        "column": column_name,
                        "total_rows": total_count,
                        "distinct_rows": distinct_count,
                        "duplicate_rows": duplicate_count,
                    },
                    assertion=f"Column '{column_name}' should contain unique values",
                )
            )

    def _normalize_type(self, type_str: str) -> str:
        """Normalize type string for comparison.

        Args:
            type_str: Type string from schema.

        Returns:
            Normalized type string.
        """
        # Convert to string if it's a Spark type object
        type_str = str(type_str)

        # Extract base type (remove parameters like Decimal(10,2))
        base_type = type_str.split("(")[0]

        # Remove "Type" suffix for Spark types (e.g., "IntegerType" -> "Integer")
        if base_type.endswith("Type"):
            base_type = base_type[:-4].lower()
        else:
            base_type = base_type.lower()

        # Map common Spark types to normalized names
        type_mapping = {
            "integer": "int",
            "long": "bigint",
            "short": "smallint",
            "float": "float",
            "double": "double",
            "string": "string",
            "boolean": "boolean",
            "timestamp": "timestamp",
            "date": "date",
            "decimal": "decimal",
        }

        return type_mapping.get(base_type, base_type)
