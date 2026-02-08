# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""CustomSQLRule — validates data using a custom SQL expression."""
from __future__ import annotations

import logging
import re
import uuid
from typing import TYPE_CHECKING, Any, Dict, List

from dq.engine.drules.rule_registry import DRule
from dq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Patterns that indicate potentially dangerous SQL
_DANGEROUS_PATTERNS = [
    re.compile(r"\bDROP\b", re.IGNORECASE),
    re.compile(r"\bDELETE\b", re.IGNORECASE),
    re.compile(r"\bINSERT\b", re.IGNORECASE),
    re.compile(r"\bUPDATE\b", re.IGNORECASE),
    re.compile(r"\bALTER\b", re.IGNORECASE),
    re.compile(r"\bTRUNCATE\b", re.IGNORECASE),
    re.compile(r"\bCREATE\b", re.IGNORECASE),
    re.compile(r";"),
    re.compile(r"--"),
]

_VALID_OPERATORS = {"<", "<=", ">", ">=", "==", "!="}


class CustomSQLRule(DRule):
    """Evaluate a custom SQL expression against the DataFrame."""

    def validate_config(self, config: dict) -> None:
        sql_expr = config.get("sql_expression")
        if not sql_expr:
            raise ConfigurationError(
                "custom_sql rule requires 'sql_expression' parameter."
            )

        for pattern in _DANGEROUS_PATTERNS:
            if pattern.search(sql_expr):
                raise ConfigurationError(
                    f"custom_sql rule: SQL expression contains prohibited pattern "
                    f"'{pattern.pattern}'. Only SELECT queries are allowed."
                )

        expected_op = config.get("expected_operator", "==")
        if expected_op not in _VALID_OPERATORS:
            raise ConfigurationError(
                f"custom_sql rule: invalid expected_operator '{expected_op}'. "
                f"Valid: {sorted(_VALID_OPERATORS)}"
            )

    def evaluate(self, dataframe: DataFrame, config: dict) -> List[Dict[str, Any]]:
        sql_expression = config["sql_expression"]
        expected_operator = config.get("expected_operator", "==")
        expected_value = config.get("expected_value", 0)
        constraint_name = config.get("constraint_name", "custom_sql_check")

        spark = dataframe.sparkSession
        # Use uuid-based temp view name to avoid collisions
        view_name = f"_dq_drules_{uuid.uuid4().hex[:12]}"

        try:
            dataframe.createOrReplaceTempView(view_name)
            # Replace {table} placeholder with the temp view name
            resolved_sql = sql_expression.replace("{table}", view_name)
            result_dataframe = spark.sql(resolved_sql)
            rows = result_dataframe.collect()
            if not rows or rows[0][0] is None:
                result_value = None
            else:
                result_value = rows[0][0]

            operator_map = {
                "<": lambda a, b: a < b,
                "<=": lambda a, b: a <= b,
                ">": lambda a, b: a > b,
                ">=": lambda a, b: a >= b,
                "==": lambda a, b: a == b,
                "!=": lambda a, b: a != b,
            }
            success = operator_map[expected_operator](result_value, expected_value)

            return [
                {
                    "check": constraint_name,
                    "success": success,
                    "details": {
                        "sql_expression": sql_expression,
                        "result_value": result_value,
                        "expected_operator": expected_operator,
                        "expected_value": expected_value,
                    },
                }
            ]
        finally:
            try:
                spark.catalog.dropTempView(view_name)
            except Exception:
                pass
