# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""CrossColumnRule — validates relationships between two columns."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from dq.engine.drules.rule_registry import DRule
from dq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

_VALID_OPERATORS = {"<", "<=", ">", ">=", "==", "!="}


class CrossColumnRule(DRule):
    """Check that a relationship holds between two columns."""

    def validate_config(self, config: dict) -> None:
        if not config.get("left_column"):
            raise ConfigurationError(
                "cross_column rule requires 'left_column' parameter."
            )
        if not config.get("right_column"):
            raise ConfigurationError(
                "cross_column rule requires 'right_column' parameter."
            )
        operator = config.get("operator")
        if not operator:
            raise ConfigurationError("cross_column rule requires 'operator' parameter.")
        if operator not in _VALID_OPERATORS:
            raise ConfigurationError(
                f"cross_column rule: invalid operator '{operator}'. "
                f"Valid operators: {sorted(_VALID_OPERATORS)}"
            )

    def evaluate(self, dataframe: DataFrame, config: dict) -> List[Dict[str, Any]]:
        from pyspark.sql import functions as F

        left = config["left_column"]
        right = config["right_column"]
        operator = config["operator"]
        constraint_name = config.get(
            "constraint_name", f"cross_column_{left}_{operator}_{right}"
        )

        _operator_map = {
            "<": lambda l, r: l < r,
            "<=": lambda l, r: l <= r,
            ">": lambda l, r: l > r,
            ">=": lambda l, r: l >= r,
            "==": lambda l, r: l == r,
            "!=": lambda l, r: l != r,
        }

        total = dataframe.count()
        left_col = F.col(left)
        right_col = F.col(right)
        condition = _operator_map[operator](left_col, right_col)

        # Rows violating: either nulls in compared columns or condition not met
        violations = dataframe.filter(
            left_col.isNull() | right_col.isNull() | ~condition
        ).count()

        return [
            {
                "check": constraint_name,
                "success": violations == 0,
                "details": {
                    "total_rows": total,
                    "violations": violations,
                    "left_column": left,
                    "right_column": right,
                    "operator": operator,
                },
            }
        ]
