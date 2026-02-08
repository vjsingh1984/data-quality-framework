# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""ColumnThresholdRule — validates column values fall within min/max/exact bounds."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from dq.engine.drules.rule_registry import DRule
from dq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ColumnThresholdRule(DRule):
    """Check that column values fall within specified thresholds."""

    def validate_config(self, config: dict) -> None:
        if not config.get("column"):
            raise ConfigurationError(
                "column_threshold rule requires 'column' parameter."
            )
        has_min = "min" in config
        has_max = "max" in config
        has_exact = "exact" in config
        if not (has_min or has_max or has_exact):
            raise ConfigurationError(
                "column_threshold rule requires at least one of "
                "'min', 'max', or 'exact'."
            )
        if has_min and has_max and config["min"] > config["max"]:
            raise ConfigurationError(
                f"column_threshold rule: min ({config['min']}) "
                f"cannot be greater than max ({config['max']})."
            )

    def evaluate(self, dataframe: DataFrame, config: dict) -> List[Dict[str, Any]]:
        from pyspark.sql import functions as F

        column = config["column"]
        constraint_name = config.get("constraint_name", f"column_threshold_{column}")

        total = dataframe.count()
        if total == 0:
            return [
                {
                    "check": constraint_name,
                    "success": True,
                    "details": {"total_rows": 0, "violations": 0},
                }
            ]

        col_ref = F.col(column)
        conditions = []

        if "exact" in config:
            conditions.append(col_ref != config["exact"])
        else:
            if "min" in config:
                conditions.append(col_ref < config["min"])
            if "max" in config:
                conditions.append(col_ref > config["max"])

        # Combine conditions with OR — any violation counts
        if conditions:
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c
            violations = dataframe.filter(combined | col_ref.isNull()).count()
        else:
            violations = 0

        return [
            {
                "check": constraint_name,
                "success": violations == 0,
                "details": {
                    "total_rows": total,
                    "violations": violations,
                    "column": column,
                },
            }
        ]
