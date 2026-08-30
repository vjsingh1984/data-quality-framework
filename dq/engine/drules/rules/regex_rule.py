# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""RegexRule — validates column values match a regex pattern."""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List

from dq.engine.drules.rule_registry import DRule
from dq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class RegexRule(DRule):
    """Check that column values match a given regex pattern."""

    def validate_config(self, config: dict) -> None:
        if not config.get("column"):
            raise ConfigurationError("regex rule requires 'column' parameter.")
        pattern = config.get("pattern")
        if not pattern:
            raise ConfigurationError("regex rule requires 'pattern' parameter.")
        try:
            re.compile(pattern)
        except re.error as e:
            raise ConfigurationError(
                f"regex rule: invalid regex pattern '{pattern}': {e}"
            )

    def evaluate(self, dataframe: DataFrame, config: dict) -> List[Dict[str, Any]]:
        from pyspark.sql import functions as F

        column = config["column"]
        pattern = config["pattern"]
        constraint_name = config.get("constraint_name", f"regex_{column}")

        total = dataframe.count()
        # Count non-null rows that do NOT match the pattern
        non_matching = dataframe.filter(
            F.col(column).isNotNull() & ~F.col(column).rlike(pattern)
        ).count()

        return [
            {
                "check": constraint_name,
                "success": non_matching == 0,
                "details": {
                    "total_rows": total,
                    "violations": non_matching,
                    "column": column,
                    "pattern": pattern,
                },
            }
        ]
