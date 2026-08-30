# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""NullCheckRule — validates null/not-null constraints on columns."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List

from dq.engine.drules.rule_registry import DRule
from dq.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

_VALID_MODES = {"not_null", "null"}


class NullCheckRule(DRule):
    """Check that column values satisfy null/not-null constraints."""

    def validate_config(self, config: dict) -> None:
        if not config.get("column"):
            raise ConfigurationError("null_check rule requires 'column' parameter.")
        mode = config.get("mode", "not_null")
        if mode not in _VALID_MODES:
            raise ConfigurationError(
                f"null_check rule: invalid mode '{mode}'. "
                f"Valid modes: {sorted(_VALID_MODES)}"
            )

    def evaluate(self, dataframe: DataFrame, config: dict) -> List[Dict[str, Any]]:
        from pyspark.sql import functions as F

        column = config["column"]
        mode = config.get("mode", "not_null")
        constraint_name = config.get("constraint_name", f"null_check_{column}")

        total = dataframe.count()
        null_count = dataframe.filter(F.col(column).isNull()).count()

        if mode == "not_null":
            success = null_count == 0
            violations = null_count
        else:  # mode == "null"
            non_null_count = total - null_count
            success = non_null_count == 0
            violations = non_null_count

        return [
            {
                "check": constraint_name,
                "success": success,
                "details": {
                    "total_rows": total,
                    "violations": violations,
                    "column": column,
                    "mode": mode,
                },
            }
        ]
