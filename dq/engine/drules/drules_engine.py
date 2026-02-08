# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Drules engine — declarative, rule-based data quality checks."""
from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

# Import rules package to trigger auto-registration BEFORE class definition
import dq.engine.drules.rules  # noqa: F401
from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError
from dq.utils import constants

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DrulesEngine(DQEngine):
    """Engine for declarative rule-based data quality checks.

    Supports rule types: column_threshold, null_check, regex,
    cross_column, custom_sql. Additional rules can be registered
    via ``RuleRegistry.register(name, cls)``.
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)

    def _validate_config(self) -> None:
        """Validate all check configs at init time (fail-fast)."""
        from dq.engine.drules.rule_registry import RuleRegistry

        checks = self._config.get("checks", [])
        for i, check in enumerate(checks):
            try:
                rule_type = check.get("rule_type", None)
            except Exception:
                rule_type = None

            if not rule_type:
                raise ConfigurationError(
                    f"Check at index {i} is missing required 'rule_type' key."
                )
            try:
                rule_class = RuleRegistry.get(rule_type)
            except KeyError as e:
                raise ConfigurationError(str(e))

            rule = rule_class()
            rule.validate_config(dict(check) if hasattr(check, "items") else check)

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply declarative rule checks to the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config (unused). Deprecated:
                Use ``repository_writer`` in constructor instead.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        if repository is not None:
            warnings.warn(
                "The 'repository' parameter is deprecated and unused. "
                "Use 'repository_writer' in the engine constructor instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        from dq.engine.drules.rule_registry import RuleRegistry

        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        checks = self._config.get("checks", [])
        all_metrics = []

        for check in checks:
            rule_type = check.get("rule_type", None)
            check_dict = dict(check) if hasattr(check, "items") else check

            try:
                rule_class = RuleRegistry.get(rule_type)
                rule = rule_class()
                raw_metrics = rule.evaluate(dataframe, check_dict)

                # Normalize each metric to DQMetric format
                for raw in raw_metrics:
                    check_name = raw.get("check", rule_type)
                    constraint = raw.get("constraint", rule_type)

                    metric = self._create_metric(
                        check=check_name,
                        success=raw.get("success", False),
                        details=raw.get("details", {}),
                        constraint=constraint,
                    )
                    all_metrics.append(metric.to_dict())

            except (ConfigurationError, KeyError):
                raise
            except Exception as e:
                constraint_name = check_dict.get("constraint_name", rule_type)
                logger.error("Rule '%s' failed: %s", constraint_name, e, exc_info=True)

                metric = self._create_metric(
                    check=constraint_name,
                    success=False,
                    details={"error": str(e)},
                    constraint=rule_type,
                )
                all_metrics.append(metric.to_dict())

        return all_metrics
