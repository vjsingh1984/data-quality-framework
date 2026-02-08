# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Built-in drules rules — auto-registered on import."""
from dq.engine.drules.rule_registry import RuleRegistry
from dq.engine.drules.rules.column_threshold_rule import ColumnThresholdRule
from dq.engine.drules.rules.cross_column_rule import CrossColumnRule
from dq.engine.drules.rules.custom_sql_rule import CustomSQLRule
from dq.engine.drules.rules.null_check_rule import NullCheckRule
from dq.engine.drules.rules.regex_rule import RegexRule

RuleRegistry.register("column_threshold", ColumnThresholdRule)
RuleRegistry.register("null_check", NullCheckRule)
RuleRegistry.register("regex", RegexRule)
RuleRegistry.register("cross_column", CrossColumnRule)
RuleRegistry.register("custom_sql", CustomSQLRule)
