# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Drules engine — no Spark required."""
import pytest
from pyhocon import ConfigFactory

from dq.engine.drules.rule_registry import DRule, RuleRegistry
from dq.exceptions import ConfigurationError


class _DummyRule(DRule):
    def validate_config(self, config):
        pass

    def evaluate(self, dataframe, config):
        return [{"check": "dummy", "success": True, "details": {}}]


class TestRuleRegistry:
    def setup_method(self):
        """Clean registry before each test."""
        for name in list(RuleRegistry._rules.keys()):
            if name.startswith("_test"):
                RuleRegistry.unregister(name)

    def test_register_and_get(self):
        RuleRegistry.register("_test_dummy", _DummyRule)
        assert RuleRegistry.get("_test_dummy") is _DummyRule
        RuleRegistry.unregister("_test_dummy")

    def test_unknown_raises_key_error(self):
        with pytest.raises(KeyError, match="Unknown rule type"):
            RuleRegistry.get("_test_nonexistent")

    def test_list_rules(self):
        RuleRegistry.register("_test_a", _DummyRule)
        assert "_test_a" in RuleRegistry.list_rules()
        RuleRegistry.unregister("_test_a")

    def test_overwrite(self):
        RuleRegistry.register("_test_ow", _DummyRule)
        RuleRegistry.register("_test_ow", _DummyRule)
        assert RuleRegistry.get("_test_ow") is _DummyRule
        RuleRegistry.unregister("_test_ow")


class TestColumnThresholdRuleConfig:
    def _rule(self):
        from dq.engine.drules.rules.column_threshold_rule import ColumnThresholdRule

        return ColumnThresholdRule()

    def test_valid_min_max(self):
        self._rule().validate_config({"column": "age", "min": 0, "max": 120})

    def test_missing_column_raises(self):
        with pytest.raises(ConfigurationError, match="column"):
            self._rule().validate_config({"min": 0})

    def test_min_greater_than_max_raises(self):
        with pytest.raises(ConfigurationError, match="cannot be greater"):
            self._rule().validate_config({"column": "age", "min": 100, "max": 10})

    def test_no_threshold_raises(self):
        with pytest.raises(ConfigurationError, match="at least one"):
            self._rule().validate_config({"column": "age"})


class TestNullCheckRuleConfig:
    def _rule(self):
        from dq.engine.drules.rules.null_check_rule import NullCheckRule

        return NullCheckRule()

    def test_valid_not_null(self):
        self._rule().validate_config({"column": "name", "mode": "not_null"})

    def test_valid_null(self):
        self._rule().validate_config({"column": "name", "mode": "null"})

    def test_missing_column_raises(self):
        with pytest.raises(ConfigurationError, match="column"):
            self._rule().validate_config({"mode": "not_null"})

    def test_invalid_mode_raises(self):
        with pytest.raises(ConfigurationError, match="invalid mode"):
            self._rule().validate_config({"column": "name", "mode": "maybe"})


class TestRegexRuleConfig:
    def _rule(self):
        from dq.engine.drules.rules.regex_rule import RegexRule

        return RegexRule()

    def test_valid_config(self):
        self._rule().validate_config({"column": "email", "pattern": r"^[^@]+@[^@]+$"})

    def test_missing_pattern_raises(self):
        with pytest.raises(ConfigurationError, match="pattern"):
            self._rule().validate_config({"column": "email"})

    def test_invalid_regex_raises(self):
        with pytest.raises(ConfigurationError, match="invalid regex"):
            self._rule().validate_config({"column": "email", "pattern": "["})

    def test_missing_column_raises(self):
        with pytest.raises(ConfigurationError, match="column"):
            self._rule().validate_config({"pattern": ".*"})


class TestCrossColumnRuleConfig:
    def _rule(self):
        from dq.engine.drules.rules.cross_column_rule import CrossColumnRule

        return CrossColumnRule()

    def test_valid_config(self):
        self._rule().validate_config(
            {"left_column": "start", "right_column": "end", "operator": "<"}
        )

    def test_missing_operator_raises(self):
        with pytest.raises(ConfigurationError, match="operator"):
            self._rule().validate_config({"left_column": "a", "right_column": "b"})

    @pytest.mark.parametrize("op", ["between", "like", "in", "&&", "||"])
    def test_invalid_operator_raises(self, op):
        with pytest.raises(ConfigurationError, match="invalid operator"):
            self._rule().validate_config(
                {"left_column": "a", "right_column": "b", "operator": op}
            )

    @pytest.mark.parametrize("op", ["<", "<=", ">", ">=", "==", "!="])
    def test_valid_operators(self, op):
        self._rule().validate_config(
            {"left_column": "a", "right_column": "b", "operator": op}
        )


class TestCustomSQLRuleConfig:
    def _rule(self):
        from dq.engine.drules.rules.custom_sql_rule import CustomSQLRule

        return CustomSQLRule()

    def test_valid_config(self):
        self._rule().validate_config(
            {"sql_expression": "SELECT COUNT(*) FROM {table} WHERE age > 0"}
        )

    def test_missing_expression_raises(self):
        with pytest.raises(ConfigurationError, match="sql_expression"):
            self._rule().validate_config({})

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1; DROP TABLE users",
            "DELETE FROM {table}",
            "DROP TABLE {table}",
            "SELECT * FROM {table} -- comment",
            "INSERT INTO {table} VALUES (1)",
            "UPDATE {table} SET x=1",
        ],
    )
    def test_sql_injection_blocked(self, sql):
        with pytest.raises(ConfigurationError, match="prohibited pattern"):
            self._rule().validate_config({"sql_expression": sql})


class TestDrulesEngineConfig:
    def _make_config(self, config_str):
        return ConfigFactory.parse_string(config_str).get("drules", {})

    def test_empty_checks_ok(self):
        from dq.engine.drules.drules_engine import DrulesEngine

        config = self._make_config("drules { checks = [] }")
        engine = DrulesEngine(config)
        assert engine is not None

    def test_unknown_rule_type_raises(self):
        from dq.engine.drules.drules_engine import DrulesEngine

        config = self._make_config(
            """
            drules {
                checks = [
                    { rule_type = "nonexistent", column = "x" }
                ]
            }
        """
        )
        with pytest.raises(ConfigurationError, match="Unknown rule type"):
            DrulesEngine(config)

    def test_multi_rule_parses(self):
        from dq.engine.drules.drules_engine import DrulesEngine

        config = self._make_config(
            """
            drules {
                checks = [
                    { rule_type = "null_check", column = "name" }
                    { rule_type = "column_threshold", column = "age", min = 0 }
                ]
            }
        """
        )
        engine = DrulesEngine(config)
        assert engine is not None
