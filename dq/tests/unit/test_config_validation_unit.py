# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for engine config validation (D6)."""

import pytest
from pyhocon import ConfigFactory

from dq.engine.custom.custom_engine import ConstraintEngine
from dq.engine.deequ.deequ_engine import DeequEngine
from dq.engine.drules.drules_engine import DrulesEngine
from dq.engine.greatexpectations.greatexpectations_engine import (
    GreatExpectationsEngine,
)
from dq.engine.schemavalidation.schemavalidation_engine import (
    SchemaValidationEngine,
)
from dq.exceptions import ConfigurationError


class TestDeequEngineValidation:
    """Tests for DeequEngine config validation."""

    def test_requires_checks_key(self):
        """Test that DeequEngine requires 'checks' in config."""
        config = ConfigFactory.parse_string("{}")
        with pytest.raises(ConfigurationError, match="requires 'checks'"):
            DeequEngine(config)

    def test_checks_must_not_be_empty(self):
        """Test that 'checks' must contain at least one check."""
        config = ConfigFactory.parse_string("{ checks: [] }")
        with pytest.raises(ConfigurationError, match="requires 'checks'"):
            DeequEngine(config)

    def test_each_check_must_have_constraint(self):
        """Test that each check must have a 'constraint' key."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "Completeness", column = "test_col" },
                    { column = "test_col2" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="missing required 'constraint'"):
            DeequEngine(config)

    def test_valid_config_passes(self):
        """Test that valid config passes validation."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "Completeness", column = "test_col" }
                ]
            }
            """
        )
        # Should not raise
        engine = DeequEngine(config)
        assert engine is not None


class TestConstraintEngineValidation:
    """Tests for ConstraintEngine config validation."""

    def test_requires_checks_key(self):
        """Test that ConstraintEngine requires 'checks' in config."""
        config = ConfigFactory.parse_string("{}")
        with pytest.raises(ConfigurationError, match="requires 'checks'"):
            ConstraintEngine(config)

    def test_checks_must_not_be_empty(self):
        """Test that 'checks' must contain at least one check."""
        config = ConfigFactory.parse_string("{ checks: [] }")
        with pytest.raises(ConfigurationError, match="requires 'checks'"):
            ConstraintEngine(config)

    def test_each_check_must_have_constraint(self):
        """Test that each check must have a 'constraint' key."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "RateOfChange" },
                    { column = "test_col2" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="missing required 'constraint'"):
            ConstraintEngine(config)

    def test_constraint_must_be_registered(self):
        """Test that unknown constraints raise an error."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "UnknownConstraint" }
                ]
            }
            """
        )
        with pytest.raises(
            ConfigurationError, match="Unknown constraint 'UnknownConstraint'"
        ):
            ConstraintEngine(config)

    def test_known_constraint_passes(self):
        """Test that known constraint passes validation."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "RateOfChange" }
                ]
            }
            """
        )
        # Should not raise
        engine = ConstraintEngine(config)
        assert engine is not None


class TestSchemaValidationEngineValidation:
    """Tests for SchemaValidationEngine config validation."""

    def test_requires_schema_key(self):
        """Test that SchemaValidationEngine requires 'schema' in config."""
        config = ConfigFactory.parse_string("{}")
        with pytest.raises(ConfigurationError, match="requires 'schema'"):
            SchemaValidationEngine(config)

    def test_valid_config_passes(self):
        """Test that valid config passes validation."""
        config = ConfigFactory.parse_string(
            """
            {
                schema = {
                    tables = [{
                        name = "test_table"
                        columns = []
                    }]
                }
            }
            """
        )
        # Should not raise
        engine = SchemaValidationEngine(config)
        assert engine is not None


class TestDrulesEngineValidation:
    """Tests for DrulesEngine config validation."""

    def test_checks_must_have_rule_type(self):
        """Test that each check must have 'rule_type' key."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { column = "test_col2" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="missing required 'rule_type'"):
            DrulesEngine(config)

    def test_unknown_rule_type_fails(self):
        """Test that unknown rule types raise an error."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { rule_type = "UnknownRule" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError):
            DrulesEngine(config)

    def test_valid_config_passes(self):
        """Test that valid config passes validation."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { rule_type = "null_check", column = "test_col" }
                ]
            }
            """
        )
        # Should not raise
        engine = DrulesEngine(config)
        assert engine is not None


class TestGreatExpectationsEngineValidation:
    """Tests for GreatExpectationsEngine config validation."""

    def test_requires_expectations_or_checks(self):
        """Test that engine requires 'expectations' or 'checks' key."""
        config = ConfigFactory.parse_string("{}")
        with pytest.raises(
            ConfigurationError, match="requires 'expectations' \\(or 'checks'\\)"
        ):
            GreatExpectationsEngine(config)

    def test_expectations_must_have_type(self):
        """Test that each expectation must have 'type' key."""
        config = ConfigFactory.parse_string(
            """
            {
                expectations = [
                    { type = "expect_column_values_to_not_be_null", column = "test_col" },
                    { column = "test_col2" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="missing required 'type'"):
            GreatExpectationsEngine(config)

    def test_unsupported_expectation_type_fails(self):
        """Test that unsupported expectation types raise an error."""
        config = ConfigFactory.parse_string(
            """
            {
                expectations = [
                    { type = "expect_unsupported_type", column = "test_col" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="Unsupported expectation type"):
            GreatExpectationsEngine(config)

    def test_column_level_expectation_requires_column(self):
        """Test that column-level expectations require 'column' parameter."""
        config = ConfigFactory.parse_string(
            """
            {
                expectations = [
                    { type = "expect_column_values_to_not_be_null" }
                ]
            }
            """
        )
        with pytest.raises(ConfigurationError, match="requires a 'column' parameter"):
            GreatExpectationsEngine(config)

    def test_checks_alias_works(self):
        """Test that 'checks' is accepted as alias for 'expectations'."""
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { type = "expect_column_to_exist", column = "test_col" }
                ]
            }
            """
        )
        # Should not raise
        engine = GreatExpectationsEngine(config)
        assert engine is not None

    def test_valid_config_passes(self):
        """Test that valid config passes validation."""
        config = ConfigFactory.parse_string(
            """
            {
                expectations = [
                    { type = "expect_column_to_exist", column = "test_col" }
                ]
            }
            """
        )
        # Should not raise
        engine = GreatExpectationsEngine(config)
        assert engine is not None


class TestBaseEngineValidation:
    """Tests for base DQEngine validation behavior."""

    def test_validate_config_called_at_init(self):
        """Test that _validate_config is called during engine initialization."""
        from dq.engine.dq_engine import DQEngine

        class ValidationTestEngine(DQEngine):
            def __init__(self, config, dqts=None):
                self.validate_called = False
                super().__init__(config, dqts)

            def _validate_config(self):
                self.validate_called = True

            def apply(self, dataframe, repository=None):
                return []

        config = ConfigFactory.parse_string("{}")
        engine = ValidationTestEngine(config)
        assert engine.validate_called

    def test_default_validate_config_does_nothing(self):
        """Test that default _validate_config implementation is safe."""
        from dq.engine.dq_engine import DQEngine

        class NoOpEngine(DQEngine):
            def apply(self, dataframe, repository=None):
                return []

        config = ConfigFactory.parse_string("{}")
        # Should not raise
        engine = NoOpEngine(config)
        assert engine is not None
