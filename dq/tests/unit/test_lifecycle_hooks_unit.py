# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DQEngine lifecycle hooks (D7 fix)."""
from unittest.mock import MagicMock

import pytest

from dq.engine.custom.custom_engine import CustomEngine
from dq.engine.dq_engine import DQEngine


class DummyEngine(DQEngine):
    """Dummy engine for testing lifecycle hooks."""

    def __init__(self, config, dqts=None):
        super().__init__(config, dqts)
        self.before_called = False
        self.after_called = False
        self.before_df = None
        self.after_df = None
        self.after_metrics = None

    def before_apply(self, dataframe):
        self.before_called = True
        self.before_df = dataframe

    def after_apply(self, dataframe, metrics):
        self.after_called = True
        self.after_df = dataframe
        self.after_metrics = metrics

    def apply(self, dataframe, repository=None):
        # Call the hooks
        self.before_apply(dataframe)
        try:
            return [{"check": "test", "success": True, "details": {}}]
        finally:
            self.after_apply(
                dataframe, [{"check": "test", "success": True, "details": {}}]
            )


class TestLifecycleHooks:
    """Tests for DQEngine lifecycle hooks."""

    def test_base_engine_has_cache(self):
        """Test that DQEngine initializes with cache dict."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        engine = DummyEngine(config)

        assert hasattr(engine, "_cache")
        assert isinstance(engine._cache, dict)
        assert engine._cache == {}

    def test_base_engine_has_hook_methods(self):
        """Test that base DQEngine has hook methods."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        engine = DummyEngine(config)

        assert hasattr(engine, "before_apply")
        assert hasattr(engine, "after_apply")
        assert callable(engine.before_apply)
        assert callable(engine.after_apply)

    def test_before_hook_called(self):
        """Test that before_apply is called during apply."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        engine = DummyEngine(config)

        # Create mock dataframe
        mock_df = MagicMock()

        # Call apply
        engine.apply(mock_df)

        # Verify before_apply was called
        assert engine.before_called
        assert engine.before_df is mock_df

    def test_after_hook_called(self):
        """Test that after_apply is called with correct arguments."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        engine = DummyEngine(config)

        # Create mock dataframe
        mock_df = MagicMock()

        # Call apply
        metrics = engine.apply(mock_df)

        # Verify after_apply was called
        assert engine.after_called
        assert engine.after_df is mock_df
        assert engine.after_metrics == metrics

    def test_after_hook_called_on_exception(self):
        """Test that after_apply is called even when apply raises an exception."""
        from pyhocon import ConfigFactory

        class FailingEngine(DQEngine):
            def __init__(self, config, dqts=None):
                super().__init__(config, dqts)
                self.after_called = False

            def before_apply(self, dataframe):
                pass

            def after_apply(self, dataframe, metrics):
                self.after_called = True

            def apply(self, dataframe, repository=None):
                self.before_apply(dataframe)
                try:
                    raise ValueError("Intentional failure")
                finally:
                    self.after_apply(dataframe, [])

        config = ConfigFactory.parse_string("{}")
        engine = FailingEngine(config)
        mock_df = MagicMock()

        # Call apply - should raise exception but after_apply still called
        with pytest.raises(ValueError, match="Intentional failure"):
            engine.apply(mock_df)

        # Verify after_apply was still called
        assert engine.after_called

    def test_hook_default_implementation_does_nothing(self):
        """Test that default hook implementations do nothing (pass)."""
        # Use a concrete engine (DummyEngine) since DQEngine is abstract
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string("{}")
        engine = DummyEngine(config)

        # Default parent implementation should do nothing
        # Call parent hooks directly (not the overridden ones)
        mock_df = MagicMock()
        DQEngine.before_apply(engine, mock_df)  # Should not raise
        DQEngine.after_apply(engine, mock_df, [])  # Should not raise


class TestCustomEngineLifecycle:
    """Tests for CustomEngine lifecycle hooks implementation."""

    def test_custom_engine_caches_reference_dataframes(self):
        """Test that CustomEngine caches reference DataFrames in before_apply."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    {
                        constraint = "LookupBasedOnColumnNameList"
                        ref_table = "test_ref_table"
                    }
                ]
            }
            """
        )

        # Mock spark session and dataframe
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.sparkSession = mock_spark

        # Mock the table() method to return a mock DataFrame
        mock_ref_df = MagicMock()
        mock_ref_df.cache.return_value = mock_ref_df
        mock_spark.table.return_value = mock_ref_df

        # Create engine and call before_apply
        engine = CustomEngine(config)
        engine.before_apply(mock_df)

        # Verify reference table was cached
        mock_spark.table.assert_called_once_with("test_ref_table")
        mock_ref_df.cache.assert_called_once()
        assert "ref_test_ref_table" in engine._cache

    def test_custom_engine_cleanup_in_after_apply(self):
        """Test that CustomEngine unpersists cached DataFrames in after_apply."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "RateOfChange" }
                ]
            }
            """
        )

        # Create engine with cached data
        engine = CustomEngine(config)
        mock_df = MagicMock()

        # Add a mock DataFrame to cache that has unpersist method
        mock_cached_df = MagicMock()
        mock_cached_df.unpersist.return_value = None
        engine._cache["test_key"] = mock_cached_df

        # Call after_apply
        engine.after_apply(mock_df, [])

        # Verify unpersist was called
        mock_cached_df.unpersist.assert_called_once()

        # Verify cache was cleared
        assert engine._cache == {}

    def test_custom_engine_handles_missing_unpersist(self):
        """Test that CustomEngine handles cached objects without unpersist method."""
        from pyhocon import ConfigFactory

        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    { constraint = "RateOfChange" }
                ]
            }
            """
        )

        # Create engine
        engine = CustomEngine(config)
        mock_df = MagicMock()

        # Add a non-DataFrame object to cache (no unpersist method)
        engine._cache["string_key"] = "some_string"

        # Call after_apply - should not raise
        engine.after_apply(mock_df, [])

        # Verify cache was still cleared
        assert "string_key" not in engine._cache
