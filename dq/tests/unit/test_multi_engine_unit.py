# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for multi-engine execution framework."""
import time

import pytest

from dq.engine.multi_engine.execution_strategy import (
    BatchedExecutionStrategy,
    EngineResult,
    EngineTask,
    ExecutionStrategy,
    ParallelExecutionStrategy,
    SequentialExecutionStrategy,
)
from dq.engine.multi_engine.multi_engine_orchestrator import (
    EngineExecutionConfig,
    MultiEngineOrchestrator,
)


class MockEngine:
    """Mock DQ engine for testing."""

    def __init__(self, name: str, should_fail: bool = False, delay_ms: int = 0):
        self.name = name
        self._should_fail = should_fail
        self._delay_ms = delay_ms

    def apply(self, dataframe, repository=None):
        if self._delay_ms > 0:
            time.sleep(self._delay_ms / 1000.0)

        if self._should_fail:
            raise RuntimeError(f"Engine {self.name} failed")

        return [
            {
                "check": f"{self.name}_check",
                "success": True,
                "details": {"engine": self.name},
            }
        ]


class MockDataFrame:
    """Mock DataFrame for testing."""

    def __init__(self, name: str):
        self.name = name


class TestExecutionStrategy:
    """Tests for execution strategies."""

    def test_sequential_execution(self):
        """Test sequential execution strategy."""
        strategy = SequentialExecutionStrategy()

        df = MockDataFrame("test_df")
        tasks = [
            EngineTask(
                engine=MockEngine("engine1"),
                dataframe=df,
                dataframe_name="test_df",
            ),
            EngineTask(
                engine=MockEngine("engine2"),
                dataframe=df,
                dataframe_name="test_df",
            ),
        ]

        results = strategy.execute(tasks)

        assert len(results) == 2
        assert all(r.success for r in results)
        # MockEngine -> "Mock" after removing "Engine" suffix
        assert results[0].engine_name == "mock"

    def test_parallel_execution(self):
        """Test parallel execution strategy."""
        strategy = ParallelExecutionStrategy(max_workers=2)

        df = MockDataFrame("test_df")
        tasks = [
            EngineTask(
                engine=MockEngine("engine1", delay_ms=50),
                dataframe=df,
                dataframe_name="test_df",
            ),
            EngineTask(
                engine=MockEngine("engine2", delay_ms=50),
                dataframe=df,
                dataframe_name="test_df",
            ),
        ]

        results = strategy.execute(tasks)

        assert len(results) == 2
        assert all(r.success for r in results)

    def test_batched_execution(self):
        """Test batched execution strategy."""
        strategy = BatchedExecutionStrategy()

        df1 = MockDataFrame("df1")
        df2 = MockDataFrame("df2")
        tasks = [
            EngineTask(
                engine=MockEngine("engine1"),
                dataframe=df1,
                dataframe_name="df1",
            ),
            EngineTask(
                engine=MockEngine("engine2"),
                dataframe=df1,
                dataframe_name="df1",
            ),
            EngineTask(
                engine=MockEngine("engine3"),
                dataframe=df2,
                dataframe_name="df2",
            ),
        ]

        results = strategy.execute(tasks)

        assert len(results) == 3
        assert all(r.success for r in results)

    def test_engine_failure_handling(self):
        """Test that engine failures are handled gracefully."""
        strategy = SequentialExecutionStrategy()

        df = MockDataFrame("test_df")
        tasks = [
            EngineTask(
                engine=MockEngine("engine1"),
                dataframe=df,
                dataframe_name="test_df",
            ),
            EngineTask(
                engine=MockEngine("engine2", should_fail=True),
                dataframe=df,
                dataframe_name="test_df",
            ),
        ]

        results = strategy.execute(tasks)

        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False
        assert isinstance(results[1].error, RuntimeError)


class TestMultiEngineOrchestrator:
    """Tests for MultiEngineOrchestrator."""

    def test_add_engine_and_dataframe(self):
        """Test adding engines and dataframes."""
        orchestrator = MultiEngineOrchestrator()

        engine = MockEngine("test_engine")
        df = MockDataFrame("test_df")

        orchestrator.add_engine(engine, ["test_df"])
        orchestrator.add_dataframe("test_df", df)

        # Should not raise
        result = orchestrator.run()
        assert result.total_engines == 1
        assert result.successful_engines == 1

    def test_sequential_execution(self):
        """Test sequential execution of multiple engines."""
        orchestrator = MultiEngineOrchestrator(strategy=ExecutionStrategy.SEQUENTIAL)

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2"), ["test_df"])

        result = orchestrator.run()

        assert result.total_engines == 2
        assert result.successful_engines == 2
        assert len(result.results) == 2

    def test_parallel_execution(self):
        """Test parallel execution of multiple engines."""
        orchestrator = MultiEngineOrchestrator(
            strategy=ExecutionStrategy.PARALLEL,
            max_workers=2,
        )

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1", delay_ms=50), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2", delay_ms=50), ["test_df"])

        result = orchestrator.run()

        assert result.total_engines == 2
        assert result.successful_engines == 2

    def test_batched_execution(self):
        """Test batched execution grouped by DataFrame."""
        orchestrator = MultiEngineOrchestrator(strategy=ExecutionStrategy.BATCHED)

        df1 = MockDataFrame("df1")
        df2 = MockDataFrame("df2")

        orchestrator.add_dataframe("df1", df1)
        orchestrator.add_dataframe("df2", df2)

        orchestrator.add_engine(MockEngine("engine1"), ["df1", "df2"])
        orchestrator.add_engine(MockEngine("engine2"), ["df1", "df2"])

        result = orchestrator.run()

        assert result.total_engines == 4
        assert result.successful_engines == 4

    def test_get_results_by_engine(self):
        """Test filtering results by engine."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2"), ["test_df"])

        result = orchestrator.run()

        # Results are grouped by engine name (MockEngine -> "mock")
        engine_results = result.get_results_by_engine("mock")
        assert len(engine_results) == 2

    def test_get_results_by_dataframe(self):
        """Test filtering results by DataFrame."""
        orchestrator = MultiEngineOrchestrator()

        df1 = MockDataFrame("df1")
        df2 = MockDataFrame("df2")

        orchestrator.add_dataframe("df1", df1)
        orchestrator.add_dataframe("df2", df2)

        orchestrator.add_engine(MockEngine("engine1"), ["df1", "df2"])

        result = orchestrator.run()

        df1_results = result.get_results_by_dataframe("df1")
        df2_results = result.get_results_by_dataframe("df2")

        assert len(df1_results) == 1
        assert len(df2_results) == 1

    def test_get_failed_results(self):
        """Test getting failed engine results."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2", should_fail=True), ["test_df"])

        result = orchestrator.run()

        failed_results = result.get_failed_results()
        assert len(failed_results) == 1
        assert failed_results[0].success is False

    def test_get_metrics(self):
        """Test getting all metrics from all engines."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2"), ["test_df"])

        result = orchestrator.run()

        all_metrics = result.get_metrics()
        assert len(all_metrics) == 2  # One metric per engine

    def test_get_summary(self):
        """Test getting execution summary."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2", should_fail=True), ["test_df"])

        result = orchestrator.run()

        summary = result.get_summary()
        assert summary["total_engines"] == 2
        assert summary["successful_engines"] == 1
        assert summary["failed_engines"] == 1
        assert "total_execution_time_ms" in summary

    def test_fail_fast_mode(self):
        """Test fail_fast mode stops on first error."""
        orchestrator = MultiEngineOrchestrator(fail_fast=True)

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])
        orchestrator.add_engine(MockEngine("engine2", should_fail=True), ["test_df"])
        orchestrator.add_engine(MockEngine("engine3"), ["test_df"])

        with pytest.raises(RuntimeError, match="Engine execution failed"):
            orchestrator.run()

    def test_missing_dataframe_skipped(self):
        """Test that missing dataframes are skipped gracefully."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df", "missing_df"])

        result = orchestrator.run()

        # Should only execute for the existing dataframe
        assert result.total_engines == 1

    def test_clear(self):
        """Test clearing engines and dataframes."""
        orchestrator = MultiEngineOrchestrator()

        df = MockDataFrame("test_df")
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(MockEngine("engine1"), ["test_df"])

        orchestrator.clear()

        # After clear, no engines should be executed
        result = orchestrator.run()
        assert result.total_engines == 0


class TestEngineExecutionConfig:
    """Tests for EngineExecutionConfig."""

    def test_config_defaults(self):
        """Test default configuration values."""
        config = EngineExecutionConfig()

        assert config.strategy == ExecutionStrategy.SEQUENTIAL
        assert config.max_workers is None
        assert config.repository is None
        assert config.fail_fast is False
        assert config.timeout_ms is None

    def test_config_with_values(self):
        """Test configuration with custom values."""
        config = EngineExecutionConfig(
            strategy=ExecutionStrategy.PARALLEL,
            max_workers=4,
            repository={"type": "memory"},
            fail_fast=True,
            timeout_ms=5000,
        )

        assert config.strategy == ExecutionStrategy.PARALLEL
        assert config.max_workers == 4
        assert config.repository is not None
        assert config.fail_fast is True
        assert config.timeout_ms == 5000


class TestEngineResult:
    """Tests for EngineResult."""

    def test_successful_result(self):
        """Test successful engine result."""
        result = EngineResult(
            task_id="task1",
            engine_name="test_engine",
            dataframe_name="test_df",
            metrics=[{"check": "test", "success": True}],
            success=True,
            execution_time_ms=100,
        )

        assert result.success is True
        assert result.execution_time_ms == 100
        assert len(result.metrics) == 1

    def test_failed_result(self):
        """Test failed engine result."""
        error = RuntimeError("Test error")
        result = EngineResult(
            task_id="task1",
            engine_name="test_engine",
            dataframe_name="test_df",
            metrics=[],
            success=False,
            error=error,
        )

        assert result.success is False
        assert result.error == error
        assert len(result.metrics) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
