# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dq.engine.multi_engine.execution_strategy import (
    BatchedExecutionStrategy,
    EngineResult,
    EngineTask,
    ExecutionStrategy,
    ExecutionStrategyBase,
    ParallelExecutionStrategy,
    SequentialExecutionStrategy,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


@dataclass
class EngineExecutionConfig:
    """Configuration for engine execution.

    Attributes:
        strategy: Execution strategy (sequential, parallel, batched).
        max_workers: Maximum number of workers for parallel execution.
        repository: Optional repository configuration for persistence.
        fail_fast: Whether to stop on first error.
        timeout_ms: Optional timeout in milliseconds.
    """

    strategy: ExecutionStrategy = ExecutionStrategy.SEQUENTIAL
    max_workers: Optional[int] = None
    repository: Optional[Dict[str, Any]] = None
    fail_fast: bool = False
    timeout_ms: Optional[int] = None


@dataclass
class MultiEngineResult:
    """Aggregated result from multi-engine execution.

    Attributes:
        results: List of individual engine results.
        total_engines: Total number of engines executed.
        successful_engines: Number of successful executions.
        failed_engines: Number of failed executions.
        total_execution_time_ms: Total execution time across all engines.
    """

    results: List[EngineResult]
    total_engines: int
    successful_engines: int
    failed_engines: int
    total_execution_time_ms: int

    def get_metrics(self) -> List[Dict[str, Any]]:
        """Get all metrics from all engine executions.

        Returns:
            Flattened list of metric dictionaries.
        """
        all_metrics = []
        for result in self.results:
            all_metrics.extend(result.metrics)
        return all_metrics

    def get_results_by_engine(self, engine_name: str) -> List[EngineResult]:
        """Get results for a specific engine.

        Args:
            engine_name: Name of the engine.

        Returns:
            List of results for the engine.
        """
        return [r for r in self.results if r.engine_name == engine_name]

    def get_results_by_dataframe(self, dataframe_name: str) -> List[EngineResult]:
        """Get results for a specific DataFrame.

        Args:
            dataframe_name: Name of the DataFrame.

        Returns:
            List of results for the DataFrame.
        """
        return [r for r in self.results if r.dataframe_name == dataframe_name]

    def get_failed_results(self) -> List[EngineResult]:
        """Get all failed engine results.

        Returns:
            List of failed results.
        """
        return [r for r in self.results if not r.success]

    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary.

        Returns:
            Dictionary with summary statistics.
        """
        from collections import Counter

        engine_counts = Counter(r.engine_name for r in self.results)
        dataframe_counts = Counter(r.dataframe_name for r in self.results)

        return {
            "total_engines": self.total_engines,
            "successful_engines": self.successful_engines,
            "failed_engines": self.failed_engines,
            "total_execution_time_ms": self.total_execution_time_ms,
            "engine_counts": dict(engine_counts),
            "dataframe_counts": dict(dataframe_counts),
        }


class MultiEngineOrchestrator:
    """Orchestrates execution of multiple data quality engines.

    Supports different execution strategies and provides aggregated
    results and error handling.

    Usage::

        orchestrator = MultiEngineOrchestrator(
            strategy=ExecutionStrategy.PARALLEL,
            max_workers=4
        )

        # Add engines
        orchestrator.add_engine(deequ_engine, ["df1", "df2"])
        orchestrator.add_engine(custom_engine, ["df1"])

        # Execute all engines
        result = orchestrator.run()
        print(f"Executed {result.total_engines} engines")
    """

    def __init__(
        self,
        strategy: ExecutionStrategy = ExecutionStrategy.SEQUENTIAL,
        max_workers: Optional[int] = None,
        fail_fast: bool = False,
    ):
        """Initialize the multi-engine orchestrator.

        Args:
            strategy: Execution strategy.
            max_workers: Maximum workers for parallel execution.
            fail_fast: Stop on first error.
        """
        self._strategy = strategy
        self._max_workers = max_workers
        self._fail_fast = fail_fast
        self._engines: List[Any] = []
        self._dataframes: Dict[str, DataFrame] = {}

    def add_engine(self, engine: Any, dataframes: List[str]) -> None:
        """Add an engine to the orchestrator.

        Args:
            engine: DQ engine instance.
            dataframes: List of DataFrame names to validate.
        """
        self._engines.append((engine, dataframes))

    def add_dataframe(self, name: str, dataframe: DataFrame) -> None:
        """Add a DataFrame for validation.

        Args:
            name: Logical name for the DataFrame.
            dataframe: Spark DataFrame.
        """
        self._dataframes[name] = dataframe

    def run(
        self,
        repository: Optional[Dict[str, Any]] = None,
        timeout_ms: Optional[int] = None,
    ) -> MultiEngineResult:
        """Execute all registered engines.

        Args:
            repository: Optional repository configuration.
            timeout_ms: Optional timeout in milliseconds.

        Returns:
            MultiEngineResult with aggregated results.
        """
        import time

        start_time_ms = int(time.time() * 1000)

        # Build execution tasks
        tasks = self._build_tasks(repository)

        # Get execution strategy
        strategy_instance = self._create_strategy()

        # Execute tasks
        results = strategy_instance.execute(tasks)

        # Handle fail_fast
        if self._fail_fast:
            failed_results = [r for r in results if not r.success]
            if failed_results:
                first_error = failed_results[0].error
                raise RuntimeError(
                    f"Engine execution failed (fail_fast=True): {first_error}"
                ) from first_error

        end_time_ms = int(time.time() * 1000)
        total_execution_time_ms = end_time_ms - start_time_ms

        # Build aggregated result
        successful_count = sum(1 for r in results if r.success)
        failed_count = len(results) - successful_count

        return MultiEngineResult(
            results=results,
            total_engines=len(results),
            successful_engines=successful_count,
            failed_engines=failed_count,
            total_execution_time_ms=total_execution_time_ms,
        )

    def run_with_config(self, config: EngineExecutionConfig) -> MultiEngineResult:
        """Execute engines with execution config.

        Args:
            config: Engine execution configuration.

        Returns:
            MultiEngineResult with aggregated results.
        """
        self._strategy = config.strategy
        self._max_workers = config.max_workers
        self._fail_fast = config.fail_fast

        return self.run(
            repository=config.repository,
            timeout_ms=config.timeout_ms,
        )

    def _build_tasks(self, repository: Optional[Dict[str, Any]]) -> List[EngineTask]:
        """Build execution tasks from registered engines and dataframes.

        Args:
            repository: Optional repository configuration.

        Returns:
            List of engine tasks.
        """
        tasks = []
        task_counter = 0

        for engine, dataframe_names in self._engines:
            for df_name in dataframe_names:
                if df_name not in self._dataframes:
                    logger.warning(
                        "DataFrame '%s' not found, skipping engine '%s'",
                        df_name,
                        engine.__class__.__name__,
                    )
                    continue

                task = EngineTask(
                    engine=engine,
                    dataframe=self._dataframes[df_name],
                    dataframe_name=df_name,
                    config={"repository": repository} if repository else {},
                    task_id=f"task_{task_counter}",
                )
                tasks.append(task)
                task_counter += 1

        return tasks

    def _create_strategy(self) -> ExecutionStrategyBase:
        """Create execution strategy instance.

        Returns:
            ExecutionStrategyBase instance.
        """
        if self._strategy == ExecutionStrategy.PARALLEL:
            return ParallelExecutionStrategy(max_workers=self._max_workers)
        elif self._strategy == ExecutionStrategy.BATCHED:
            return BatchedExecutionStrategy(max_workers=self._max_workers)
        else:
            return SequentialExecutionStrategy()

    def clear(self) -> None:
        """Clear all registered engines and dataframes."""
        self._engines.clear()
        self._dataframes.clear()
