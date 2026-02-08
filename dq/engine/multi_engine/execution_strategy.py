# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ExecutionStrategy(str, Enum):
    """Engine execution strategy.

    Attributes:
        SEQUENTIAL: Execute engines one at a time (default).
        PARALLEL: Execute engines concurrently using threads.
        BATCHED: Group engines by dataframe and execute in batches.
    """

    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    BATCHED = "batched"


@dataclass
class EngineTask:
    """A single engine execution task.

    Attributes:
        engine: DQ engine instance.
        dataframe: DataFrame to validate.
        dataframe_name: Name of the DataFrame.
        config: Execution configuration.
        task_id: Unique task identifier.
    """

    engine: Any
    dataframe: DataFrame
    dataframe_name: str
    config: Dict[str, Any] = field(default_factory=dict)
    task_id: Optional[str] = None


@dataclass
class EngineResult:
    """Result from a single engine execution.

    Attributes:
        task_id: Task identifier.
        engine_name: Name of the engine.
        dataframe_name: Name of the DataFrame.
        metrics: List of metric dictionaries.
        success: Whether execution succeeded.
        error: Exception if execution failed.
        execution_time_ms: Execution time in milliseconds.
    """

    task_id: str
    engine_name: str
    dataframe_name: str
    metrics: List[Dict[str, Any]]
    success: bool
    error: Optional[Exception] = None
    execution_time_ms: Optional[int] = None


class ExecutionStrategyBase(ABC):
    """Base class for execution strategies.

    Defines the interface for different engine execution patterns.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """Initialize the execution strategy.

        Args:
            max_workers: Maximum number of workers for parallel execution.
        """
        self._max_workers = max_workers

    @abstractmethod
    def execute(self, tasks: List[EngineTask]) -> List[EngineResult]:
        """Execute a list of engine tasks.

        Args:
            tasks: List of tasks to execute.

        Returns:
            List of engine results.
        """
        pass

    def _execute_task(self, task: EngineTask) -> EngineResult:
        """Execute a single engine task.

        Args:
            task: Task to execute.

        Returns:
            EngineResult with metrics or error.
        """
        import time

        start_time_ms = int(time.time() * 1000)
        task_id = (
            task.task_id or f"{task.engine.__class__.__name__}_{task.dataframe_name}"
        )

        try:
            # Get engine name
            engine_name = (
                getattr(task.engine, "__class__", type(task.engine))
                .__name__.replace("Engine", "")
                .lower()
            )

            # Execute the engine
            logger.info(
                "Executing engine '%s' on DataFrame '%s'",
                engine_name,
                task.dataframe_name,
            )

            metrics = task.engine.apply(
                task.dataframe, repository=task.config.get("repository")
            )

            end_time_ms = int(time.time() * 1000)
            execution_time_ms = end_time_ms - start_time_ms

            logger.info(
                "Engine '%s' completed in %d ms",
                engine_name,
                execution_time_ms,
            )

            return EngineResult(
                task_id=task_id,
                engine_name=engine_name,
                dataframe_name=task.dataframe_name,
                metrics=metrics,
                success=True,
                execution_time_ms=execution_time_ms,
            )

        except Exception as e:
            end_time_ms = int(time.time() * 1000)
            execution_time_ms = end_time_ms - start_time_ms

            logger.error(
                "Engine '%s' failed on DataFrame '%s': %s",
                task.engine.__class__.__name__,
                task.dataframe_name,
                e,
                exc_info=True,
            )

            return EngineResult(
                task_id=task_id,
                engine_name=task.engine.__class__.__name__,
                dataframe_name=task.dataframe_name,
                metrics=[],
                success=False,
                error=e,
                execution_time_ms=execution_time_ms,
            )


class SequentialExecutionStrategy(ExecutionStrategyBase):
    """Execute engines sequentially, one at a time.

    This is the default execution strategy and provides
    predictable execution order and minimal resource usage.
    """

    def execute(self, tasks: List[EngineTask]) -> List[EngineResult]:
        """Execute tasks sequentially.

        Args:
            tasks: List of tasks to execute.

        Returns:
            List of engine results in execution order.
        """
        results = []
        for task in tasks:
            result = self._execute_task(task)
            results.append(result)
        return results


class ParallelExecutionStrategy(ExecutionStrategyBase):
    """Execute engines in parallel using a thread pool.

    Suitable for I/O-bound engines or when you have multiple
    dataframes to validate concurrently.
    """

    def __init__(self, max_workers: Optional[int] = None):
        """Initialize the parallel execution strategy.

        Args:
            max_workers: Maximum number of worker threads.
                Defaults to min(32, os.cpu_count() + 4).
        """
        super().__init__(max_workers)
        self._executor: Optional[Executor] = None
        self._results_lock = Lock()
        self._results: List[EngineResult] = []

    def execute(self, tasks: List[EngineTask]) -> List[EngineResult]:
        """Execute tasks in parallel.

        Args:
            tasks: List of tasks to execute.

        Returns:
            List of engine results (order not guaranteed).
        """
        if not tasks:
            return []

        self._results = []
        max_workers = self._max_workers or min(32, (os.cpu_count() or 1) + 4)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures: Dict[Future, EngineTask] = {}
            for task in tasks:
                future = executor.submit(self._execute_task, task)
                futures[future] = task

            # Collect results as they complete
            for future in futures:
                try:
                    result = future.result()
                    with self._results_lock:
                        self._results.append(result)
                except Exception as e:
                    task = futures[future]
                    logger.error("Task execution failed: %s", e, exc_info=True)
                    with self._results_lock:
                        self._results.append(
                            EngineResult(
                                task_id=task.task_id or "unknown",
                                engine_name=task.engine.__class__.__name__,
                                dataframe_name=task.dataframe_name,
                                metrics=[],
                                success=False,
                                error=e,
                            )
                        )

        return self._results


class BatchedExecutionStrategy(ExecutionStrategyBase):
    """Execute engines in batches grouped by DataFrame.

    This strategy groups tasks by DataFrame and executes all engines
    for a single DataFrame before moving to the next DataFrame.
    This can be more efficient when multiple engines operate on
    the same DataFrame.
    """

    def execute(self, tasks: List[EngineTask]) -> List[EngineResult]:
        """Execute tasks in batches grouped by DataFrame.

        Args:
            tasks: List of tasks to execute.

        Returns:
            List of engine results grouped by DataFrame.
        """
        # Group tasks by DataFrame
        batches: Dict[str, List[EngineTask]] = {}
        for task in tasks:
            df_name = task.dataframe_name
            if df_name not in batches:
                batches[df_name] = []
            batches[df_name].append(task)

        # Execute each batch
        results = []
        for df_name, batch_tasks in batches.items():
            logger.info(
                "Executing batch of %d engines on DataFrame '%s'",
                len(batch_tasks),
                df_name,
            )
            for task in batch_tasks:
                result = self._execute_task(task)
                results.append(result)

        return results
