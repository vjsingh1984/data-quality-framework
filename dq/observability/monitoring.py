# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dq.observability.exporter import MetricsExporter

logger = logging.getLogger(__name__)


@dataclass
class PerformanceSnapshot:
    """Performance measurement snapshot.

    Attributes:
        start_time_ms: Start time in milliseconds.
        end_time_ms: End time in milliseconds.
        duration_ms: Duration in milliseconds.
        engine: Engine name.
        dataset: Dataset name.
        metadata: Additional performance metadata.
    """

    start_time_ms: int
    end_time_ms: int
    duration_ms: int
    engine: str
    dataset: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "duration_ms": self.duration_ms,
            "engine": self.engine,
            "dataset": self.dataset,
            "metadata": self.metadata,
        }


@dataclass
class ResourceUsage:
    """Resource usage statistics.

    Attributes:
        executor_memory_mb: Executor memory in MB (if available).
        driver_memory_mb: Driver memory in MB (if available).
        num_executors: Number of executors.
        cores: Total cores.
    """

    executor_memory_mb: Optional[int] = None
    driver_memory_mb: Optional[int] = None
    num_executors: Optional[int] = None
    cores: Optional[int] = None

    @classmethod
    def from_spark_context(cls, spark) -> "ResourceUsage":
        """Create resource usage from Spark context.

        Args:
            spark: SparkSession.

        Returns:
            ResourceUsage object.
        """
        try:
            sc = spark.sparkContext
            conf = sc.getConf()

            return cls(
                executor_memory_mb=_parse_memory_mb(
                    conf.get("spark.executor.memory", "1g")
                ),
                driver_memory_mb=_parse_memory_mb(
                    conf.get("spark.driver.memory", "1g")
                ),
                num_executors=int(conf.get("spark.executor.instances", "0") or 0),
                cores=int(conf.get("spark.executor.cores", "1") or 1),
            )
        except Exception as e:
            logger.debug("Failed to get Spark resource info: %s", e)
            return cls()


def _parse_memory_mb(mem_str: str) -> Optional[int]:
    """Parse memory string to MB.

    Args:
        mem_str: Memory string (e.g., "4g", "512m").

    Returns:
        Memory in MB, or None if parsing fails.
    """
    try:
        mem_str = mem_str.strip().lower()
        if mem_str.endswith("g"):
            return int(mem_str[:-1]) * 1024
        elif mem_str.endswith("m"):
            return int(mem_str[:-1])
        elif mem_str.endswith("k"):
            return int(mem_str[:-1]) // 1024
        else:
            return int(mem_str) // (1024 * 1024)
    except (ValueError, AttributeError):
        return None


class PerformanceMonitor:
    """Performance monitoring for data quality validation.

    Tracks execution times, resource usage, and other performance metrics.
    Integrates with the engine lifecycle hooks.

    Usage::

        monitor = PerformanceMonitor(exporter)

        # Wrap engine execution
        with monitor.monitor_engine("deequ", "my_table"):
            result = engine.apply(dataframe)

        # Get performance snapshots
        snapshots = monitor.get_snapshots()
    """

    def __init__(
        self,
        exporter: Optional[MetricsExporter] = None,
        enable_resource_tracking: bool = True,
    ):
        """Initialize performance monitor.

        Args:
            exporter: Optional metrics exporter for performance data.
            enable_resource_tracking: Whether to track Spark resource usage.
        """
        self._exporter = exporter
        self._enable_resource_tracking = enable_resource_tracking
        self._snapshots: List[PerformanceSnapshot] = []

    @contextmanager
    def monitor_engine(
        self,
        engine_name: str,
        dataset_name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Context manager for monitoring engine execution.

        Args:
            engine_name: Name of the engine being monitored.
            dataset_name: Name of the dataset.
            metadata: Optional additional metadata.

        Yields:
            PerformanceSnapshot object (populated after execution).
        """
        start_time_ms = int(time.time() * 1000)
        snapshot = PerformanceSnapshot(
            start_time_ms=start_time_ms,
            end_time_ms=0,
            duration_ms=0,
            engine=engine_name,
            dataset=dataset_name,
            metadata=metadata or {},
        )

        try:
            yield snapshot
        finally:
            end_time_ms = int(time.time() * 1000)
            snapshot.end_time_ms = end_time_ms
            snapshot.duration_ms = end_time_ms - start_time_ms
            self._snapshots.append(snapshot)

            # Export performance metric
            if self._exporter is not None:
                from dq.observability.metrics import PerformanceMetric

                perf_metric = PerformanceMetric(
                    name=f"engine_{engine_name}_duration_ms",
                    value=float(snapshot.duration_ms),
                    engine=engine_name,
                    dataset=dataset_name,
                    metadata=metadata or {},
                )
                self._exporter.export_performance_metrics([perf_metric])

            logger.debug(
                "Engine '%s' completed in %d ms",
                engine_name,
                snapshot.duration_ms,
            )

    def record_dataframe_stats(
        self, dataframe: DataFrame, dataset_name: str
    ) -> Dict[str, Any]:
        """Record DataFrame statistics.

        Args:
            dataframe: Spark DataFrame.
            dataset_name: Dataset name.

        Returns:
            Dictionary with DataFrame statistics.
        """
        stats = {
            "dataset": dataset_name,
            "row_count": dataframe.count(),
            "column_count": len(dataframe.columns),
        }

        logger.debug(
            "DataFrame stats for '%s': %d rows, %d columns",
            dataset_name,
            stats["row_count"],
            stats["column_count"],
        )

        return stats

    def get_snapshots(
        self, engine: Optional[str] = None, dataset: Optional[str] = None
    ) -> List[PerformanceSnapshot]:
        """Get performance snapshots.

        Args:
            engine: Optional engine name filter.
            dataset: Optional dataset name filter.

        Returns:
            List of matching performance snapshots.
        """
        filtered = list(self._snapshots)
        if engine is not None:
            filtered = [s for s in filtered if s.engine == engine]
        if dataset is not None:
            filtered = [s for s in filtered if s.dataset == dataset]
        return filtered

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary statistics.

        Returns:
            Dictionary with summary statistics.
        """
        if not self._snapshots:
            return {
                "total_executions": 0,
                "total_duration_ms": 0,
                "avg_duration_ms": 0,
                "engine_stats": {},
            }

        engine_stats = {}
        engine_durations: Dict[str, List[int]] = {}

        for snapshot in self._snapshots:
            if snapshot.engine not in engine_durations:
                engine_durations[snapshot.engine] = []
            engine_durations[snapshot.engine].append(snapshot.duration_ms)

        for engine_name, durations in engine_durations.items():
            engine_stats[engine_name] = {
                "executions": len(durations),
                "total_duration_ms": sum(durations),
                "avg_duration_ms": sum(durations) // len(durations),
                "min_duration_ms": min(durations),
                "max_duration_ms": max(durations),
            }

        return {
            "total_executions": len(self._snapshots),
            "total_duration_ms": sum(s.duration_ms for s in self._snapshots),
            "avg_duration_ms": sum(s.duration_ms for s in self._snapshots)
            // len(self._snapshots),
            "engine_stats": engine_stats,
        }

    def clear(self) -> None:
        """Clear all performance snapshots."""
        self._snapshots.clear()


class ObservableDQEngine:
    """Wrapper for DQ engines with observability support.

    Automatically tracks execution time and exports metrics.

    Usage::

        engine = ObservableDQEngine(base_engine, monitor)
        result = engine.apply(dataframe)
    """

    def __init__(
        self,
        base_engine,
        performance_monitor: PerformanceMonitor,
        dataset_name: str = "unknown",
    ):
        """Initialize observable engine wrapper.

        Args:
            base_engine: Base DQ engine instance.
            performance_monitor: Performance monitor instance.
            dataset_name: Dataset name for metrics.
        """
        self._base_engine = base_engine
        self._monitor = performance_monitor
        self._dataset_name = dataset_name

        # Extract engine name
        self._engine_name = (
            getattr(base_engine, "__class__", type(base_engine))
            .__name__.replace("Engine", "")
            .lower()
        )

    def apply(self, dataframe, repository=None):
        """Apply engine with performance monitoring.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config.

        Returns:
            List of metric dictionaries.
        """
        # Record DataFrame stats
        df_stats = self._monitor.record_dataframe_stats(dataframe, self._dataset_name)

        # Monitor execution
        with self._monitor.monitor_engine(
            self._engine_name, self._dataset_name, metadata=df_stats
        ):
            result = self._base_engine.apply(dataframe, repository)

        return result

    def __getattr__(self, name):
        """Delegate all other attributes to base engine."""
        return getattr(self._base_engine, name)
