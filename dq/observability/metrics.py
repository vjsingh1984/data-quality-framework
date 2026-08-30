# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class MetricType(str, Enum):
    """Type of metric for categorization and export.

    Attributes:
        COUNTER: Monotonically increasing value (e.g., total checks run).
        GAUGE: Point-in-time value (e.g., current execution time).
        HISTOGRAM: Distribution of values (e.g., execution times).
        SUMMARY: Summary with quantiles (e.g., request durations).
    """

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricLabel:
    """Key-value label for metrics.

    Attributes:
        key: Label key (e.g., "engine", "dataset", "constraint").
        value: Label value.
    """

    key: str
    value: str


@dataclass
class Metric:
    """Observability metric with normalized schema.

    Attributes:
        name: Metric name (e.g., "dq_checks_total", "dq_execution_time_ms").
        description: Human-readable description.
        metric_type: Type of metric (counter, gauge, histogram, summary).
        value: Numeric metric value.
        labels: Optional labels for dimensional metrics.
        timestamp_ms: Epoch milliseconds when metric was recorded.
    """

    name: str
    description: str
    metric_type: MetricType
    value: float
    labels: List[MetricLabel] = field(default_factory=list)
    timestamp_ms: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "type": self.metric_type.value,
            "value": self.value,
            "labels": {label.key: label.value for label in self.labels},
            "timestamp_ms": self.timestamp_ms,
        }

    @classmethod
    def from_dq_metric(cls, dq_metric: Dict[str, Any]) -> List["Metric"]:
        """Convert DQ metric to observability metrics.

        Creates multiple metrics from a single DQ metric:
        - Check result metric (pass/fail)
        - Execution time metric (if available)
        - Constraint type metric

        Args:
            dq_metric: DQ metric dictionary.

        Returns:
            List of Metric objects.
        """
        import time

        metrics = []
        timestamp_ms = dq_metric.get("ts", int(time.time() * 1000))
        engine = dq_metric.get("engine", "unknown")
        dataset = dq_metric.get("dataset", "unknown")
        constraint = dq_metric.get("constraint", "unknown")
        success = dq_metric.get("success", True)

        # Check result metric
        labels = [
            MetricLabel("engine", engine),
            MetricLabel("dataset", dataset),
            MetricLabel("constraint", constraint),
        ]
        if "check" in dq_metric:
            labels.append(MetricLabel("check", dq_metric["check"]))

        metrics.append(
            cls(
                name="dq_check_result",
                description="Data quality check result (1=pass, 0=fail)",
                metric_type=MetricType.GAUGE,
                value=1.0 if success else 0.0,
                labels=labels,
                timestamp_ms=timestamp_ms,
            )
        )

        # Total checks counter
        metrics.append(
            cls(
                name="dq_checks_total",
                description="Total number of data quality checks executed",
                metric_type=MetricType.COUNTER,
                value=1.0,
                labels=[
                    MetricLabel("engine", engine),
                    MetricLabel("dataset", dataset),
                    MetricLabel("constraint", constraint),
                    MetricLabel("status", "pass" if success else "fail"),
                ],
                timestamp_ms=timestamp_ms,
            )
        )

        # Execution time metric (if available)
        execution_time_ms = dq_metric.get("execution_time_ms")
        if execution_time_ms is not None:
            metrics.append(
                cls(
                    name="dq_check_execution_time_ms",
                    description="Data quality check execution time in milliseconds",
                    metric_type=MetricType.GAUGE,
                    value=float(execution_time_ms),
                    labels=[
                        MetricLabel("engine", engine),
                        MetricLabel("dataset", dataset),
                        MetricLabel("constraint", constraint),
                    ],
                    timestamp_ms=timestamp_ms,
                )
            )

        return metrics


@dataclass
class PerformanceMetric:
    """Performance monitoring metric.

    Attributes:
        name: Metric name (e.g., "engine_apply_duration_ms").
        value: Numeric value.
        engine: Engine name.
        dataset: Dataset name.
        metadata: Additional metadata (e.g., row_count, column_count).
    """

    name: str
    value: float
    engine: str
    dataset: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class MetricsRegistry:
    """Registry for managing observability metrics.

    Provides thread-safe storage and retrieval of metrics.
    """

    def __init__(self):
        """Initialize the metrics registry."""
        self._metrics: List[Metric] = []
        self._performance_metrics: List[PerformanceMetric] = []

    def record(self, metric: Metric) -> None:
        """Record a metric.

        Args:
            metric: Metric to record.
        """
        self._metrics.append(metric)

    def record_performance(self, metric: PerformanceMetric) -> None:
        """Record a performance metric.

        Args:
            metric: Performance metric to record.
        """
        self._performance_metrics.append(metric)

    def get_metrics(
        self, filter_labels: Optional[Dict[str, str]] = None
    ) -> List[Metric]:
        """Get all recorded metrics.

        Args:
            filter_labels: Optional label filters (key=value pairs).

        Returns:
            List of metrics matching the filter.
        """
        if filter_labels is None:
            return list(self._metrics)

        filtered = []
        for metric in self._metrics:
            metric_labels = {label.key: label.value for label in metric.labels}
            if all(metric_labels.get(k) == v for k, v in filter_labels.items()):
                filtered.append(metric)
        return filtered

    def get_performance_metrics(
        self, engine: Optional[str] = None, dataset: Optional[str] = None
    ) -> List[PerformanceMetric]:
        """Get performance metrics.

        Args:
            engine: Optional engine name filter.
            dataset: Optional dataset name filter.

        Returns:
            List of performance metrics matching the filter.
        """
        filtered = list(self._performance_metrics)
        if engine is not None:
            filtered = [m for m in filtered if m.engine == engine]
        if dataset is not None:
            filtered = [m for m in filtered if m.dataset == dataset]
        return filtered

    def clear(self) -> None:
        """Clear all recorded metrics."""
        self._metrics.clear()
        self._performance_metrics.clear()

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics.

        Returns:
            Dictionary with metric counts and statistics.
        """
        from collections import Counter

        check_results = [m for m in self._metrics if m.name == "dq_check_result"]
        pass_count = sum(1 for m in check_results if m.value > 0)
        fail_count = sum(1 for m in check_results if m.value == 0)

        engine_counts: Counter[str] = Counter()
        for metric in self._metrics:
            for label in metric.labels:
                if label.key == "engine":
                    engine_counts[label.value] += 1

        return {
            "total_metrics": len(self._metrics),
            "total_performance_metrics": len(self._performance_metrics),
            "checks_passed": pass_count,
            "checks_failed": fail_count,
            "engine_counts": dict(engine_counts),
        }
