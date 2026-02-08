# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for observability components."""

import pytest

from dq.observability.exporter import (
    DatadogExporter,
    LoggingExporter,
    OpenTelemetryExporter,
    PrometheusExporter,
)
from dq.observability.metrics import (
    Metric,
    MetricLabel,
    MetricsRegistry,
    MetricType,
    PerformanceMetric,
)
from dq.observability.monitoring import PerformanceMonitor, PerformanceSnapshot


class TestMetricModels:
    """Tests for metric data models."""

    def test_metric_creation(self):
        """Test creating a metric."""
        metric = Metric(
            name="test_metric",
            description="Test metric",
            metric_type=MetricType.GAUGE,
            value=42.0,
            labels=[MetricLabel("key", "value")],
            timestamp_ms=1234567890,
        )
        assert metric.name == "test_metric"
        assert metric.value == 42.0
        assert metric.metric_type == MetricType.GAUGE

    def test_metric_to_dict(self):
        """Test converting metric to dictionary."""
        metric = Metric(
            name="test_metric",
            description="Test metric",
            metric_type=MetricType.COUNTER,
            value=100.0,
            labels=[MetricLabel("engine", "deequ")],
        )
        result = metric.to_dict()
        assert result["name"] == "test_metric"
        assert result["value"] == 100.0
        assert result["type"] == "counter"
        assert result["labels"]["engine"] == "deequ"

    def test_metric_from_dq_metric(self):
        """Test converting DQ metric to observability metrics."""
        dq_metric = {
            "check": "completeness_check",
            "constraint": "Completeness",
            "success": True,
            "engine": "deequ",
            "dataset": "test_table",
            "execution_time_ms": 100,
        }
        metrics = Metric.from_dq_metric(dq_metric)
        assert len(metrics) >= 2  # check_result + checks_total + execution_time
        check_result = [m for m in metrics if m.name == "dq_check_result"][0]
        assert check_result.value == 1.0  # success = 1

    def test_metric_from_dq_metric_failure(self):
        """Test converting failed DQ metric."""
        dq_metric = {
            "check": "completeness_check",
            "constraint": "Completeness",
            "success": False,
            "engine": "deequ",
            "dataset": "test_table",
        }
        metrics = Metric.from_dq_metric(dq_metric)
        check_result = [m for m in metrics if m.name == "dq_check_result"][0]
        assert check_result.value == 0.0  # failure = 0

    def test_performance_metric(self):
        """Test PerformanceMetric dataclass."""
        metric = PerformanceMetric(
            name="engine_duration_ms",
            value=250.0,
            engine="deequ",
            dataset="test_table",
            metadata={"row_count": 1000},
        )
        assert metric.name == "engine_duration_ms"
        assert metric.engine == "deequ"
        assert metric.metadata["row_count"] == 1000


class TestMetricsRegistry:
    """Tests for MetricsRegistry."""

    def test_record_and_get_metrics(self):
        """Test recording and retrieving metrics."""
        registry = MetricsRegistry()
        metric = Metric(
            name="test_metric",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=1.0,
        )
        registry.record(metric)
        metrics = registry.get_metrics()
        assert len(metrics) == 1
        assert metrics[0].name == "test_metric"

    def test_filter_metrics_by_labels(self):
        """Test filtering metrics by labels."""
        registry = MetricsRegistry()
        metric1 = Metric(
            name="test",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=1.0,
            labels=[MetricLabel("engine", "deequ")],
        )
        metric2 = Metric(
            name="test",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=2.0,
            labels=[MetricLabel("engine", "custom")],
        )
        registry.record(metric1)
        registry.record(metric2)

        filtered = registry.get_metrics(filter_labels={"engine": "deequ"})
        assert len(filtered) == 1
        assert filtered[0].value == 1.0

    def test_record_performance_metrics(self):
        """Test recording performance metrics."""
        registry = MetricsRegistry()
        perf_metric = PerformanceMetric(
            name="test_perf",
            value=100.0,
            engine="deequ",
            dataset="test_table",
        )
        registry.record_performance(perf_metric)
        perf_metrics = registry.get_performance_metrics()
        assert len(perf_metrics) == 1

    def test_get_performance_metrics_filtered(self):
        """Test filtering performance metrics."""
        registry = MetricsRegistry()
        metric1 = PerformanceMetric(
            name="perf",
            value=100.0,
            engine="deequ",
            dataset="table1",
        )
        metric2 = PerformanceMetric(
            name="perf",
            value=200.0,
            engine="custom",
            dataset="table2",
        )
        registry.record_performance(metric1)
        registry.record_performance(metric2)

        # Filter by engine
        deeq_metrics = registry.get_performance_metrics(engine="deequ")
        assert len(deeq_metrics) == 1
        assert deeq_metrics[0].engine == "deequ"

    def test_clear_metrics(self):
        """Test clearing all metrics."""
        registry = MetricsRegistry()
        registry.record(
            Metric(
                name="test",
                description="Test",
                metric_type=MetricType.GAUGE,
                value=1.0,
            )
        )
        assert len(registry.get_metrics()) == 1
        registry.clear()
        assert len(registry.get_metrics()) == 0

    def test_get_summary(self):
        """Test getting metrics summary."""
        registry = MetricsRegistry()
        # Add some check result metrics
        registry.record(
            Metric(
                name="dq_check_result",
                description="Test",
                metric_type=MetricType.GAUGE,
                value=1.0,
                labels=[MetricLabel("engine", "deequ")],
            )
        )
        registry.record(
            Metric(
                name="dq_check_result",
                description="Test",
                metric_type=MetricType.GAUGE,
                value=0.0,
                labels=[MetricLabel("engine", "custom")],
            )
        )

        summary = registry.get_summary()
        assert summary["total_metrics"] == 2
        assert summary["checks_passed"] == 1
        assert summary["checks_failed"] == 1
        assert "deequ" in summary["engine_counts"]


class TestPerformanceMonitor:
    """Tests for PerformanceMonitor."""

    def test_monitor_engine_context(self):
        """Test monitoring engine execution with context manager."""
        import time

        monitor = PerformanceMonitor()
        with monitor.monitor_engine("deequ", "test_table") as snapshot:
            assert snapshot.engine == "deequ"
            assert snapshot.dataset == "test_table"
            # Duration is only calculated after context exits
            time.sleep(0.001)  # Small delay to ensure measurable duration

        # After context exit
        assert snapshot.duration_ms >= 0
        assert snapshot.end_time_ms >= snapshot.start_time_ms

    def test_monitor_records_duration(self):
        """Test that monitor records execution duration."""
        import time

        monitor = PerformanceMonitor()
        with monitor.monitor_engine("deequ", "test_table"):
            time.sleep(0.01)  # 10ms

        snapshots = monitor.get_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].duration_ms >= 10  # At least 10ms

    def test_get_snapshots_filtered(self):
        """Test filtering snapshots."""
        monitor = PerformanceMonitor()
        with monitor.monitor_engine("deequ", "table1"):
            pass
        with monitor.monitor_engine("custom", "table2"):
            pass

        deeq_snapshots = monitor.get_snapshots(engine="deequ")
        assert len(deeq_snapshots) == 1
        assert deeq_snapshots[0].engine == "deequ"

    def test_get_summary(self):
        """Test getting performance summary."""
        monitor = PerformanceMonitor()
        with monitor.monitor_engine("deequ", "table1"):
            pass
        with monitor.monitor_engine("custom", "table2"):
            pass

        summary = monitor.get_summary()
        assert summary["total_executions"] == 2
        assert summary["total_duration_ms"] >= 0  # Can be 0 if execution is very fast
        assert "deequ" in summary["engine_stats"]
        assert "custom" in summary["engine_stats"]

    def test_clear_snapshots(self):
        """Test clearing performance snapshots."""
        monitor = PerformanceMonitor()
        with monitor.monitor_engine("deeq", "test"):
            pass

        assert len(monitor.get_snapshots()) == 1
        monitor.clear()
        assert len(monitor.get_snapshots()) == 0


class TestExporters:
    """Tests for metrics exporters."""

    def test_logging_exporter(self, caplog):
        """Test LoggingExporter writes to logs."""
        import logging

        exporter = LoggingExporter(level=logging.INFO)
        metric = Metric(
            name="test_metric",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=42.0,
        )

        with caplog.at_level(logging.INFO, logger="dq.observability.exporter"):
            exporter._export([metric])

        assert "test_metric" in caplog.text
        assert "42.0" in caplog.text

    def test_prometheus_exporter_enable_disable(self):
        """Test PrometheusExporter enable/disable."""
        exporter = PrometheusExporter(port=9091)
        assert exporter.is_enabled() is True

        exporter.disable()
        assert exporter.is_enabled() is False

        exporter.enable()
        assert exporter.is_enabled() is True

    def test_prometheus_exporter_no_prometheus_client(self, caplog):
        """Test PrometheusExporter doesn't throw exceptions."""
        exporter = PrometheusExporter(port=9091)
        metric = Metric(
            name="test_metric",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=42.0,
        )

        # Should not throw exception even if prometheus_client is or isn't installed
        exporter._export([metric])

    def test_opentelemetry_exporter_no_otel(self, caplog):
        """Test OpenTelemetryExporter doesn't throw exceptions."""
        exporter = OpenTelemetryExporter()
        metric = Metric(
            name="test_metric",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=42.0,
        )

        # Should not throw exception even if OTel SDK is or isn't installed
        exporter._export([metric])

    def test_datadog_exporter_no_datadog(self, caplog):
        """Test DatadogExporter doesn't throw exceptions."""
        exporter = DatadogExporter()
        metric = Metric(
            name="test_metric",
            description="Test",
            metric_type=MetricType.GAUGE,
            value=42.0,
        )

        # Should not throw exception even if datadog is or isn't installed
        exporter._export([metric])


class TestPerformanceSnapshot:
    """Tests for PerformanceSnapshot."""

    def test_snapshot_to_dict(self):
        """Test converting snapshot to dictionary."""
        snapshot = PerformanceSnapshot(
            start_time_ms=1000,
            end_time_ms=1500,
            duration_ms=500,
            engine="deequ",
            dataset="test_table",
            metadata={"rows": 1000},
        )
        result = snapshot.to_dict()
        assert result["engine"] == "deequ"
        assert result["duration_ms"] == 500
        assert result["metadata"]["rows"] == 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
