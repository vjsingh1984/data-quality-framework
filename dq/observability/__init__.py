# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Observability and monitoring integration for the Data Quality Framework.

This module provides metrics export, performance monitoring, and alerting
capabilities for data quality validation.

Supported exporters:
- **Prometheus**: Expose metrics for Prometheus scraping
- **OpenTelemetry**: Send metrics to OTel-compatible backends
- **Datadog**: Send metrics to Datadog API
- **Logging**: Fallback to structured logging

Usage::

    from dq.observability import MetricsExporter, PrometheusExporter

    # Create exporter
    exporter = PrometheusExporter(port=9090)
    exporter.start()

    # Export metrics
    exporter.export_metrics(metrics)

    # Stop exporter
    exporter.stop()
"""

from dq.observability.exporter import (
    DatadogExporter,
    LoggingExporter,
    MetricsExporter,
    OpenTelemetryExporter,
    PrometheusExporter,
)
from dq.observability.metrics import MetricsRegistry
from dq.observability.monitoring import PerformanceMonitor

__all__ = [
    "MetricsExporter",
    "PrometheusExporter",
    "OpenTelemetryExporter",
    "DatadogExporter",
    "LoggingExporter",
    "MetricsRegistry",
    "PerformanceMonitor",
]
