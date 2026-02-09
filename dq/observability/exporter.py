# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from dq.observability.metrics import Metric, PerformanceMetric

logger = logging.getLogger(__name__)


class MetricsExporter(ABC):
    """Abstract base class for metrics exporters.

    Exporters send metrics to external monitoring systems like
    Prometheus, OpenTelemetry, Datadog, or log files.

    Implementations should be thread-safe and handle failures gracefully.
    """

    def __init__(self):
        """Initialize the exporter."""
        self._enabled = True

    def export_metrics(self, metrics: List[Dict[str, Any]]) -> None:
        """Export DQ metrics to external system.

        Args:
            metrics: List of DQ metric dictionaries.
        """
        if not self._enabled:
            return

        try:
            observability_metrics = self._convert_to_metrics(metrics)
            self._export(observability_metrics)
        except Exception as e:
            logger.error("Failed to export metrics: %s", e, exc_info=True)

    def export_performance_metrics(self, metrics: List[PerformanceMetric]) -> None:
        """Export performance metrics.

        Args:
            metrics: List of performance metrics.
        """
        if not self._enabled:
            return

        try:
            self._export_performance(metrics)
        except Exception as e:
            logger.error("Failed to export performance metrics: %s", e, exc_info=True)

    @abstractmethod
    def _export(self, metrics: List[Metric]) -> None:
        """Export observability metrics.

        Args:
            metrics: List of Metric objects.
        """
        pass

    def _export_performance(self, metrics: List[PerformanceMetric]) -> None:
        """Export performance metrics (default implementation).

        Default implementation converts performance metrics to regular metrics
        and exports them. Subclasses can override for custom handling.

        Args:
            metrics: List of PerformanceMetric objects.
        """
        from dq.observability.metrics import Metric, MetricLabel, MetricType

        regular_metrics = []
        for perf_metric in metrics:
            labels = [
                MetricLabel("engine", perf_metric.engine),
                MetricLabel("dataset", perf_metric.dataset),
            ]
            # Add metadata as labels
            for key, value in perf_metric.metadata.items():
                labels.append(MetricLabel(key, str(value)))

            regular_metrics.append(
                Metric(
                    name=perf_metric.name,
                    description=f"Performance metric: {perf_metric.name}",
                    metric_type=MetricType.GAUGE,
                    value=perf_metric.value,
                    labels=labels,
                )
            )
        self._export(regular_metrics)

    def _convert_to_metrics(self, dq_metrics: List[Dict[str, Any]]) -> List[Metric]:
        """Convert DQ metrics to observability metrics.

        Args:
            dq_metrics: List of DQ metric dictionaries.

        Returns:
            List of Metric objects.
        """
        from dq.observability.metrics import Metric

        observability_metrics = []
        for dq_metric in dq_metrics:
            observability_metrics.extend(Metric.from_dq_metric(dq_metric))
        return observability_metrics

    def enable(self) -> None:
        """Enable the exporter."""
        self._enabled = True

    def disable(self) -> None:
        """Disable the exporter."""
        self._enabled = False

    def is_enabled(self) -> bool:
        """Check if exporter is enabled.

        Returns:
            True if enabled, False otherwise.
        """
        return self._enabled


class PrometheusExporter(MetricsExporter):
    """Export metrics to Prometheus format.

    Exposes metrics on an HTTP endpoint for Prometheus to scrape.
    Uses prometheus_client library if available, otherwise falls back
    to logging.

    Usage::

        exporter = PrometheusExporter(port=9090)
        exporter.start()
        # ... metrics are exported ...
        exporter.stop()
    """

    def __init__(self, port: int = 9090, host: str = "0.0.0.0"):
        """Initialize Prometheus exporter.

        Args:
            port: HTTP port for metrics endpoint (default: 9090).
            host: Host to bind to (default: all interfaces).
        """
        super().__init__()
        self._port = port
        self._host = host
        self._server = None
        self._registry = None
        self._prometheus_modules: dict[str, Any] | None = None

        try:
            from prometheus_client import (
                CollectorRegistry,
                Counter,
                Gauge,
                start_http_server,
            )

            self._prometheus_registry = CollectorRegistry()
            self._prometheus_modules = {
                "CollectorRegistry": CollectorRegistry,
                "Gauge": Gauge,
                "Counter": Counter,
                "start_http_server": start_http_server,
            }
            self._metrics_cache: Dict[str, Any] = {}
        except ImportError:
            logger.warning(
                "prometheus_client not installed. "
                "PrometheusExporter will fall back to logging."
            )
            self._prometheus_modules = None

    def start(self) -> None:
        """Start the Prometheus HTTP server."""
        if self._prometheus_modules is None:
            logger.info("Prometheus exporter running in logging mode (no HTTP server)")
            return

        try:
            self._server = self._prometheus_modules["start_http_server"](
                self._port, addr=self._host
            )
            logger.info(
                "Prometheus exporter started on http://%s:%d", self._host, self._port
            )
        except Exception as e:
            logger.error("Failed to start Prometheus server: %s", e)

    def stop(self) -> None:
        """Stop the Prometheus HTTP server."""
        if self._server is not None:
            # Prometheus client doesn't provide a stop method
            # The server runs in a daemon thread and will stop when the process exits
            logger.info("Prometheus exporter stopped")

    def _export(self, metrics: List[Metric]) -> None:
        """Export metrics to Prometheus.

        Args:
            metrics: List of Metric objects.
        """
        if self._prometheus_modules is None:
            # Fallback to logging
            for metric in metrics:
                logger.info(
                    "Prometheus metric: %s%s %s",
                    metric.name,
                    self._format_labels(metric),
                    metric.value,
                )
            return

        try:
            self._update_prometheus_metrics(metrics)
        except Exception as e:
            logger.error("Failed to update Prometheus metrics: %s", e)

    def _update_prometheus_metrics(self, metrics: List[Metric]) -> None:
        """Update Prometheus metrics.

        Args:
            metrics: List of Metric objects.
        """
        for metric in metrics:
            label_names = [label.key for label in metric.labels]
            label_values = [label.value for label in metric.labels]
            cache_key = f"{metric.name}_{tuple(label_names)}"

            if cache_key not in self._metrics_cache:
                # Create new metric
                modules = self._prometheus_modules
                if modules is None:
                    continue

                if metric.metric_type.value == "counter":
                    prom_metric = modules["Counter"](
                        metric.name,
                        metric.description,
                        label_names,
                        registry=self._prometheus_registry,
                    )
                else:
                    prom_metric = modules["Gauge"](
                        metric.name,
                        metric.description,
                        label_names,
                        registry=self._prometheus_registry,
                    )
                self._metrics_cache[cache_key] = prom_metric
            else:
                prom_metric = self._metrics_cache[cache_key]

            # Set metric value
            if label_values:
                prom_metric.labels(*label_values).set(metric.value)
            else:
                prom_metric.set(metric.value)

    def _format_labels(self, metric: Metric) -> str:
        """Format labels for Prometheus text format.

        Args:
            metric: Metric object.

        Returns:
            Formatted label string.
        """
        if not metric.labels:
            return ""

        label_pairs = [f'{label.key}="{label.value}"' for label in metric.labels]
        return "{" + ",".join(label_pairs) + "}"


class OpenTelemetryExporter(MetricsExporter):
    """Export metrics to OpenTelemetry-compatible backends.

    Supports any OTel-compatible backend using the OpenTelemetry SDK.
    Can export to collectors, Jaeger, and other OTel-compatible systems.

    Usage::

        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        exporter = OpenTelemetryExporter(endpoint="http://localhost:4318")
        exporter.export_metrics(metrics)
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:4318",
        service_name: str = "data-quality-framework",
    ):
        """Initialize OpenTelemetry exporter.

        Args:
            endpoint: OTLP endpoint URL.
            service_name: Service name for metrics.
        """
        super().__init__()
        self._endpoint = endpoint
        self._service_name = service_name
        self._otel_modules: dict[str, Any] | None = None
        self._meter: Any = None

        try:
            from opentelemetry import metrics
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                OTLPMetricExporter,
            )
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

            self._otel_modules = {
                "metrics": metrics,
                "MeterProvider": MeterProvider,
                "PeriodicExportingMetricReader": PeriodicExportingMetricReader,
                "OTLPMetricExporter": OTLPMetricExporter,
            }

            # Initialize meter provider
            otlp_exporter = OTLPMetricExporter(endpoint=self._endpoint)
            reader = PeriodicExportingMetricReader(otlp_exporter)
            provider = MeterProvider(metric_readers=[reader])
            metrics.set_meter_provider(provider)

            self._meter = metrics.get_meter(__name__)
            self._instruments: Dict[str, Any] = {}

        except ImportError:
            logger.warning(
                "opentelemetry-sdk not installed. "
                "OpenTelemetryExporter will fall back to logging."
            )
            self._otel_modules = None
            self._meter = None

    def _export(self, metrics: List[Metric]) -> None:
        """Export metrics to OpenTelemetry.

        Args:
            metrics: List of Metric objects.
        """
        if self._meter is None:
            # Fallback to logging
            for metric in metrics:
                logger.info("OpenTelemetry metric: %s %s", metric.name, metric.value)
            return

        try:
            for metric in metrics:
                self._record_otel_metric(metric)
        except Exception as e:
            logger.error("Failed to export OpenTelemetry metrics: %s", e)

    def _record_otel_metric(self, metric: Metric) -> None:
        """Record a single OpenTelemetry metric.

        Args:
            metric: Metric object.
        """
        instrument_key = f"{metric.name}_{metric.metric_type.value}"

        if instrument_key not in self._instruments:
            # Create instrument
            if metric.metric_type.value == "counter":
                instrument = self._meter.create_counter(
                    metric.name, description=metric.description
                )
            else:
                instrument = self._meter.create_gauge(
                    metric.name, description=metric.description
                )
            self._instruments[instrument_key] = instrument
        else:
            instrument = self._instruments[instrument_key]

        # Record metric
        attributes = {label.key: label.value for label in metric.labels}
        instrument.record(metric.value, attributes=attributes)


class DatadogExporter(MetricsExporter):
    """Export metrics to Datadog using the Datadog API.

    Requires datadog package and valid API credentials.
    Metrics are sent to Datadog via the API or DogStatsD.

    Usage::

        exporter = DatadogExporter(api_key="...", app_key="...")
        exporter.export_metrics(metrics)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        app_key: Optional[str] = None,
        host: str = "localhost",
        port: int = 8125,
    ):
        """Initialize Datadog exporter.

        Args:
            api_key: Datadog API key (for API export).
            app_key: Datadog app key (for API export).
            host: DogStatsD host (for DogStatsD export).
            port: DogStatsD port (default: 8125).
        """
        super().__init__()
        self._api_key = api_key
        self._app_key = app_key
        self._host = host
        self._port = port
        self._datadog_modules: dict[str, Any] | None = None
        self._statsd: Any = None

        try:
            from datadog import DogStatsd, api, initialize

            self._datadog_modules = {
                "DogStatsd": DogStatsd,
                "initialize": initialize,
                "api": api,
            }

            # Initialize DogStatsD client
            self._statsd = DogStatsd(host=self._host, port=self._port)

            # Initialize API client if keys provided
            if api_key and app_key:
                initialize(api_key=api_key, app_key=app_key)

        except ImportError:
            logger.warning(
                "datadog package not installed. "
                "DatadogExporter will fall back to logging."
            )
            self._datadog_modules = None
            self._statsd = None

    def _export(self, metrics: List[Metric]) -> None:
        """Export metrics to Datadog.

        Args:
            metrics: List of Metric objects.
        """
        if self._statsd is None:
            # Fallback to logging
            for metric in metrics:
                logger.info("Datadog metric: %s %s", metric.name, metric.value)
            return

        try:
            for metric in metrics:
                self._export_datadog_metric(metric)
        except Exception as e:
            logger.error("Failed to export Datadog metrics: %s", e)

    def _export_datadog_metric(self, metric: Metric) -> None:
        """Export a single Datadog metric.

        Args:
            metric: Metric object.
        """
        # Convert metric name to Datadog format (use dots instead of underscores for hierarchy)
        metric_name = metric.name.replace("_", ".")

        # Get tags from labels
        tags = [f"{label.key}:{label.value}" for label in metric.labels]

        # Send to DogStatsD
        self._statsd.gauge(metric_name, metric.value, tags=tags)


class LoggingExporter(MetricsExporter):
    """Export metrics to structured logs.

    Simple fallback exporter that writes metrics to logs in JSON format.
    Useful for development and testing.

    Usage::

        exporter = LoggingExporter(level=logging.INFO)
        exporter.export_metrics(metrics)
    """

    def __init__(self, level: int = 20):
        """Initialize logging exporter.

        Args:
            level: Logging level (default: INFO).
        """
        super().__init__()
        self._level = level

    def _export(self, metrics: List[Metric]) -> None:
        """Export metrics to logs.

        Args:
            metrics: List of Metric objects.
        """
        import json

        for metric in metrics:
            log_entry = {
                "metric": metric.name,
                "value": metric.value,
                "type": metric.metric_type.value,
                "labels": {label.key: label.value for label in metric.labels},
                "timestamp_ms": metric.timestamp_ms,
            }
            logger.log(self._level, json.dumps(log_entry))
