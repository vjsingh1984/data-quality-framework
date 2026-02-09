# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Main orchestrator for the Data Quality Framework."""
import json
import logging
from typing import Any, Dict, List, Optional, Sequence

from dq.catalog.catalog_factory import CatalogFactory
from dq.config.config_loader import AutoConfigLoader, ConfigLoader
from dq.engine.engine_loader import EngineLoader
from dq.exceptions import ConfigurationError
from dq.observability import (
    DatadogExporter,
    LoggingExporter,
    MetricsExporter,
    MetricsRegistry,
    OpenTelemetryExporter,
    PerformanceMonitor,
    PrometheusExporter,
)
from dq.resolver import (
    CatalogProviderResolver,
    ChainedResolver,
    ConfigDataFrameResolver,
    DefaultDataFrameResolver,
    SparkCatalogResolver,
)
from dq.utils import constants

logger = logging.getLogger(__name__)


class DQFramework:
    """Main orchestrator that loads configurations, manages DataFrames,
    and coordinates engine execution for data quality validation.

    Args:
        spark: Active SparkSession.
        config: Configuration string (HOCON), or URI (file://, s3://, abfss://, http://).
        default_dataframe: Optional default DataFrame for rules that don't
            specify explicit dataframe references.
        config_loader: Optional custom ConfigLoader. Defaults to AutoConfigLoader.
        resolver: Optional custom DataFrameResolver. Built automatically if not provided.
        engine_loader: Optional custom EngineLoader.
    """

    def __init__(
        self,
        spark,
        config,
        default_dataframe=None,
        *,
        config_loader: Optional[ConfigLoader] = None,
        resolver=None,
        engine_loader=None,
    ):
        self._spark = spark
        self._config_loader = config_loader or AutoConfigLoader()
        self._config = self._config_loader.load(config)
        self.default_dataframe = default_dataframe
        self._catalog_type = self._config.get("dqframework.catalog_type", None)
        self._catalog_provider = CatalogFactory.get_provider(
            spark, catalog_type=self._catalog_type
        )
        self._engine_loader = engine_loader or EngineLoader()
        self.dataframes = self._load_dataframes()
        self._resolver = resolver or self._build_resolver()

        # Initialize observability components
        self._metrics_exporters = self._init_metrics_exporters()
        self._metrics_registry = MetricsRegistry()
        self._performance_monitor = PerformanceMonitor(
            exporter=self._metrics_exporters[0] if self._metrics_exporters else None
        )

    def _build_resolver(self) -> ChainedResolver:
        """Build the default resolver chain."""
        return ChainedResolver(
            resolvers=[
                DefaultDataFrameResolver(self.default_dataframe),
                ConfigDataFrameResolver(self.dataframes),
                SparkCatalogResolver(self._spark),
                CatalogProviderResolver(self._catalog_provider, self._catalog_type),
            ],
            catalog_type=self._catalog_type,
        )

    def _init_metrics_exporters(self) -> Sequence[MetricsExporter]:
        """Initialize metrics exporters from configuration.

        Returns:
            Sequence of configured metrics exporters.
        """
        exporters = []
        observability_config = self._config.get("dqframework.observability", {})

        # Check which exporters are enabled
        if observability_config.get("prometheus_enabled", False):
            prometheus_port = observability_config.get("prometheus_port", 9090)
            prometheus_exporter = PrometheusExporter(port=prometheus_port)
            exporters.append(prometheus_exporter)
            logger.info("Prometheus exporter enabled on port %d", prometheus_port)

        if observability_config.get("opentelemetry_enabled", False):
            otel_endpoint = observability_config.get(
                "opentelemetry_endpoint", "http://localhost:4318"
            )
            otel_exporter = OpenTelemetryExporter(endpoint=otel_endpoint)
            exporters.append(otel_exporter)
            logger.info("OpenTelemetry exporter enabled: %s", otel_endpoint)

        if observability_config.get("datadog_enabled", False):
            api_key = observability_config.get("datadog_api_key")
            app_key = observability_config.get("datadog_app_key")
            datadog_exporter = DatadogExporter(api_key=api_key, app_key=app_key)
            exporters.append(datadog_exporter)
            logger.info("Datadog exporter enabled")

        # Always add logging exporter as fallback
        logging_exporter = LoggingExporter()
        exporters.append(logging_exporter)
        logger.info("Logging exporter enabled")

        return exporters

    def _load_dataframes(self):
        """Load DataFrames from configuration.

        Returns:
            Dict mapping logical names to DataFrames.
        """
        dataframes = {}
        configured_dataframes = self._config.get("dqframework.dataframes", {})

        for dataframe_name, table_reference in configured_dataframes.items():
            try:
                df = self._resolve_config_dataframe(dataframe_name, table_reference)
                if df is not None:
                    dataframes[dataframe_name] = df
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load DataFrame '{dataframe_name}': {e}"
                ) from e

        if self.default_dataframe is not None and "default" not in dataframes:
            dataframes["default"] = self.default_dataframe

        return dataframes

    def _resolve_config_dataframe(self, dataframe_name, table_reference):
        """Resolve a DataFrame reference from configuration."""
        if isinstance(table_reference, str):
            return self._catalog_provider.get_dataframe(table_reference)
        elif hasattr(table_reference, "get"):
            table = table_reference.get("table", dataframe_name)
            database = table_reference.get("database", None)
            catalog = table_reference.get("catalog", None)
            return self._catalog_provider.get_dataframe(
                table, database=database, catalog=catalog
            )
        else:
            return self._catalog_provider.get_dataframe(str(table_reference))

    def run(self) -> List[Dict[str, Any]]:
        """Execute all configured data quality rules.

        Returns:
            List of metric dictionaries with keys:
                - ``check``: Check description
                - ``success``: Boolean result
                - ``details``: Detailed check output
                - ``ts``: Timestamp in milliseconds
                - ``jobid``: Spark application ID (output format uses 'jobid' key)
        """
        # Check if multi-engine mode is enabled
        execution_mode = self._config.get("dqframework.execution_mode", "sequential")

        if execution_mode in ("multi_engine", "multi-engine", "parallel", "batched"):
            return self._run_multi_engine(execution_mode)
        else:
            return self._run_sequential()

    def _run_sequential(self) -> List[Dict[str, Any]]:
        """Run engines in sequential mode (default behavior).

        Returns:
            List of metric dictionaries.
        """
        from pydeequ.repository import ResultKey

        current_timestamp_ms = ResultKey.current_milli_time()
        application_id = self._spark.sparkContext.applicationId
        cumulative_metrics = []

        for rule_config in self._config.get("dqframework.dqrules", []):
            dataframe_names = rule_config.get("dataframes", ["default"])
            engine_name = rule_config.get(constants.DQ_ENGINE_NAME, None)

            if engine_name is None:
                logger.warning("Rule missing 'engine' key, skipping: %s", rule_config)
                continue

            engine = self._engine_loader.load_engine(
                engine_name, rule_config, current_timestamp_ms
            )

            for dataframe_name in dataframe_names:
                dataframe = self.resolve_dataframe(dataframe_name)

                # Track performance with monitoring
                with self._performance_monitor.monitor_engine(
                    engine_name, dataframe_name
                ):
                    summary_metrics = engine.apply(
                        dataframe,
                        repository=self._config.get("dqframework.repository", {}),
                    )

                # Process metrics
                for metric in summary_metrics:
                    metric["ts"] = current_timestamp_ms
                    metric["jobid"] = application_id
                    metric["engine"] = engine_name
                    metric["dataset"] = dataframe_name
                    if constants.DQ_METRICS_RESULT_SUCCESS_KEY in metric:
                        if not metric[constants.DQ_METRICS_RESULT_SUCCESS_KEY]:
                            try:
                                logger.warning("Check failed: %s", json.dumps(metric))
                            except (TypeError, ValueError):
                                # Metric contains non-JSON-serializable objects
                                logger.warning(
                                    "Check failed: %s",
                                    str(metric.get("check", "unknown")),
                                )
                    cumulative_metrics.append(metric)

        # Export metrics to configured exporters
        if self._metrics_exporters:
            for exporter in self._metrics_exporters:
                try:
                    exporter.export_metrics(cumulative_metrics)
                except Exception as e:
                    logger.error("Failed to export metrics: %s", e)

        # Log performance summary
        perf_summary = self._performance_monitor.get_summary()
        logger.info(
            "Execution completed: %d checks, %d engines, total time: %d ms",
            len(cumulative_metrics),
            perf_summary.get("total_executions", 0),
            perf_summary.get("total_execution_time_ms", 0),
        )

        return cumulative_metrics

    def _run_multi_engine(self, execution_mode: str) -> List[Dict[str, Any]]:
        """Run engines using multi-engine orchestrator.

        Args:
            execution_mode: Execution mode (parallel, batched).

        Returns:
            List of metric dictionaries.
        """
        from pydeequ.repository import ResultKey

        from dq.engine.multi_engine import (
            EngineExecutionConfig,
            ExecutionStrategy,
            MultiEngineOrchestrator,
        )

        current_timestamp_ms = ResultKey.current_milli_time()
        application_id = self._spark.sparkContext.applicationId

        # Determine strategy
        if execution_mode in ("parallel", "multi_engine"):
            strategy = ExecutionStrategy.PARALLEL
        elif execution_mode == "batched":
            strategy = ExecutionStrategy.BATCHED
        else:
            strategy = ExecutionStrategy.SEQUENTIAL

        # Get max workers from config
        max_workers = self._config.get("dqframework.max_workers", None)

        # Create orchestrator
        orchestrator = MultiEngineOrchestrator(
            strategy=strategy,
            max_workers=max_workers,
            fail_fast=self._config.get("dqframework.fail_fast", False),
        )

        # Load engines and dataframes
        engine_configs = self._config.get("dqframework.dqrules", [])

        for rule_config in engine_configs:
            engine_name = rule_config.get(constants.DQ_ENGINE_NAME)
            if not engine_name:
                continue

            # Load engine
            engine = self._engine_loader.load_engine(
                engine_name, rule_config, current_timestamp_ms
            )

            # Get dataframes for this rule
            dataframe_names = rule_config.get("dataframes", ["default"])

            # Add to orchestrator
            orchestrator.add_engine(engine, dataframe_names)

        # Register all pre-loaded dataframes
        for df_name, df in self.dataframes.items():
            orchestrator.add_dataframe(df_name, df)

        # Add default dataframe if present
        if self.default_dataframe is not None:
            orchestrator.add_dataframe("default", self.default_dataframe)

        # Create execution config
        execution_config = EngineExecutionConfig(
            strategy=strategy,
            max_workers=max_workers,
            repository=self._config.get("dqframework.repository", None),
            fail_fast=self._config.get("dqframework.fail_fast", False),
        )

        # Execute
        multi_result = orchestrator.run_with_config(execution_config)

        # Convert MultiEngineResult to metric list format
        cumulative_metrics = []
        for engine_result in multi_result.results:
            # Add timestamps and jobid to each metric
            for metric in engine_result.metrics:
                metric["ts"] = current_timestamp_ms
                metric["jobid"] = application_id
                metric["engine"] = engine_result.engine_name
                metric["dataset"] = engine_result.dataframe_name
                cumulative_metrics.append(metric)

        # Export metrics to configured exporters
        if self._metrics_exporters:
            for exporter in self._metrics_exporters:
                try:
                    exporter.export_metrics(cumulative_metrics)
                except Exception as e:
                    logger.error("Failed to export metrics: %s", e)

        # Log summary
        logger.info(
            "Multi-engine execution completed: %d engines executed",
            multi_result.total_engines,
        )
        logger.info(
            "Successful: %d, Failed: %d, Total time: %d ms",
            multi_result.successful_engines,
            multi_result.failed_engines,
            multi_result.total_execution_time_ms,
        )

        return cumulative_metrics

    def resolve_dataframe(self, dataframe_name):
        """Resolve a logical DataFrame name to an actual DataFrame.

        Uses the configured resolver chain. Checks in order:
        1. Default DataFrame (if dataframe_name is "default")
        2. Pre-loaded DataFrames from config
        3. Spark catalog (temp views / tables)
        4. Configured catalog provider

        Args:
            dataframe_name: Logical DataFrame name.

        Returns:
            Spark DataFrame.

        Raises:
            DataFrameNotFoundError: If DataFrame cannot be found.
        """
        return self._resolver.resolve(dataframe_name)
