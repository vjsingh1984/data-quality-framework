# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Main orchestrator for the Data Quality Framework."""
import json
import logging
from typing import Any, Dict, List, Optional

from dq.catalog.catalog_factory import CatalogFactory
from dq.config.config_loader import AutoConfigLoader, ConfigLoader
from dq.engine.engine_loader import EngineLoader
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

    def _load_dataframes(self):
        """Load DataFrames from configuration.

        Returns:
            Dict mapping logical names to DataFrames.
        """
        dataframes = {}
        config_dataframes = self._config.get("dqframework.dataframes", {})

        for df_name, table_ref in config_dataframes.items():
            try:
                df = self._resolve_config_dataframe(df_name, table_ref)
                if df is not None:
                    dataframes[df_name] = df
            except Exception as e:
                logger.warning("Could not load DataFrame '%s': %s", df_name, e)

        if self.default_dataframe is not None and "default" not in dataframes:
            dataframes["default"] = self.default_dataframe

        return dataframes

    def _resolve_config_dataframe(self, df_name, table_ref):
        """Resolve a DataFrame reference from configuration."""
        if isinstance(table_ref, str):
            return self._catalog_provider.get_dataframe(table_ref)
        elif hasattr(table_ref, "get"):
            table = table_ref.get("table", df_name)
            database = table_ref.get("database", None)
            catalog = table_ref.get("catalog", None)
            return self._catalog_provider.get_dataframe(
                table, database=database, catalog=catalog
            )
        else:
            return self._catalog_provider.get_dataframe(str(table_ref))

    def run(self) -> List[Dict[str, Any]]:
        """Execute all configured data quality rules.

        Returns:
            List of metric dictionaries with keys:
                - ``check``: Check description
                - ``success``: Boolean result
                - ``details``: Detailed check output
                - ``ts``: Timestamp in milliseconds
                - ``jobid``: Spark application ID
        """
        from pydeequ.repository import ResultKey

        current_time_in_millis = ResultKey.current_milli_time()
        cumulative_metrics = []

        for rule_config in self._config.get("dqframework.dqrules", []):
            df_names = rule_config.get("dataframes", ["default"])
            engine_name = rule_config.get(constants.DQ_ENGINE_NAME, None)

            if engine_name is None:
                logger.warning("Rule missing 'engine' key, skipping: %s", rule_config)
                continue

            engine = self._engine_loader.load_engine(
                engine_name, rule_config, current_time_in_millis
            )

            for df_name in df_names:
                dataframe = self.get_dataframe(df_name)
                summary_metrics = engine.apply(
                    dataframe, repository=self._config.get("dqframework.repository", {})
                )
                for metric in summary_metrics:
                    metric["ts"] = current_time_in_millis
                    metric["jobid"] = self._spark.sparkContext.applicationId
                    if constants.DQ_METRICS_RESULT_SUCCESS_KEY in metric:
                        if not metric[constants.DQ_METRICS_RESULT_SUCCESS_KEY]:
                            logger.warning("Check failed: %s", json.dumps(metric))
                    cumulative_metrics.append(metric)

        return cumulative_metrics

    def get_dataframe(self, df_name):
        """Retrieve a DataFrame by logical name.

        Uses the configured resolver chain. Checks in order:
        1. Default DataFrame (if df_name is "default")
        2. Pre-loaded DataFrames from config
        3. Spark catalog (temp views / tables)
        4. Configured catalog provider

        Args:
            df_name: Logical DataFrame name.

        Returns:
            Spark DataFrame.

        Raises:
            DataFrameNotFoundError: If DataFrame cannot be found.
        """
        return self._resolver.resolve(df_name)
