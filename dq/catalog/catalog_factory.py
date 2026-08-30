# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Factory for creating catalog provider instances."""
import logging
import threading

from dq.catalog.base import CatalogProvider
from dq.catalog.glue_catalog import GlueCatalogProvider
from dq.catalog.spark_catalog import SparkCatalogProvider
from dq.catalog.unity_catalog import UnityCatalogProvider

logger = logging.getLogger(__name__)


class CatalogFactory:
    """Factory that creates catalog provider instances based on catalog type.

    Supports automatic detection of the catalog type from SparkSession
    configuration when no explicit type is specified.

    Custom catalog providers can be registered via ``register_provider()``.

    Usage::

        factory = CatalogFactory()
        provider = factory.get_provider(spark, catalog_type="unity")
        df = provider.get_dataframe("main.sales.orders")
        schema = provider.get_table_schema("main.sales.orders")
    """

    # Registry of catalog type names to provider classes
    _CATALOG_PROVIDERS = {
        "spark": SparkCatalogProvider,
        "hive": SparkCatalogProvider,  # Hive uses Spark's built-in catalog
        "unity": UnityCatalogProvider,
        "delta": UnityCatalogProvider,  # Delta tables use Unity Catalog on Databricks
        "glue": GlueCatalogProvider,
    }
    _lock = threading.Lock()

    @classmethod
    def register_provider(cls, catalog_type: str, provider_class):
        """Register a custom catalog provider.

        Args:
            catalog_type: Catalog type identifier (e.g., "my_system").
            provider_class: CatalogProvider subclass to register.

        Raises:
            TypeError: If provider_class doesn't inherit from CatalogProvider.
        """
        if not issubclass(provider_class, CatalogProvider):
            raise TypeError(
                f"{provider_class.__name__} must inherit from CatalogProvider"
            )

        catalog_type = catalog_type.lower().strip()
        with cls._lock:
            cls._CATALOG_PROVIDERS[catalog_type] = provider_class
            logger.debug(
                "Registered catalog provider '%s' -> %s",
                catalog_type,
                provider_class.__name__,
            )

    @staticmethod
    def get_provider(spark_session, catalog_type=None, **kwargs):
        """Create a catalog provider for the given type.

        Args:
            spark_session: Active SparkSession.
            catalog_type: Catalog type string ("spark", "unity", "glue", "hive").
                If None, attempts to auto-detect from SparkSession configuration.
            **kwargs: Additional keyword arguments passed to the provider constructor.
                For Glue: ``region_name``, ``glue_client``.

        Returns:
            CatalogProvider instance.

        Raises:
            ValueError: If the catalog type is not recognized.
        """
        if catalog_type is None:
            catalog_type = CatalogFactory._detect_catalog_type(spark_session)

        catalog_type = catalog_type.lower().strip()
        logger.info("Using catalog provider: %s", catalog_type)

        provider_class = CatalogFactory._CATALOG_PROVIDERS.get(catalog_type)
        if provider_class is None:
            raise ValueError(
                f"Unknown catalog_type '{catalog_type}'. "
                f"Supported types: {', '.join(CatalogFactory._CATALOG_PROVIDERS.keys())}"
            )

        if provider_class == GlueCatalogProvider:
            return provider_class(
                spark_session,
                region_name=kwargs.get("region_name"),
                glue_client=kwargs.get("glue_client"),
            )

        return provider_class(spark_session)

    @staticmethod
    def _detect_catalog_type(spark_session):
        """Auto-detect catalog type from SparkSession configuration.

        Detection heuristics:
        1. If ``spark.sql.catalog.spark_catalog`` contains "DeltaCatalog"
           or "UnityCatalog", return "unity".
        2. If ``spark.hadoop.hive.metastore.client.factory.class`` contains
           "AWSGlue", return "glue".
        3. If ``spark.sql.catalog.spark_catalog`` contains "BigLakeCatalog",
           return "spark" (Dataproc BigLake uses Spark catalog interface).
        4. If ``spark.sql.catalog.spark_catalog`` contains "OneLakeCatalog",
           return "spark" (Fabric OneLake uses Spark catalog interface).
        5. Otherwise, return "spark" (default).

        Args:
            spark_session: Active SparkSession.

        Returns:
            Detected catalog type string.
        """
        try:
            conf = spark_session.sparkContext.getConf()

            # Check for Unity Catalog / Delta Catalog
            spark_catalog = conf.get("spark.sql.catalog.spark_catalog", "")
            if "DeltaCatalog" in spark_catalog or "UnityCatalog" in spark_catalog:
                logger.info("Auto-detected Unity Catalog from SparkSession config")
                return "unity"

            # Check for Glue metastore
            metastore_class = conf.get(
                "spark.hadoop.hive.metastore.client.factory.class", ""
            )
            if "AWSGlue" in metastore_class:
                logger.info("Auto-detected Glue Catalog from SparkSession config")
                return "glue"

            # Dataproc BigLake and Fabric OneLake use Spark catalog interface
            if "BigLakeCatalog" in spark_catalog:
                logger.info("Auto-detected Dataproc BigLake from SparkSession config")
                return "spark"
            if "OneLakeCatalog" in spark_catalog:
                logger.info("Auto-detected Fabric OneLake from SparkSession config")
                return "spark"

        except Exception as e:
            logger.debug("Could not auto-detect catalog type: %s", e)

        logger.debug("Defaulting to Spark catalog")
        return "spark"

    @classmethod
    def supported_types(cls):
        """Return list of supported catalog type names.

        Returns:
            List of strings.
        """
        return list(cls._CATALOG_PROVIDERS.keys())
