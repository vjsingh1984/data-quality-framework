# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for CatalogFactory provider selection (mocked SparkSession)."""
from unittest.mock import MagicMock

import pytest

from dq.catalog.catalog_factory import CatalogFactory
from dq.catalog.glue_catalog import GlueCatalogProvider
from dq.catalog.spark_catalog import SparkCatalogProvider
from dq.catalog.unity_catalog import UnityCatalogProvider


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession with configurable conf."""
    spark = MagicMock()
    conf = MagicMock()
    conf.get.return_value = ""
    spark.sparkContext.getConf.return_value = conf
    return spark


class TestCatalogFactorySelection:
    """Tests for catalog provider selection by type string."""

    def test_spark_type(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="spark")
        assert isinstance(provider, SparkCatalogProvider)

    def test_hive_type(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="hive")
        assert isinstance(provider, SparkCatalogProvider)

    def test_unity_type(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="unity")
        assert isinstance(provider, UnityCatalogProvider)

    def test_delta_type(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="delta")
        assert isinstance(provider, UnityCatalogProvider)

    def test_glue_type(self, mock_spark):
        provider = CatalogFactory.get_provider(
            mock_spark, catalog_type="glue", glue_client=MagicMock()
        )
        assert isinstance(provider, GlueCatalogProvider)

    def test_unknown_type_raises(self, mock_spark):
        with pytest.raises(ValueError, match="Unknown catalog_type"):
            CatalogFactory.get_provider(mock_spark, catalog_type="unknown")

    def test_case_insensitive(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="SPARK")
        assert isinstance(provider, SparkCatalogProvider)

    def test_whitespace_stripped(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark, catalog_type="  spark  ")
        assert isinstance(provider, SparkCatalogProvider)

    def test_supported_types(self):
        types = CatalogFactory.supported_types()
        assert "spark" in types
        assert "unity" in types
        assert "glue" in types
        assert "hive" in types
        assert "delta" in types


class TestCatalogAutoDetection:
    """Tests for auto-detection from SparkSession config."""

    def test_detect_unity(self, mock_spark):
        conf = MagicMock()
        conf.get.side_effect = lambda key, default="": {
            "spark.sql.catalog.spark_catalog": "com.databricks.sql.DeltaCatalog",
            "spark.hadoop.hive.metastore.client.factory.class": "",
        }.get(key, default)
        mock_spark.sparkContext.getConf.return_value = conf
        provider = CatalogFactory.get_provider(mock_spark)
        assert isinstance(provider, UnityCatalogProvider)

    def test_detect_glue(self, mock_spark):
        conf = MagicMock()
        conf.get.side_effect = lambda key, default="": {
            "spark.sql.catalog.spark_catalog": "",
            "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        }.get(key, default)
        mock_spark.sparkContext.getConf.return_value = conf
        provider = CatalogFactory.get_provider(mock_spark, glue_client=MagicMock())
        assert isinstance(provider, GlueCatalogProvider)

    def test_detect_default_spark(self, mock_spark):
        provider = CatalogFactory.get_provider(mock_spark)
        assert isinstance(provider, SparkCatalogProvider)
