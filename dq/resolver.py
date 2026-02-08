# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""DataFrame resolution abstractions using Chain of Responsibility pattern."""
from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from dq.exceptions import DataFrameNotFoundError

logger = logging.getLogger(__name__)

# Pattern for valid Spark/Hive/Unity table identifiers
_TABLE_NAME_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){0,2}$"
)


class DataFrameResolver(ABC):
    """Abstract base class for DataFrame resolution."""

    @abstractmethod
    def resolve(self, name: str) -> Optional[DataFrame]:
        """Attempt to resolve a DataFrame by name.

        Args:
            name: Logical DataFrame name.

        Returns:
            Spark DataFrame if found, None to delegate to next resolver.
        """


class DefaultDataFrameResolver(DataFrameResolver):
    """Resolves the special ``"default"`` DataFrame name."""

    def __init__(self, default_dataframe: Optional[DataFrame] = None):
        self._default_dataframe = default_dataframe

    def resolve(self, name: str) -> Optional[DataFrame]:
        if name == "default" and self._default_dataframe is not None:
            return self._default_dataframe
        return None


class ConfigDataFrameResolver(DataFrameResolver):
    """Resolves DataFrames pre-loaded from configuration."""

    def __init__(self, dataframes: dict):
        self._dataframes = dataframes

    def resolve(self, name: str) -> Optional[DataFrame]:
        return self._dataframes.get(name)


class SparkCatalogResolver(DataFrameResolver):
    """Resolves DataFrames from Spark's registered tables and temp views."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def resolve(self, name: str) -> Optional[DataFrame]:
        try:
            table_names = [t.name for t in self._spark.catalog.listTables()]
            if name in table_names:
                if not _TABLE_NAME_PATTERN.match(name):
                    raise DataFrameNotFoundError(f"Invalid table name format: '{name}'")
                return self._spark.table(name)
        except DataFrameNotFoundError:
            raise
        except Exception as e:
            logger.debug("Spark catalog lookup failed for '%s': %s", name, e)
        return None


class CatalogProviderResolver(DataFrameResolver):
    """Resolves DataFrames via a configured catalog provider (Unity/Glue/Spark)."""

    def __init__(self, catalog_provider, catalog_type: Optional[str] = None):
        self._catalog_provider = catalog_provider
        self._catalog_type = catalog_type

    def resolve(self, name: str) -> Optional[DataFrame]:
        try:
            return self._catalog_provider.get_dataframe(name)
        except Exception as e:
            logger.debug("Catalog provider lookup failed for '%s': %s", name, e)
            return None


class ChainedResolver(DataFrameResolver):
    """Chains multiple resolvers, trying each in order.

    Raises ``DataFrameNotFoundError`` if no resolver can resolve the name.
    """

    def __init__(
        self, resolvers: List[DataFrameResolver], catalog_type: Optional[str] = None
    ):
        self._resolvers = resolvers
        self._catalog_type = catalog_type

    def resolve(self, name: str) -> DataFrame:
        """Resolve a DataFrame by trying each resolver in order.

        Args:
            name: Logical DataFrame name.

        Returns:
            Spark DataFrame.

        Raises:
            DataFrameNotFoundError: If no resolver can find the DataFrame.
        """
        for resolver in self._resolvers:
            result = resolver.resolve(name)
            if result is not None:
                return result
        raise DataFrameNotFoundError(
            f"DataFrame '{name}' not found in config, Spark catalog, "
            f"or {self._catalog_type or 'default'} catalog."
        )
