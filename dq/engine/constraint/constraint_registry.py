# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Registry for custom constraint implementations."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Tuple, Type

from dq.utils.registry_base import GenericRegistry

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class CustomConstraint(ABC):
    """Abstract base class for custom constraint implementations."""

    @abstractmethod
    def evaluate(
        self, dataframe: DataFrame, config: dict, spark_session
    ) -> Tuple[List[list], List[list]]:
        """Evaluate the constraint against a DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            config: Check configuration dict with constraint parameters.
            spark_session: Active SparkSession.

        Returns:
            Tuple of (metric_results, verification_results) where each is
            a list of row lists.
        """


class ConstraintRegistry(GenericRegistry[CustomConstraint]):
    """Registry for custom constraint types.

    Supports external constraint registration via ``register(name, cls)``.
    Inherits thread-safe registration and lookup from GenericRegistry.
    """

    _constraints: Dict[str, Type[CustomConstraint]] = {}

    @classmethod
    def _get_registry(cls):
        """Return _constraints for backward compatibility.

        This allows the parent class methods to work with _constraints dict.
        """
        return cls._constraints

    # Note: register, get, is_registered, unregister inherited from GenericRegistry
    # and work with _constraints via _get_registry()

    @classmethod
    def list_constraints(cls) -> List[str]:
        """Return list of registered constraint names.

        Alias for list_items() for backward compatibility.
        """
        return cls.list_items()
