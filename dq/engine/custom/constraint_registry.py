# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Registry for custom constraint implementations."""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Type

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


class ConstraintRegistry:
    """Registry for custom constraint types.

    Supports external constraint registration via ``register(name, cls)``.
    """

    _constraints: Dict[str, Type[CustomConstraint]] = {}

    @classmethod
    def register(cls, name: str, constraint_class: Type[CustomConstraint]) -> None:
        """Register a constraint class.

        Args:
            name: Constraint name as it appears in config.
            constraint_class: CustomConstraint subclass.
        """
        cls._constraints[name] = constraint_class
        logger.debug(
            "Registered constraint '%s' -> %s", name, constraint_class.__name__
        )

    @classmethod
    def get(cls, name: str) -> Type[CustomConstraint]:
        """Look up a constraint class by name.

        Args:
            name: Constraint name.

        Returns:
            CustomConstraint subclass.

        Raises:
            KeyError: If constraint is not registered.
        """
        if name not in cls._constraints:
            raise KeyError(
                f"Unknown constraint '{name}'. "
                f"Available: {', '.join(cls._constraints.keys())}"
            )
        return cls._constraints[name]

    @classmethod
    def list_constraints(cls) -> List[str]:
        """Return list of registered constraint names."""
        return list(cls._constraints.keys())
