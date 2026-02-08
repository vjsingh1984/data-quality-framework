# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Registry for declarative rule implementations."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

from dq.utils.registry_base import GenericRegistry

logger = logging.getLogger(__name__)


class DRule(ABC):
    """Abstract base class for declarative rule implementations."""

    @abstractmethod
    def validate_config(self, config: dict) -> None:
        """Validate rule configuration at init time.

        Args:
            config: Check configuration dict.

        Raises:
            ConfigurationError: If config is invalid.
        """
        pass

    @abstractmethod
    def evaluate(self, dataframe, config: dict) -> List[Dict[str, Any]]:
        """Evaluate the rule against a DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            config: Check configuration dict.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        pass


class RuleRegistry(GenericRegistry[DRule]):
    """Registry for declarative rule types.

    Supports external rule registration via ``register(name, cls)``.
    Inherits thread-safe registration and lookup from GenericRegistry.
    """

    _rules: Dict[str, Type[DRule]] = {}

    @classmethod
    def _get_registry(cls):
        """Return _rules for backward compatibility.

        This allows the parent class methods to work with _rules dict.
        """
        return cls._rules

    @classmethod
    def get(cls, name: str) -> Type[DRule]:
        """Look up a rule class by name.

        Args:
            name: Rule name.

        Returns:
            DRule subclass.

        Raises:
            KeyError: If rule is not registered.
        """
        normalized_name = name.lower()
        registry = cls._get_registry()
        with cls._lock:
            if normalized_name not in registry:
                available = ", ".join(registry.keys())
                raise KeyError(f"Unknown rule type '{name}'. Available: {available}")
            return registry[normalized_name]

    # Note: register, unregister, is_registered inherited from GenericRegistry
    # and work with _rules via _get_registry()

    @classmethod
    def list_rules(cls) -> List[str]:
        """Return list of registered rule names.

        Alias for list_items() for backward compatibility.
        """
        return cls.list_items()
