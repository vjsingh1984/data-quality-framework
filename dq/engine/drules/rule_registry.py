# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Registry for declarative rule implementations."""
from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

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

    @abstractmethod
    def evaluate(self, dataframe, config: dict) -> List[Dict[str, Any]]:
        """Evaluate the rule against a DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            config: Check configuration dict.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """


class RuleRegistry:
    """Registry for declarative rule types."""

    _rules: Dict[str, Type[DRule]] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, rule_class: Type[DRule]) -> None:
        """Register a rule class.

        Args:
            name: Rule name as it appears in config.
            rule_class: DRule subclass.
        """
        with cls._lock:
            cls._rules[name] = rule_class
        logger.debug("Registered rule '%s' -> %s", name, rule_class.__name__)

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
        with cls._lock:
            if name not in cls._rules:
                raise KeyError(
                    f"Unknown rule type '{name}'. "
                    f"Available: {', '.join(cls._rules.keys())}"
                )
            return cls._rules[name]

    @classmethod
    def unregister(cls, name: str) -> None:
        """Remove a registered rule."""
        with cls._lock:
            cls._rules.pop(name, None)

    @classmethod
    def list_rules(cls) -> List[str]:
        """Return list of registered rule names."""
        return list(cls._rules.keys())
