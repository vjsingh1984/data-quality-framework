# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Base registry class for consistent registration across the framework."""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Dict, Generic, List, Type, TypeVar

if TYPE_CHECKING:
    pass

T = TypeVar("T")
logger = logging.getLogger(__name__)


class GenericRegistry(Generic[T]):
    """Base registry class for type-safe registration and lookup.

    Provides thread-safe registration, retrieval, and listing of items.
    Subclasses should inherit with the specific type: ``class MyRegistry(GenericRegistry[MyType])``.

    Subclasses must define a class variable ``_registry`` as a dict.
    For backward compatibility, subclasses can define aliases like ``_constraints`` or ``_rules``
    that reference the same dict.

    Attributes:
        _registry: Dictionary mapping names to registered types.
        _lock: Thread lock for thread-safe operations.
    """

    _registry: Dict[str, Type[T]] = {}
    _lock = threading.Lock()

    @classmethod
    def _get_registry(cls) -> Dict[str, Type[T]]:
        """Get the registry dict for this class.

        Allows subclasses to define alternate names like _constraints or _rules.
        """
        return cls._registry

    @classmethod
    def register(cls, name: str, item: Type[T]) -> None:
        """Register an item in the registry.

        Args:
            name: Name identifier (will be normalized to lowercase).
            item: Type to register.
        """
        normalized_name = name.lower()
        registry = cls._get_registry()
        with cls._lock:
            registry[normalized_name] = item
        # Get the name safely - works for both classes and instances
        item_name = getattr(item, "__name__", str(item))
        logger.debug("Registered '%s' -> %s", normalized_name, item_name)

    @classmethod
    def get(cls, name: str) -> Type[T]:
        """Look up an item by name.

        Args:
            name: Name identifier (case-insensitive).

        Returns:
            Registered type.

        Raises:
            KeyError: If name is not registered.
        """
        normalized_name = name.lower()
        registry = cls._get_registry()
        with cls._lock:
            if normalized_name not in registry:
                available = ", ".join(registry.keys())
                raise KeyError(f"Unknown '{name}'. Available: {available}")
            return registry[normalized_name]

    @classmethod
    def unregister(cls, name: str) -> None:
        """Remove a registered item.

        Args:
            name: Name identifier (case-insensitive).
        """
        normalized_name = name.lower()
        registry = cls._get_registry()
        with cls._lock:
            registry.pop(normalized_name, None)
        logger.debug("Unregistered '%s'", normalized_name)

    @classmethod
    def is_registered(cls, name: str) -> bool:
        """Check if an item is registered.

        Args:
            name: Name identifier (case-insensitive).

        Returns:
            True if registered, False otherwise.
        """
        normalized_name = name.lower()
        registry = cls._get_registry()
        with cls._lock:
            return normalized_name in registry

    @classmethod
    def list_items(cls) -> List[str]:
        """List all registered item names.

        Returns:
            List of registered names.
        """
        registry = cls._get_registry()
        with cls._lock:
            return list(registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered items.

        This is primarily useful for testing.
        """
        registry = cls._get_registry()
        with cls._lock:
            registry.clear()
