# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Engine discovery and registration system.

Supports three discovery methods in priority order:
1. Explicit registration via ``EngineRegistry.register()``
2. Convention-based discovery: ``dq.engine.{name}.{name}_engine``
3. Entry points: ``importlib.metadata.entry_points(group="dq.engines")``
"""
from __future__ import annotations

import importlib
import logging
import re
from typing import TYPE_CHECKING, Dict, Optional, Type

from dq.utils.registry_base import GenericRegistry

if TYPE_CHECKING:
    from dq.engine.dq_engine import DQEngine

logger = logging.getLogger(__name__)

_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class EngineRegistry(GenericRegistry["DQEngine"]):
    """Registry for discovering and instantiating DQEngine subclasses.

    Inherits thread-safe registration and lookup from GenericRegistry.
    Adds convention-based discovery and entry point loading.
    """

    _explicit: Dict[str, Type["DQEngine"]] = {}

    @classmethod
    def _get_registry(cls):
        """Return _explicit for backward compatibility.

        The explicit registry contains manually registered engines.
        Convention and entry point discovery are checked separately.
        """
        return cls._explicit

    @classmethod
    def register(cls, name: str, engine_class: Type["DQEngine"]) -> None:
        """Explicitly register an engine class.

        Args:
            name: Engine name (lowercase alphanumeric).
            engine_class: DQEngine subclass.
        """
        super().register(name, engine_class)

    @classmethod
    def get_engine_class(cls, name: str) -> Type["DQEngine"]:
        """Look up an engine class by name.

        Tries in order:
        1. Explicit registry
        2. Convention-based import (``dq.engine.{name}.{name}_engine``)
        3. Entry points (``dq.engines`` group)

        Args:
            name: Engine name.

        Returns:
            DQEngine subclass.

        Raises:
            ValueError: If name is invalid.
            ImportError: If engine cannot be found.
        """
        name = name.lower()

        if not _ENGINE_NAME_PATTERN.match(name):
            raise ValueError(
                f"Invalid engine name '{name}': must be lowercase "
                "alphanumeric with optional underscores."
            )

        # 1. Explicit registry
        if cls.is_registered(name):
            logger.debug("Found engine '%s' in explicit registry", name)
            return super().get(name)

        # 2. Convention-based discovery
        engine_class = cls._try_convention_import(name)
        if engine_class is not None:
            return engine_class

        # 3. Entry points
        engine_class = cls._try_entry_points(name)
        if engine_class is not None:
            return engine_class

        raise ImportError(
            f"Engine '{name}' not found via registry, convention import, or entry points."
        )

    @classmethod
    def _try_convention_import(cls, name: str) -> Optional[Type["DQEngine"]]:
        """Try to import engine via convention: dq.engine.{name}.{name}_engine.

        The class name follows the convention: remove underscores, capitalize each word,
        and append 'Engine'. Handles compound words correctly (e.g., "schema_validation"
        -> "SchemaValidationEngine", not "SchemaValidationEngine").

        Examples:
        - "my_engine" -> "MyEngineEngine"
        - "schema_validation" -> "SchemaValidationEngine"
        - "great_expectations" -> "GreatExpectationsEngine"

        Validates that the discovered class is actually a DQEngine subclass
        and has the required 'apply' method.
        """
        module_path = f"dq.engine.{name}.{name}_engine"

        # Handle compound words: split on underscore, capitalize each part
        if "_" in name:
            # "schema_validation" -> ["Schema", "Validation"] -> "SchemaValidationEngine"
            class_name = "".join(word.capitalize() for word in name.split("_")) + "Engine"
        else:
            # Handle compound words without underscores using common patterns
            # "schemavalidation" -> "SchemaValidationEngine"
            # "greatexpectations" -> "GreatExpectationsEngine"
            from dq.utils.string_utils import split_compound_word
            words = split_compound_word(name)
            class_name = "".join(word.capitalize() for word in words) + "Engine"

        try:
            module = importlib.import_module(module_path)
            engine_class = getattr(module, class_name)

            # Validate it's actually a class
            if not isinstance(engine_class, type):
                raise ImportError(
                    f"Engine '{name}' found '{class_name}' but it is not a class "
                    f"(type: {type(engine_class).__name__})"
                )

            # Validate it inherits from DQEngine (runtime import to avoid circular)
            from dq.engine.dq_engine import DQEngine

            if not issubclass(engine_class, DQEngine):
                raise ImportError(
                    f"Engine '{name}' found '{class_name}' but it does not inherit from DQEngine. "
                    f"Found bases: {engine_class.__bases__}"
                )

            # Validate it has the required 'apply' method
            if not hasattr(engine_class, "apply"):
                raise ImportError(
                    f"Engine '{name}' found '{class_name}' but it is missing the required 'apply' method"
                )

            logger.debug("Loaded engine '%s' from %s", class_name, module_path)
            return engine_class
        except ImportError as e:
            # ImportErrors raised during validation - log as warning
            logger.warning("Convention import failed for '%s': %s", name, e)
            return None
        except (ModuleNotFoundError, AttributeError) as e:
            # Module not found or attribute error - expected during discovery
            logger.debug("Convention import failed for '%s': %s", name, e)
            return None

    @classmethod
    def _try_entry_points(cls, name: str) -> Optional[Type["DQEngine"]]:
        """Try to find engine via importlib.metadata entry points.

        Validates that the discovered class is a valid DQEngine subclass.
        """
        try:
            from importlib.metadata import entry_points

            eps = entry_points()
            # Python 3.9+ compatibility
            if hasattr(eps, "select"):
                dq_eps = eps.select(group="dq.engines")
            else:
                dq_eps = eps.get("dq.engines", [])

            for ep in dq_eps:
                if ep.name == name:
                    engine_class = ep.load()

                    # Validate it's actually a class
                    if not isinstance(engine_class, type):
                        logger.warning(
                            "Entry point '%s' returned non-class type: %s",
                            name,
                            type(engine_class).__name__,
                        )
                        return None

                    # Validate it inherits from DQEngine
                    from dq.engine.dq_engine import DQEngine

                    if not issubclass(engine_class, DQEngine):
                        logger.warning(
                            "Entry point '%s' has class that does not inherit from DQEngine: %s",
                            name,
                            engine_class.__name__,
                        )
                        return None

                    # Validate it has the required 'apply' method
                    if not hasattr(engine_class, "apply"):
                        logger.warning(
                            "Entry point '%s' has class missing required 'apply' method: %s",
                            name,
                            engine_class.__name__,
                        )
                        return None

                    logger.debug("Loaded engine '%s' from entry point", name)
                    return engine_class
        except Exception as e:
            logger.debug("Entry point lookup failed for '%s': %s", name, e)
        return None

    @classmethod
    def list_engines(cls) -> Dict[str, str]:
        """List all explicitly registered engines.

        Returns:
            Dict mapping engine names to class names.
        """
        return {name: klass.__name__ for name, klass in cls._explicit.items()}

    # Note: unregister inherited from GenericRegistry and works with _explicit
