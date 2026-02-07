# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env python
"""Dynamic engine loader for the Data Quality Framework."""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from dq.engine.engine_registry import EngineRegistry

if TYPE_CHECKING:
    from dq.engine.dq_engine import DQEngine

logger = logging.getLogger(__name__)

# Engine name must be lowercase alphanumeric (no dots, slashes, etc.)
_ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


class EngineLoader:
    """Loads engine classes dynamically using the EngineRegistry.

    The registry supports three discovery methods:
    1. Explicit registration via ``EngineRegistry.register()``
    2. Convention-based: ``dq.engine.{name}.{name}_engine``
    3. Entry points: ``dq.engines`` group
    """

    def load_engine(self, engine_name: str, *args, **kwargs) -> DQEngine:
        """Dynamically load and instantiate a DQEngine subclass.

        Args:
            engine_name: The engine identifier (e.g., ``"deequ"``).
            *args: Positional arguments forwarded to the engine constructor.
            **kwargs: Keyword arguments forwarded to the engine constructor.

        Returns:
            An instantiated engine object.

        Raises:
            ImportError: If the engine module cannot be found.
            AttributeError: If the engine class cannot be found.
            ValueError: If the engine name contains invalid characters.
        """
        name = engine_name.lower()

        if not _ENGINE_NAME_PATTERN.match(name):
            raise ValueError(
                f"Invalid engine name '{engine_name}': must be lowercase "
                "alphanumeric with optional underscores."
            )

        try:
            engine_class = EngineRegistry.get_engine_class(name)
            logger.debug("Loaded engine %s", engine_class.__name__)
            return engine_class(*args, **kwargs)
        except ValueError:
            raise
        except ImportError as e:
            raise ImportError(f"Engine module for '{name}' not found: {e}") from e
