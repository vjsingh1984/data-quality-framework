# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from dq.engine.custom.custom_engine import ConstraintEngine
from dq.engine.engine_registry import EngineRegistry

# Explicitly register ConstraintEngine with the "custom" engine name
# This allows the engine to have a semantically meaningful class name
# while maintaining backward compatibility with existing configurations
EngineRegistry.register("custom", ConstraintEngine)

__all__ = ["ConstraintEngine", "EngineRegistry"]
