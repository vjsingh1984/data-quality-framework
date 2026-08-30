# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from dq.engine.constraint.constraint_engine import ConstraintEngine
from dq.engine.engine_registry import EngineRegistry

# Register ConstraintEngine with the "constraint" engine name
EngineRegistry.register("constraint", ConstraintEngine)

__all__ = ["ConstraintEngine", "EngineRegistry"]
