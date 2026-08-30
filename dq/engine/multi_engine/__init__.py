# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Multi-engine execution framework.

Provides orchestration for running multiple data quality engines
with different execution strategies.

Usage::

    from dq.engine.multi_engine import MultiEngineOrchestrator, ExecutionStrategy

    orchestrator = MultiEngineOrchestrator(
        strategy=ExecutionStrategy.PARALLEL,
        max_workers=4
    )
    results = orchestrator.run(engines, dataframes)
"""

from dq.engine.multi_engine.execution_strategy import (
    BatchedExecutionStrategy,
    ExecutionStrategy,
    ParallelExecutionStrategy,
    SequentialExecutionStrategy,
)
from dq.engine.multi_engine.multi_engine_orchestrator import (
    EngineExecutionConfig,
    MultiEngineOrchestrator,
    MultiEngineResult,
)

__all__ = [
    "MultiEngineOrchestrator",
    "MultiEngineResult",
    "EngineExecutionConfig",
    "ExecutionStrategy",
    "SequentialExecutionStrategy",
    "ParallelExecutionStrategy",
    "BatchedExecutionStrategy",
]
