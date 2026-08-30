# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Data Quality Framework - Configuration-driven data quality for Apache Spark."""
from dq.result_models import DQResult

__all__ = ["DQFramework", "DQResult"]
__version__ = "2.0.0"


def __getattr__(name):
    if name == "DQFramework":
        from dq.dq_framework import DQFramework

        return DQFramework
    raise AttributeError(f"module 'dq' has no attribute {name!r}")
