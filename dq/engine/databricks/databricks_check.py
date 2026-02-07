# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Backward-compatible import shim.

DatabricksCheck has moved to ``dq.integrations.databricks.databricks_check``.
This module re-exports it for backward compatibility.
"""
from dq.integrations.databricks.databricks_check import DatabricksCheck  # noqa: F401
