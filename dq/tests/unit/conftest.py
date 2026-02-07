# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Conftest for unit tests — no Spark or Deequ dependencies required."""
import os

# Ensure SPARK_VERSION is set to avoid pydeequ import errors
os.environ.setdefault("SPARK_VERSION", "3.5")
