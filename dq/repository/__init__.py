# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Repository abstractions for persisting DQ metrics and verifications."""
from dq.repository.repository_writer import (
    NoOpRepositoryWriter,
    RepositoryWriter,
    SparkRepositoryWriter,
)

__all__ = ["RepositoryWriter", "SparkRepositoryWriter", "NoOpRepositoryWriter"]
