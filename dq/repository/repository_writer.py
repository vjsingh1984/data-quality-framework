# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Repository writer abstractions for persisting DQ results."""
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class RepositoryWriter(ABC):
    """Abstract base class for persisting DQ metrics and verifications."""

    @abstractmethod
    def save_metrics(self, df, dataset: str, timestamp_ms: int) -> None:
        """Save metrics DataFrame to repository.

        Args:
            df: Spark DataFrame of metrics.
            dataset: Dataset name for partitioning.
            timestamp_ms: Timestamp in milliseconds.
        """

    @abstractmethod
    def save_verifications(self, df, dataset: str, timestamp_ms: int) -> None:
        """Save verification results DataFrame to repository.

        Args:
            df: Spark DataFrame of verification results.
            dataset: Dataset name for partitioning.
            timestamp_ms: Timestamp in milliseconds.
        """

    def save_both(self, metrics_df, verifications_df, timestamp_ms: int) -> None:
        """Convenience method to save both metrics and verifications.

        Args:
            metrics_df: Spark DataFrame of metrics.
            verifications_df: Spark DataFrame of verifications.
            timestamp_ms: Timestamp in milliseconds.
        """
        self.save_metrics(metrics_df, "", timestamp_ms)
        self.save_verifications(verifications_df, "", timestamp_ms)


class SparkRepositoryWriter(RepositoryWriter):
    """Writes DQ results using the existing repository_utils."""

    def __init__(self, repo_config):
        self._repo_config = repo_config

    def save_metrics(self, df, dataset: str, timestamp_ms: int) -> None:
        from dq.utils import constants, repository_utils

        repository_utils.save_to_repository(
            self._repo_config, df, constants.DQ_REPOSITORY_METRICS, timestamp_ms
        )

    def save_verifications(self, df, dataset: str, timestamp_ms: int) -> None:
        from dq.utils import constants, repository_utils

        repository_utils.save_to_repository(
            self._repo_config, df, constants.DQ_REPOSITORY_VERIFICATIONS, timestamp_ms
        )


class NoOpRepositoryWriter(RepositoryWriter):
    """No-op writer used when no repository is configured."""

    def save_metrics(self, df, dataset: str, timestamp_ms: int) -> None:
        logger.debug("No repository configured; skipping metrics save.")

    def save_verifications(self, df, dataset: str, timestamp_ms: int) -> None:
        logger.debug("No repository configured; skipping verifications save.")
