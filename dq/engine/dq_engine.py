# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dq.repository.repository_writer import RepositoryWriter


@dataclass
class DQMetric:
    """Normalized metric returned by all data quality engines.

    Provides a consistent schema for metrics across all engines,
    making it easier for consumers to process results.

    Attributes:
        check: Name of the check/constraint that was evaluated.
        constraint: Specific constraint type (e.g., "Completeness", "RangeCheck").
        success: Whether the check passed (True) or failed (False).
        engine: Name of the engine that produced this metric.
        timestamp_ms: Epoch milliseconds when the metric was produced.
        dataset: Name of the dataset/table that was validated.
        details: Engine-specific details as a dict.
        execution_time_ms: Optional execution time in milliseconds.
        assertion: Optional assertion message describing the check.
    """

    check: str
    constraint: str
    success: bool
    engine: str
    timestamp_ms: int
    dataset: str
    details: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: Optional[int] = None
    assertion: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility."""
        return asdict(self)

    @classmethod
    def time_ms(cls) -> int:
        """Get current time in milliseconds since epoch."""
        import time

        return int(time.time() * 1000)


class DQEngine(ABC):
    """Abstract base class for all data quality engines.

    Engines implement validation logic by overriding the ``apply`` method.
    They are loaded dynamically by ``EngineLoader`` based on the engine name
    in the HOCON configuration.
    """

    def __init__(
        self,
        config: ConfigTree,
        repository_writer: Optional[RepositoryWriter] = None,
    ):
        self._config = config
        self._cache: dict[
            str, Any
        ] = {}  # Cache for engine-specific data (DFs, temp views, etc.)
        if repository_writer is not None:
            self._repository_writer = repository_writer
        else:
            from dq.repository.repository_writer import NoOpRepositoryWriter

            self._repository_writer = NoOpRepositoryWriter()

        # Validate configuration at init time (fail-fast)
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration at init time.

        Subclasses can override this method to perform engine-specific
        validation. This is called at the end of __init__ to provide
        fail-fast behavior for configuration errors.

        Raises:
            ConfigurationError: If the configuration is invalid.
        """
        pass

    def _get_engine_name(self) -> str:
        """Get the engine name for metrics.

        Subclasses can override to customize the engine name in metrics.
        """
        return self.__class__.__name__.replace("Engine", "").lower()

    def _get_dataset_name(self) -> str:
        """Get the dataset name from config.

        Returns the dataset name if configured, otherwise 'unknown'.
        """
        from dq.utils import constants

        return self._config.get(constants.DQ_DATASET, "unknown")

    def _create_metric(
        self,
        check: str,
        success: bool,
        details: Dict[str, Any],
        constraint: Optional[str] = None,
        execution_time_ms: Optional[int] = None,
        assertion: Optional[str] = None,
    ) -> DQMetric:
        """Create a normalized metric with common fields populated.

        Args:
            check: Name of the check.
            success: Whether the check passed.
            details: Engine-specific details.
            constraint: Optional constraint type (defaults to check name).
            execution_time_ms: Optional execution time.
            assertion: Optional assertion message.

        Returns:
            DQMetric with engine, timestamp, and dataset populated.
        """
        return DQMetric(
            check=check,
            constraint=constraint or check,
            success=success,
            engine=self._get_engine_name(),
            timestamp_ms=DQMetric.time_ms(),
            dataset=self._get_dataset_name(),
            details=details,
            execution_time_ms=execution_time_ms,
            assertion=assertion,
        )

    def before_apply(self, dataframe: DataFrame) -> None:
        """Hook called before apply() executes.

        Subclasses can override this to perform setup logic such as:
        - Caching reference DataFrames
        - Creating temporary views
        - Initializing resources

        Args:
            dataframe: The DataFrame that will be validated.
        """
        pass

    def after_apply(self, dataframe: DataFrame, metrics: List[Dict[str, Any]]) -> None:
        """Hook called after apply() completes (even if it fails).

        Subclasses can override this to perform cleanup logic such as:
        - Unpersisting cached DataFrames
        - Dropping temporary views
        - Releasing resources

        Args:
            dataframe: The DataFrame that was validated.
            metrics: The metrics returned by apply() (may be empty if apply failed).
        """
        pass

    @abstractmethod
    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply data quality checks to the given DataFrame.

        Implementations are encouraged to call the lifecycle hooks:
        - ``self.before_apply(dataframe)`` before processing
        - ``self.after_apply(dataframe, metrics)`` after processing

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository configuration for persisting results.
                Deprecated: use ``repository_writer`` in constructor instead.

        Returns:
            List of metric dictionaries with ``check``, ``success``,
            and ``details`` keys.
        """
        raise NotImplementedError("Subclasses must implement this method")
