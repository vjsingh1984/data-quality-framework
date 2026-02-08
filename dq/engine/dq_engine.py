# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from dq.repository.repository_writer import RepositoryWriter


class DQEngine(ABC):
    """Abstract base class for all data quality engines.

    Engines implement validation logic by overriding the ``apply`` method.
    They are loaded dynamically by ``EngineLoader`` based on the engine name
    in the HOCON configuration.
    """

    def __init__(
        self,
        config: ConfigTree,
        dqts: Optional[int] = None,
        repository_writer: Optional[RepositoryWriter] = None,
    ):
        self._config = config
        self._dqts = dqts
        self._cache = {}  # Cache for engine-specific data (DFs, temp views, etc.)
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
