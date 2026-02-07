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
        if repository_writer is not None:
            self._repository_writer = repository_writer
        else:
            from dq.repository.repository_writer import NoOpRepositoryWriter

            self._repository_writer = NoOpRepositoryWriter()

    @abstractmethod
    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply data quality checks to the given DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository configuration for persisting results.
                Deprecated: use ``repository_writer`` in constructor instead.

        Returns:
            List of metric dictionaries with ``check``, ``success``,
            and ``details`` keys.
        """
        raise NotImplementedError("Subclasses must implement this method")
