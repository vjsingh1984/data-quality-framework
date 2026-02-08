# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

from dq.engine.deequ.deequ_check import DeequCheck
from dq.engine.dq_engine import DQEngine
from dq.exceptions import ConfigurationError
from dq.utils import constants, repository_utils

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DeequEngine(DQEngine):
    """Engine that validates DataFrames using Amazon Deequ.

    Wraps PyDeequ's ``VerificationSuite`` to run constraint checks
    defined in the HOCON configuration.  Supports both single-check
    mode (all constraints in one ``Check``) and multi-check mode
    (one ``Check`` per constraint).
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._sparkSession = None
        super().__init__(config, dqts)

    def _validate_config(self) -> None:
        """Validate Deequ configuration at init time."""
        checks = self._config.get("checks", [])
        if not checks:
            raise ConfigurationError(
                "DeequEngine requires 'checks' in configuration with at least one check."
            )

        for i, check in enumerate(checks):
            try:
                constraint = check.get("constraint")
            except Exception:
                constraint = None

            if not constraint:
                raise ConfigurationError(
                    f"Check at index {i} is missing required 'constraint' key."
                )

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run Deequ verification checks against the DataFrame.

        Args:
            dataframe: Spark DataFrame to validate.
            repository: Optional repository config for persisting metrics.
                Deprecated: Use ``repository_writer`` in constructor instead.

        Returns:
            List of metric dicts with ``check``, ``success``, ``details`` keys.
        """
        if repository is not None:
            warnings.warn(
                "The 'repository' parameter is deprecated. "
                "Use 'repository_writer' in the engine constructor instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        rule_name = self._config.get(constants.DQ_RULE_NAME, "Unknown")
        engine_name = self._config.get(constants.DQ_ENGINE_NAME, "Unknown")
        logger.info("Processing %s with %s Engine", rule_name, engine_name)

        from pydeequ.verification import VerificationResult, VerificationSuite

        deequ_check = DeequCheck(
            checks_config=self._config.get("checks", []),
            single_check_mode=self._config.get(constants.DQ_SINGLE_CHECK_MODE, False),
        )
        self._sparkSession = dataframe.sparkSession

        verification_run_builder = VerificationSuite(
            spark_session=self._sparkSession
        ).onData(df=dataframe)

        verification_run_builder = deequ_check.apply_checks(
            verification_run_builder, self._sparkSession
        )

        verification_result = verification_run_builder.run()

        if verification_result.status == "SUCCESS":
            logger.info("Data quality checks passed successfully.")
        else:
            logger.warning("Data quality checks failed.")

        success_metrics = VerificationResult.successMetricsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )
        check_verifications = VerificationResult.checkResultsAsDataFrame(
            spark_session=self._sparkSession,
            verificationResult=verification_result,
        )

        if repository:
            from pydeequ.repository import ResultKey

            current_milli_time = ResultKey.current_milli_time()
            repository_utils.save_to_repository(
                repository,
                success_metrics,
                constants.DQ_REPOSITORY_METRICS,
                current_milli_time,
            )
            repository_utils.save_to_repository(
                repository,
                check_verifications,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_milli_time,
            )

        summary_metrics = []
        for check in check_verifications.collect():
            check_name = check["check"]
            # Extract constraint from check name (e.g., "Completeness for column_x" -> "Completeness")
            constraint = check_name.split(" ")[0] if " " in check_name else check_name

            metric = self._create_metric(
                check=check_name,
                success=check["check_status"] == "Success",
                details=check,
                constraint=constraint,
            )
            summary_metrics.append(metric.to_dict())

        return summary_metrics
