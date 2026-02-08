# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.engine.schemavalidation.schemavalidation_check import SchemavalidationCheck
from dq.utils import constants, repository_utils

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SchemavalidationEngine(DQEngine):
    """Engine that validates DataFrame schemas against expected definitions.

    Validates datatype, nullable, unique, and foreign-key constraints
    using Spark schema introspection and PyDeequ's VerificationSuite
    (via composition, not inheritance).
    """

    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        super().__init__(config, dqts)
        self._sparkSession = None

    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Run schema validation checks against the DataFrame.

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

        self._sparkSession = dataframe.sparkSession
        success_metrics, check_verifications = self._run_verification(dataframe)

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
            summary_metrics.append(
                {
                    "check": check["check"],
                    "success": check["check_status"] == "Success",
                    "details": check,
                }
            )
        return summary_metrics

    def _run_verification(self, df):
        """Run PyDeequ VerificationSuite on the DataFrame.

        Args:
            df: Spark DataFrame to validate.

        Returns:
            Tuple of (successMetrics DataFrame, checkVerifications DataFrame).
        """
        from pydeequ.verification import VerificationResult, VerificationSuite

        single_check_mode = self._config.get(constants.DQ_SINGLE_CHECK_MODE, True)
        schema_validation_check = SchemavalidationCheck(
            schema_config=self._config.get(constants.SCHEMA_VALIDATION_SCHEMA, {}),
            single_check_mode=single_check_mode,
        )

        checks = schema_validation_check.apply_checks(df)
        joined_df = (
            schema_validation_check.get_joined_dataframe_from_foreign_key_constraints(
                df
            )
        )

        if single_check_mode:
            verification_result = (
                VerificationSuite(self._sparkSession)
                .onData(joined_df)
                .addCheck(checks)
                .run()
            )
        else:
            verification_run_builder = VerificationSuite(self._sparkSession).onData(
                joined_df
            )
            for check in checks:
                verification_run_builder = verification_run_builder.addCheck(check)
            verification_result = verification_run_builder.run()

        if verification_result.status == "Success":
            logger.info("Schema validation succeeded.")
        else:
            logger.warning("Schema validation failed.")

        success_metrics = VerificationResult.successMetricsAsDataFrame(
            self._sparkSession, verification_result
        )
        check_verifications = VerificationResult.checkResultsAsDataFrame(
            self._sparkSession, verification_result
        )
        return success_metrics, check_verifications
