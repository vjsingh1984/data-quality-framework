# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List

from pyhocon import ConfigTree

from dq.engine.dq_engine import DQEngine
from dq.engine.schemavalidation.schemavalidation_check import SchemaValidationCheck
from dq.exceptions import ConfigurationError
from dq.utils import constants, repository_utils

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SchemaValidationEngine(DQEngine):
    """Engine that validates DataFrame schemas against expected definitions.

    Validates datatype, nullable, unique, and foreign-key constraints using:

    - **native backend**: Spark schema introspection (no PyDeequ required)
    - **deequ backend**: PyDeequ VerificationSuite (legacy, for advanced features)

    The backend is selected via the ``backend`` config option (default: "deequ").

    Usage::

        # Native mode (no PyDeequ required)
        dqframework {
            schema_validation {
                backend = "native"
                schema {
                    tables = [{
                        name = "my_table"
                        columns = [
                            { name = "id", type = "int", nullable = false, unique = true }
                            { name = "name", type = "string" }
                        ]
                    }]
                }
            }
        }

        # Deequ mode (default, requires PyDeequ)
        dqframework {
            schema_validation {
                backend = "deequ"
                # ... existing deequ config ...
            }
        }
    """

    def __init__(self, config: ConfigTree):
        super().__init__(config)
        self._spark_session = None

    def _validate_config(self) -> None:
        """Validate SchemaValidationEngine configuration at init time."""
        try:
            schema = self._config.get("schema")
        except Exception:
            schema = None

        if not schema:
            raise ConfigurationError(
                "SchemaValidationEngine requires 'schema' in configuration with "
                "table definitions including columns and constraints."
            )

        # Validate backend option
        backend = self._config.get("backend", "deequ")
        if backend not in ("native", "deequ"):
            raise ConfigurationError(
                f"Invalid backend '{backend}'. Must be 'native' or 'deequ'."
            )

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

        self._spark_session = dataframe.sparkSession

        # Choose backend based on config
        backend = self._config.get("backend", "deequ")

        if backend == "native":
            summary_metrics = self._run_native_validation(dataframe)
        else:
            summary_metrics = self._run_deequ_validation(dataframe, repository)

        return summary_metrics

    def _run_native_validation(self, df: DataFrame) -> List[Dict[str, Any]]:
        """Run native Spark validation (no PyDeequ required).

        Args:
            df: DataFrame to validate.

        Returns:
            List of metric dicts.
        """
        from dq.engine.dq_engine import DQMetric
        from dq.validation.schema_validator import NativeSchemaValidator

        schema_config = self._config.get(constants.SCHEMA, {})
        validator = NativeSchemaValidator(schema_config, self._spark_session)

        summary = validator.validate(df)

        # Convert to metric dicts
        timestamp_ms = DQMetric.time_ms()
        dataset = self._get_dataset_name()
        engine_name = self._get_engine_name()

        return summary.to_metric_dicts(engine_name, dataset, timestamp_ms)

    def _run_deequ_validation(
        self, df: DataFrame, repository=None
    ) -> List[Dict[str, Any]]:
        """Run PyDeequ VerificationSuite on the DataFrame (legacy backend).

        Args:
            df: DataFrame to validate.
            repository: Optional repository config for persisting metrics.

        Returns:
            List of metric dicts.
        """
        from pydeequ.verification import VerificationResult, VerificationSuite

        single_check_mode = self._config.get(constants.DQ_SINGLE_CHECK_MODE, True)
        schema_validation_check = SchemaValidationCheck(
            schema_config=self._config.get(constants.SCHEMA, {}),
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
                VerificationSuite(self._spark_session)
                .onData(joined_df)
                .addCheck(checks)
                .run()
            )
        else:
            verification_run_builder = VerificationSuite(self._spark_session).onData(
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
            self._spark_session, verification_result
        )
        check_verifications = VerificationResult.checkResultsAsDataFrame(
            self._spark_session, verification_result
        )

        if repository:
            from pydeequ.repository import ResultKey

            current_timestamp_ms = ResultKey.current_milli_time()
            repository_utils.save_to_repository(
                repository,
                success_metrics,
                constants.DQ_REPOSITORY_METRICS,
                current_timestamp_ms,
            )
            repository_utils.save_to_repository(
                repository,
                check_verifications,
                constants.DQ_REPOSITORY_VERIFICATIONS,
                current_timestamp_ms,
            )

        summary_metrics = []
        for check in check_verifications.collect():
            check_name = check["check"]
            # Extract constraint from check name
            constraint = check_name.split(" ")[0] if " " in check_name else check_name

            metric = self._create_metric(
                check=check_name,
                success=check["check_status"] == "Success",
                details=check,
                constraint=constraint,
            )
            summary_metrics.append(metric.to_dict())
        return summary_metrics
