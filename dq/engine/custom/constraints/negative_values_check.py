# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""WideTablesNegativeValuesCheck constraint — finds negative values across wide tables."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from dq.engine.custom.constraint_registry import CustomConstraint

logger = logging.getLogger(__name__)


class WideTablesNegativeValuesCheck(CustomConstraint):
    """Check for negative values across all numeric columns in a wide table."""

    def evaluate(
        self, dataframe: DataFrame, config: dict, spark_session
    ) -> Tuple[List[list], List[list]]:
        from pyspark.sql import functions as F

        dq_dimension = config.get("dq_dimension", "Compliance")
        constraint = config.get("constraint", "WideTablesNegativeValuesCheck")
        level = config.get("level", "Warning")
        ignore_columns = config.get("ignore_columns", None)
        source = config.get("source", None)

        logger.debug("Running WideTablesNegativeValuesCheck constraint")

        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)

        columns_to_check = dataframe.columns
        negative_counts = (
            dataframe.select(
                [(F.sum((F.col(c) < 0).cast("int"))).alias(c) for c in columns_to_check]
            )
            .collect()[0]
            .asDict()
        )

        if source == "timeSeries":
            source = ""

        metric_results = []
        check_verifications = []

        for column in dataframe.columns:
            if negative_counts[column] is not None:
                if (isinstance(negative_counts[column], int)) and (
                    negative_counts[column] > 0
                ):
                    metric_results.append(
                        [
                            "MultiColumn",
                            f"{constraint} for {column} {source}",
                            dq_dimension,
                            0,
                        ]
                    )
                    check_verifications.append(
                        [
                            constraint,
                            level,
                            "Failure",
                            f"{constraint}  for {column} {source}",
                            "Failure",
                            f"Negative values found for {column} {source}",
                        ]
                    )
                else:
                    metric_results.append(
                        [
                            "MultiColumn",
                            f"{constraint} for {column} {source}",
                            dq_dimension,
                            1,
                        ]
                    )
                    check_verifications.append(
                        [
                            constraint,
                            level,
                            "Success",
                            f"{constraint}  for {column} {source}",
                            "Success",
                            f"Rule NoNegative values passed for {column} {source}",
                        ]
                    )
            else:
                metric_results.append(
                    [
                        "MultiColumn",
                        f"{constraint} for {column} {source}",
                        dq_dimension,
                        1,
                    ]
                )
                check_verifications.append(
                    [
                        constraint,
                        level,
                        "Success",
                        f"{constraint}  for {column} {source}",
                        "Success",
                        f"Rule NoNegative values passed for {column} {source}",
                    ]
                )

        return metric_results, check_verifications
