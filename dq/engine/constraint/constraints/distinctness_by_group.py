# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""DistinctnessByGroup constraint — validates distinct counts within groups."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from dq.engine.constraint.constraint_registry import CustomConstraint

logger = logging.getLogger(__name__)


def _check_min_max_threshold(
    check_verifications,
    metric_results,
    column,
    dq_dimension,
    constraint,
    value,
    group_by,
    level,
    max_value,
    min_value,
):
    """Check value against min/max thresholds and append results."""
    if min_value and value < min_value:
        metric_results.append(
            ["MultiColumn", f"{constraint} {group_by} for {column}", dq_dimension, 0]
        )
        check_verifications.append(
            [
                constraint,
                level,
                "Error",
                f"{constraint} {group_by} for {column}",
                "Failure",
                f"{value} is below the threshold - {min_value}",
            ]
        )
    elif max_value and value > max_value:
        metric_results.append(
            ["MultiColumn", f"{constraint} {group_by} for {column}", dq_dimension, 0]
        )
        check_verifications.append(
            [
                constraint,
                level,
                "Error",
                f"{constraint} {group_by} for {column}",
                "Failure",
                f"{value} is above the threshold - {max_value}",
            ]
        )
    else:
        metric_results.append(
            ["MultiColumn", f"{constraint} {group_by} for {column}", dq_dimension, 1]
        )
        check_verifications.append(
            [
                constraint,
                level,
                "Success",
                f"{constraint} {group_by} for {column}",
                "Success",
                f"{value} meets the threshold",
            ]
        )


class DistinctnessByGroup(CustomConstraint):
    """Validate distinct counts of columns within groups meet thresholds."""

    def evaluate(
        self, dataframe: DataFrame, config: dict, spark_session
    ) -> Tuple[List[list], List[list]]:
        from pyspark.sql import functions as F

        columns = config.get("columns", None)
        group_by = config.get("group_by", None)
        dq_dimension = config.get("dq_dimension", "Compliance")
        constraint = config.get("constraint", "DistinctnessByGroup")
        min_value = config.get("min", None)
        max_value = config.get("max", None)
        level = config.get("level", "Warning")

        logger.info(
            "Checking distinctness of columns '%s' by group '%s'",
            columns,
            group_by,
        )
        funcs = [F.countDistinct]
        exprs = [f(F.col(c)).alias(c) for f in funcs for c in columns]
        group_df = dataframe.groupBy(*group_by).agg(*exprs)
        data_collect = group_df.collect()

        metric_results = []
        check_verifications = []

        for row in data_collect:
            for column in columns:
                distinct_count = row[column]
                _check_min_max_threshold(
                    check_verifications,
                    metric_results,
                    column,
                    dq_dimension,
                    constraint,
                    distinct_count,
                    group_by,
                    level,
                    max_value,
                    min_value,
                )

        if len(metric_results) == 0:
            metric_results.append(
                [
                    "MultiColumn",
                    f"{constraint} {group_by} for {','.join(columns)}",
                    dq_dimension,
                    1,
                ]
            )
            check_verifications.append(
                [
                    constraint,
                    level,
                    "Success",
                    f"{constraint} {group_by} for {','.join(columns)}",
                    "Success",
                    "No suitable data to validate this rule",
                ]
            )

        return metric_results, check_verifications
