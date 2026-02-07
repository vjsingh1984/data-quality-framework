# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""RateOfChange constraint — detects sudden value changes between consecutive rows."""
import logging
from typing import List, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from dq.engine.custom.constraint_registry import CustomConstraint
from dq.engine.custom.constraints.distinctness_by_group import _check_min_max_threshold

logger = logging.getLogger(__name__)

# Maximum rows to collect to avoid OOM
_MAX_COLLECT_ROWS = 100_000


class RateOfChange(CustomConstraint):
    """Detect sudden rate-of-change spikes between consecutive rows."""

    def evaluate(
        self, dataframe: DataFrame, config: dict, spark_session
    ) -> Tuple[List[list], List[list]]:
        columns = config.get("columns", None)
        group_by = config.get("group_by", None)
        dq_dimension = config.get("dq_dimension", "Compliance")
        constraint = config.get("constraint", "RateOfChange")
        min_val = config.get("min", None)
        max_val = config.get("max", None)
        level = config.get("level", "Warning")
        sort_by = config.get("sort_by", None)
        max_rows = config.get("max_rows", _MAX_COLLECT_ROWS)

        logger.info(
            "Checking rate of change for '%s' by group '%s' and sort by '%s'",
            columns,
            group_by,
            sort_by,
        )
        window = (
            Window.partitionBy(*group_by).orderBy(F.col(sort_by)).rowsBetween(-1, 0)
        )
        for column in columns:
            dataframe = dataframe.withColumn(
                f"{column}_list", F.collect_list(column).over(window)
            )
        logger.debug(
            "Updated dataframe contains %d rows and %d columns",
            dataframe.count(),
            len(dataframe.columns),
        )

        # Limit rows collected to avoid OOM
        df_final = dataframe.limit(max_rows).collect()

        metric_results = []
        check_verifications = []

        for row in df_final:
            for col in columns:
                value_list = row[f"{col}_list"]
                if value_list and len(value_list) == 2:
                    # Guard against division by zero
                    if value_list[0] == 0:
                        change_percentile = float("inf") if value_list[1] != 0 else 0.0
                    else:
                        change_percentile = abs(
                            ((value_list[0] - value_list[1]) / value_list[0]) * 100
                        )
                    logger.debug(
                        "%s - value list is %s and chg %% is %s",
                        col,
                        value_list,
                        change_percentile,
                    )
                    _check_min_max_threshold(
                        check_verifications,
                        metric_results,
                        col,
                        dq_dimension,
                        constraint,
                        change_percentile,
                        group_by,
                        level,
                        max_val,
                        min_val,
                    )

        if len(metric_results) == 0:
            metric_results.append(
                [
                    "MultiColumn",
                    f"{constraint} {group_by} for {columns}",
                    dq_dimension,
                    0,
                ]
            )
            check_verifications.append(
                [
                    constraint,
                    level,
                    "Success",
                    f"{constraint} {group_by} for {columns}",
                    "Success",
                    "No suitable data to validate this rule",
                ]
            )

        return metric_results, check_verifications
