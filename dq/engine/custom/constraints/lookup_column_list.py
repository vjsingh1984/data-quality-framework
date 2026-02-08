# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""LookupColumnList constraint — checks column names against a reference table."""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from dq.engine.custom.constraint_registry import CustomConstraint

logger = logging.getLogger(__name__)

# Pattern for valid SQL identifiers (prevents SQL injection)
_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


class LookupColumnList(CustomConstraint):
    """Check if DataFrame column names are present as rows in a reference table."""

    def evaluate(
        self, dataframe: DataFrame, config: dict, spark_session
    ) -> Tuple[List[list], List[list]]:
        dq_dimension = config.get("dq_dimension", "Compliance")
        constraint = config.get("constraint", "LookupColumnList")
        ref_table = config.get("ref_table", None)
        ref_columns = config.get("ref_columns", None)
        level = config.get("level", "Warning")
        ignore_columns = config.get("ignore_columns", None)
        source = config.get("source", None)

        logger.debug("Running LookupColumnList constraint")

        if ignore_columns and len(ignore_columns) > 0:
            for col in ignore_columns:
                dataframe = dataframe.drop(col)

        # Validate identifiers to prevent SQL injection
        if ref_table and not _SQL_IDENTIFIER.match(ref_table):
            raise ValueError(f"Invalid reference table name: '{ref_table}'")
        if ref_columns and not _SQL_IDENTIFIER.match(ref_columns):
            raise ValueError(f"Invalid reference column name: '{ref_columns}'")

        reference_dataframe = spark_session.sql(
            f"SELECT {ref_columns} FROM {ref_table}"
        )
        column_name_list = reference_dataframe.rdd.flatMap(lambda x: x).collect()
        column_list = list(map(str, column_name_list))

        if source == "timeSeries":
            source = ""

        metric_results = []
        check_verifications = []

        for column in dataframe.columns:
            if column in column_list:
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
                        "Column found in the reference table",
                    ]
                )
            else:
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
                        "Column not found in the reference table",
                    ]
                )

        return metric_results, check_verifications
