# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os


def save_to_repository(
    repoconfig, df, metric_type_suffix, result_timestamp_ms
):
    """
    Saves DataFrame to configured repository (file system or catalog).

    Args:
        repoconfig (ConfigTree): The configuration object containing repository settings.
        df (pyspark.DataFrame): The DataFrame to be saved.
        metric_type_suffix (str): The metric type suffix to be added to the file/table name.
        result_timestamp_ms (int): Result timestamp to tie verifications and metrics together.

    Returns:
        None
    """
    from pyspark.sql import functions as F

    partition_dataset = repoconfig.get("dataset", None)
    if partition_dataset is None:
        raise ValueError("Dataset name is not provided in the configuration.")

    format = repoconfig.get("format", "delta")
    file_repo_config = repoconfig.get("file", {})

    if format not in ["parquet", "csv", "json", "delta", "orc"]:
        raise ValueError("Invalid format specified in the configuration.")

    partition_year = F.year(
        F.from_unixtime(F.lit(result_timestamp_ms / 1000))
    )
    enriched_df = (
        df.withColumn("dqts", F.lit(result_timestamp_ms))
        .withColumn("dataset", F.lit(partition_dataset))
        .withColumn("year", F.lit(partition_year))
    )

    if file_repo_config:
        paths = file_repo_config.get("paths", [])
        if paths:
            for path in paths:
                # Save to file
                enriched_df.coalesce(1).write.mode("append").format(format).partitionBy(
                    "dataset", "year"
                ).save(os.path.join(path, metric_type_suffix))

    catalog_repo_config = repoconfig.get("catalog", {})
    if catalog_repo_config:
        # Save to catalog
        tables_config = catalog_repo_config.get("tables", [])
        if tables_config:
            # Check if table already exists
            for table in tables_config:
                if table is None:
                    raise ValueError("Table name is not provided in the configuration.")
                else:
                    table_with_suffix = table + "_" + metric_type_suffix
                    parts = table_with_suffix.split(".")
                    if len(parts) != 2:
                        raise ValueError(
                            f"Table reference '{table_with_suffix}' must be in "
                            f"'database.table' format, got {len(parts)} part(s)."
                        )
                    dbname, tabname = parts
                    table_exists = (
                        df.sparkSession._jsparkSession.catalog().tableExists(
                            dbname, tabname
                        )
                    )
                    if table_exists:
                        enriched_df.coalesce(1).write.mode("append").format(format).option(
                            "mergeSchema", "true"
                        ).insertInto(table_with_suffix)
                    else:
                        enriched_df.coalesce(1).write.mode("overwrite").format(
                            format
                        ).saveAsTable(table_with_suffix)
