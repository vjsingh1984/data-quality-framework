# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import os


def save_to_repository(
    repoconfig, df, metric_type_suffix, resultkey_current_time_to_millis
):
    """
    Saves the repository configuration to a file.

    Args:
        repoconfig (ConfigTree): The configuration object containing the repository settings.
        df (pyspark.DataFrame): The DataFrame to be saved.
        metric_type_suffix (str) : The metric type suffix to be added to the file name.
        resultkey_current_time_to_millis (long): result key to tie verifications and metrics executed together

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
        F.from_unixtime(F.lit(resultkey_current_time_to_millis / 1000))
    )
    nextdf = (
        df.withColumn("dqts", F.lit(resultkey_current_time_to_millis))
        .withColumn("dataset", F.lit(partition_dataset))
        .withColumn("year", F.lit(partition_year))
    )

    if file_repo_config:
        paths = file_repo_config.get("paths", [])
        if paths:
            for path in paths:
                # Save to file
                nextdf.coalesce(1).write.mode("append").format(format).partitionBy(
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
                    tablewithsuffix = table + "_" + metric_type_suffix
                    parts = tablewithsuffix.split(".")
                    if len(parts) != 2:
                        raise ValueError(
                            f"Table reference '{tablewithsuffix}' must be in "
                            f"'database.table' format, got {len(parts)} part(s)."
                        )
                    dbname, tabname = parts
                    table_exists_already = (
                        df.sparkSession._jsparkSession.catalog().tableExists(
                            dbname, tabname
                        )
                    )
                    if table_exists_already:
                        nextdf.coalesce(1).write.mode("append").format(format).option(
                            "mergeSchema", "true"
                        ).insertInto(tablewithsuffix)
                    else:
                        nextdf.coalesce(1).write.mode("overwrite").format(
                            format
                        ).saveAsTable(tablewithsuffix)
