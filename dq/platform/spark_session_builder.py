# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Platform-aware SparkSession builder.

Auto-detects the runtime environment and applies appropriate defaults.
"""
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Environment variable -> platform name mapping
_PLATFORM_ENV_VARS = {
    "DATABRICKS_RUNTIME_VERSION": "databricks",
    "EMR_CLUSTER_ID": "emr",
    "AWS_GLUE_JOB_ID": "glue",
    "DATAPROC_VERSION": "dataproc",
    "FABRIC_WORKSPACE_ID": "fabric",
}


def detect_platform() -> str:
    """Detect the current runtime platform from environment variables.

    Returns:
        Platform name string: "databricks", "emr", "glue", "dataproc",
        "fabric", or "local".
    """
    for env_var, platform in _PLATFORM_ENV_VARS.items():
        if os.environ.get(env_var):
            logger.info("Detected platform: %s (via %s)", platform, env_var)
            return platform
    return "local"


def build_spark_session(
    app_name: str = "dq-framework",
    master: Optional[str] = None,
    platform: Optional[str] = None,
):
    """Build a SparkSession with platform-appropriate defaults.

    Args:
        app_name: Spark application name.
        master: Spark master URL. Defaults based on platform.
        platform: Override platform detection.

    Returns:
        SparkSession instance.
    """
    from pyspark.sql import SparkSession

    if platform is None:
        platform = detect_platform()

    builder = SparkSession.builder.appName(app_name)

    if platform == "local":
        builder = builder.master(master or "local[*]")
    elif master:
        builder = builder.master(master)

    logger.info("Building SparkSession for platform: %s", platform)
    return builder.getOrCreate()
