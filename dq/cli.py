# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface for Data Quality Framework."""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for dq-framework CLI."""
    parser = argparse.ArgumentParser(
        description="Data Quality Framework - Run data quality checks on Spark DataFrames",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  dq-framework config.conf
  dq-framework file://path/to/config.conf
  dq-framework s3://bucket/path/to/config.conf --spark-master spark://host:7077
        """,
    )
    parser.add_argument(
        "config",
        help="Path to HOCON configuration file (supports file://, s3://, abfss://, http://)",
    )
    parser.add_argument(
        "--spark-master",
        default=None,
        help="Spark master URL (default: auto-detect or local[*])",
    )
    parser.add_argument(
        "--app-name",
        default="dq-framework",
        help="Spark application name (default: dq-framework)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        from dq.dq_framework import DQFramework
        from dq.formatters import format_summary
        from dq.platform.spark_session_builder import build_spark_session

        logger.info("Initializing Spark session...")
        spark = build_spark_session(
            app_name=args.app_name,
            master=args.spark_master,
        )

        logger.info("Loading configuration from: %s", args.config)
        framework = DQFramework(spark, args.config)

        logger.info("Running data quality checks...")
        results = framework.run()

        print(format_summary(results))

        failed = sum(1 for r in results if not r.get("success", False))
        return 1 if failed > 0 else 0

    except ImportError as e:
        logger.error("Missing dependency: %s", e)
        logger.error(
            "Install Spark support with: pip install data-quality-framework[spark]"
        )
        return 1
    except Exception as e:
        logger.error("Error running data quality checks: %s", e)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
