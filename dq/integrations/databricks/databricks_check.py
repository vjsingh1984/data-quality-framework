# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime
from time import time

from cstriggers.core.trigger import QuartzCron
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState, RunType
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DatabricksCheck:
    """Monitors Databricks job timeliness and invocation metrics.

    Connects to a Databricks workspace to inspect scheduled job runs
    and compare actual vs expected invocations.
    """

    def __init__(self, databricks_token, databricks_url):
        self._databricks_token = databricks_token
        self._databricks_url = databricks_url

    def check_scheduled_jobs_timeliness(
        self, spark, domain, jobs_to_monitor, job_created_by, last_run
    ):
        """Check whether scheduled Databricks jobs ran on time.

        Args:
            spark: Active SparkSession.
            domain: Domain label used for metric table naming.
            jobs_to_monitor: List of job names to check.
            job_created_by: Username filter for job ownership.
            last_run: Epoch timestamp (milliseconds) marking the lookback start.

        Returns:
            None. Results are written to a Delta table.
        """
        last_run_epoch = last_run / 1000
        start_datetime = datetime.fromtimestamp(last_run_epoch)

        w = WorkspaceClient(host=self._databricks_url, token=self._databricks_token)

        timeline_metrics = []

        dq_metrics_table = f"{domain}_dq.dq_metrics"
        end_date = datetime.now()
        logger.info("Monitoring jobs: %s", jobs_to_monitor)
        for domain_job_name in jobs_to_monitor:
            logger.debug("Checking job: %s", domain_job_name)
            jobs_iterator = w.jobs.list(name=domain_job_name)
            for domain_job_def in jobs_iterator:
                if domain_job_def.creator_user_name == job_created_by:
                    job_id = domain_job_def.job_id
                    logger.info(
                        "Job name: %s, schedule: %s",
                        domain_job_name,
                        domain_job_def.settings.schedule.quartz_cron_expression,
                    )
                    quartz_cron = QuartzCron(
                        schedule_string=domain_job_def.settings.schedule.quartz_cron_expression,
                        start_date=start_datetime,
                        end_date=end_date,
                    )
                    trigger_iterator = quartz_cron.next_triggers(100, isoformat=True)
                    expected_runs = len(list(trigger_iterator))

                    run_list = w.jobs.list_runs(
                        job_id=job_id,
                        start_time_from=last_run,
                        run_type=RunType.JOB_RUN,
                    )
                    actual_runs = 0
                    for run in run_list:
                        timeline_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, run id : {run.run_id}",
                                "Timeliness.IngestionTime",
                                run.run_duration,
                            ]
                        )
                        invocation_value = 0
                        if RunResultState.SUCCESS == run.state.result_state:
                            invocation_value = 1
                        timeline_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, run id : {run.run_id}",
                                "Timeliness.Invocations",
                                invocation_value,
                            ]
                        )
                        actual_runs += 1
                        logger.debug(
                            "%s duration: %s exec: %s run_id: %s success: %s",
                            run.run_name,
                            run.run_duration,
                            run.execution_duration,
                            run.run_id,
                            RunResultState.SUCCESS == run.state.result_state,
                        )
                    if expected_runs > actual_runs:
                        timeline_metrics.append(
                            [
                                domain_job_name,
                                f"Job id: {job_id}, expected to run {expected_runs} but actual run was {actual_runs}",
                                "Timeliness.MissedInvocations",
                                (actual_runs - expected_runs),
                            ]
                        )

        if len(timeline_metrics) > 0:
            metrics_dataframe = spark.createDataFrame(
                timeline_metrics, ["entity", "instance", "name", "value"]
            )
            current_time_in_millis = time() * 1000
            partition_year = F.year(F.from_unixtime(F.lit(time())))
            metrics_dataframe = (
                metrics_dataframe.withColumn("dqts", F.lit(current_time_in_millis))
                .withColumn("dataset", F.lit(domain))
                .withColumn("year", F.lit(partition_year))
            )
            table_exists = spark.catalog.tableExists(dq_metrics_table)

            if table_exists:
                metrics_dataframe.coalesce(1).write.mode("append").format(
                    "delta"
                ).option("mergeSchema", "true").insertInto(dq_metrics_table)
            else:
                metrics_dataframe.coalesce(1).write.mode("overwrite").format(
                    "delta"
                ).saveAsTable(dq_metrics_table)
