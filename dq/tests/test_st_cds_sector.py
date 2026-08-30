# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import json

import pytest

from dq.dq_framework import DQFramework


@pytest.fixture
def sample_domain_config():
    config_file = "file://./dq/tests/resources/sample_domain_validation.conf"
    return config_file


@pytest.mark.spark
def test_deequ_engine_success(spark, multi_column_dataframe, sample_domain_config):
    dqf = DQFramework(spark, sample_domain_config, multi_column_dataframe)
    cumulative_metrics = dqf.run()
    overall_success = True
    for metric in cumulative_metrics:
        print(json.dumps(metric))
        # assert metric['success'] == True, f"{metric} failed."
        if not (metric["success"]):
            print("Error in : " + json.dumps(metric))
            overall_success = False

    assert False == overall_success, "At least one metric failed."
