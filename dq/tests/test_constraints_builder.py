# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0


# from deequ_constraints_builder import DeequConstraintsBuilder

import pytest

from dq.engine.deequ.deequ_constraints_builder import DeequConstraintsBuilder


@pytest.mark.spark
def test_build_constraints(spark, sample_dataframe):
    # Create a Spark DataFrame
    deequ_constraints_builder = DeequConstraintsBuilder()

    constraints = deequ_constraints_builder.build_constraints(spark, sample_dataframe)
    assert "constraint_suggestions" in constraints
    assert len(constraints["constraint_suggestions"]) > 1
    print(constraints)
    datatype_cons = {
        "constraint_name": "AnalysisBasedConstraint(DataType(load_dt,None),<function1>,Some(<function1>),None)",
        "column_name": "load_dt",
        "current_value": "DataType: Integral",
        "description": "'load_dt' has type Integral",
        "suggesting_rule": "RetainTypeRule()",
        "rule_description": "If we detect a non-string type, we suggest a type constraint",
        "code_for_constraint": '.hasDataType("load_dt", ConstrainableDataTypes.Integral)',
    }
    constraints["constraint_suggestions"].append(datatype_cons)

    file_name = deequ_constraints_builder.save_in_hocon_format(
        constraints, "TestDataset", "TestMetrics", "dist/Testfile.conf"
    )

    assert file_name == "dist/Testfile.conf"
    # self.assertEqual(len(constraints), 3)
