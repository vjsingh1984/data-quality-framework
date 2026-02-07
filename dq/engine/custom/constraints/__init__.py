# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Built-in custom constraints — auto-registered on import."""
from dq.engine.custom.constraint_registry import ConstraintRegistry
from dq.engine.custom.constraints.distinctness_by_group import DistinctnessByGroup
from dq.engine.custom.constraints.lookup_column_list import LookupBasedOnColumnNameList
from dq.engine.custom.constraints.negative_values_check import (
    WideTablesNegativeValuesCheck,
)
from dq.engine.custom.constraints.rate_of_change import RateOfChange

ConstraintRegistry.register("DistinctnessByGroup", DistinctnessByGroup)
ConstraintRegistry.register("RateOfChange", RateOfChange)
ConstraintRegistry.register("LookupBasedOnColumnNameList", LookupBasedOnColumnNameList)
ConstraintRegistry.register(
    "WideTablesNegativeValuesCheck", WideTablesNegativeValuesCheck
)
