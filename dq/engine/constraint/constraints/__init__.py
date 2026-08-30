# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Built-in custom constraints — auto-registered on import."""
from dq.engine.constraint.constraint_registry import ConstraintRegistry
from dq.engine.constraint.constraints.distinctness_by_group import DistinctnessByGroup
from dq.engine.constraint.constraints.lookup_column_list import LookupColumnList
from dq.engine.constraint.constraints.negative_values_check import NegativeValuesCheck
from dq.engine.constraint.constraints.rate_of_change import RateOfChange

# Register constraints with semantic names
ConstraintRegistry.register("DistinctnessByGroup", DistinctnessByGroup)
ConstraintRegistry.register("RateOfChange", RateOfChange)
ConstraintRegistry.register("LookupColumnList", LookupColumnList)
ConstraintRegistry.register("NegativeValuesCheck", NegativeValuesCheck)
