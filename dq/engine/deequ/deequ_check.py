# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import ast
import json

from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.verification import VerificationSuite
from pyhocon import ConfigTree
from pyspark.sql import SparkSession


class DeequCheck:
    """Builds and applies PyDeequ Check constraints from HOCON configuration.

    Supports both single-check mode (all constraints chained on one Check)
    and multi-check mode (one Check per constraint for granular reporting).
    """

    _check_level_map = {"Warning": CheckLevel.Warning, "Error": CheckLevel.Error}

    def __init__(self, checks_config: ConfigTree, single_check_mode=False):
        self._checks_config = checks_config
        self._single_check_mode = single_check_mode

    # Allowed AST node types for safe assertion parsing
    _SAFE_NODES = (
        ast.Lambda,
        ast.Compare,
        ast.BoolOp,
        ast.UnaryOp,
        ast.BinOp,
        ast.Constant,
        ast.Num,
        ast.Str,
        ast.NameConstant,  # py3.7 compat
        ast.Name,
        ast.Load,
        ast.arguments,
        ast.arg,
        ast.Eq,
        ast.NotEq,
        ast.Lt,
        ast.LtE,
        ast.Gt,
        ast.GtE,
        ast.And,
        ast.Or,
        ast.Not,
        ast.Is,
        ast.IsNot,
        ast.In,
        ast.NotIn,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
        ast.USub,
        ast.UAdd,
    )

    def safe_eval_lambda(self, lambda_expr):
        """Safely evaluate an assertion lambda from a string.

        Parses the expression as AST and validates that it contains only
        safe nodes (comparisons, arithmetic, boolean ops) before compiling.

        Args:
            lambda_expr: String like ``"lambda x: x > 0.95"``.

        Returns:
            Callable lambda function.

        Raises:
            ValueError: If expression is not a lambda or contains unsafe nodes.
        """
        try:
            parsed_expr = ast.parse(lambda_expr, mode="eval")
            if not isinstance(parsed_expr.body, ast.Lambda):
                raise ValueError("Provided expression is not a lambda function.")

            # Walk all AST nodes and reject anything not in the safe set
            for node in ast.walk(parsed_expr):
                if not isinstance(node, (ast.Expression, *self._SAFE_NODES)):
                    raise ValueError(f"Unsafe AST node type: {type(node).__name__}")

            # Compile from the validated AST (no eval of arbitrary strings)
            code = compile(parsed_expr, "<assertion>", "eval")
            return eval(code, {"__builtins__": {}})
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Error evaluating lambda: {e}")

    def apply_checks(
        self, verification_run_builder: VerificationSuite, spark_session: SparkSession
    ):
        if self._single_check_mode:
            check = Check(
                spark_session=spark_session,
                level=CheckLevel.Error,
                description="Deeque Single Check mode",
            )
        else:
            check_list = []

        for check_config in self._checks_config:
            columns = check_config.get("columns", None)
            column = check_config.get("column", None)
            constraint = check_config.get("constraint", None)
            assertion = check_config.get("assertion", None)
            hint = check_config.get("hint", None)
            kwargs = check_config.get("kwargs", None)
            if not (self._single_check_mode):
                check = Check(
                    spark_session=spark_session,
                    level=self._check_level_map.get(check_config.get("level", "Error")),
                    description=json.dumps(
                        {
                            "alias": check_config.get("alias", "*Unknown"),
                            "description": check_config.get("description", "*Unknown"),
                            "constraint": constraint,
                        }
                    ),
                )
            # Apply constraint dynamically
            # Refer following githb link for complete list of supported python-deequ checks.
            # https://github.com/awslabs/python-deequ/blob/master/docs/checks.md

            method = getattr(check, constraint)
            if assertion:
                assertion_func = self.safe_eval_lambda(assertion)
            else:
                assertion_func = None

            if method:
                constrainedCheck = None

                if "hasDataType" == constraint:
                    datatype = check_config.get("datatype", None)
                    if datatype:
                        deequ_datatype = getattr(ConstrainableDataTypes, datatype, None)
                        if deequ_datatype is None:
                            raise ValueError(f"Invalid data type '{datatype}'")
                        constrainedCheck = method(
                            column, deequ_datatype, assertion=assertion_func, hint=hint
                        )

                elif assertion:
                    # Safely evaluate the assertion (e.g., lambda)
                    assertion_func = self.safe_eval_lambda(assertion)
                    if column or columns:
                        if kwargs:
                            constrainedCheck = method(
                                column or columns,
                                assertion=assertion_func,
                                hint=hint,
                                **kwargs,
                            )
                        else:
                            constrainedCheck = method(
                                column or columns, assertion=assertion_func, hint=hint
                            )
                    else:
                        if kwargs:
                            constrainedCheck = method(
                                assertion=assertion_func, hint=hint, **kwargs
                            )
                        else:
                            constrainedCheck = method(
                                assertion=assertion_func, hint=hint
                            )
                elif column or columns:
                    # Apply constraint without assertion
                    if kwargs:
                        constrainedCheck = method(
                            column or columns, hint=hint, **kwargs
                        )
                    else:
                        constrainedCheck = method(column or columns, hint=hint)
                elif kwargs:
                    constrainedCheck = method(**kwargs)
                else:
                    constrainedCheck = method()
                if self._single_check_mode:
                    check = constrainedCheck
                else:
                    check_list.append(constrainedCheck)
            else:
                raise AttributeError(f"Deequ method '{constraint}' not found.")

        # Process All daisy chained constraints via single or multi check approach in verification run builder
        if self._single_check_mode:
            verification_run_builder.addCheck(check)
        else:
            for check in check_list:
                verification_run_builder.addCheck(check)

        return verification_run_builder
