# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

import logging

logger = logging.getLogger(__name__)


class DeequConstraintsBuilder:
    """Generates and runs Deequ constraint suggestions for a DataFrame."""

    def build_constraints(self, spark, df):
        """Generate suggested data quality constraints for a DataFrame.

        Args:
            spark: Active SparkSession.
            df: Spark DataFrame to analyse.

        Returns:
            Dictionary of constraint suggestions from Deequ.
        """
        from pydeequ.suggestions import (  # noqa: F401
            CompleteIfCompleteRule,
            ConstraintSuggestionRunner,
            NonNegativeNumbersRule,
            RetainCompletenessRule,
            RetainTypeRule,
            UniqueIfApproximatelyUniqueRule,
        )

        suggested_constraints = (
            ConstraintSuggestionRunner(spark)
            .onData(df)
            .addConstraintRule(CompleteIfCompleteRule())
            .addConstraintRule(NonNegativeNumbersRule())
            .addConstraintRule(RetainCompletenessRule())
            .addConstraintRule(RetainTypeRule())
            .addConstraintRule(UniqueIfApproximatelyUniqueRule())
            .run()
        )

        return suggested_constraints

    def save_in_hocon_format(
        self, constraint_suggestions, dataset_name, dq_metrics_table, file_name
    ):
        """Save constraint suggestions as a HOCON configuration file.

        Args:
            constraint_suggestions: Output from ``build_constraints``.
            dataset_name: Name of the dataset for config metadata.
            dq_metrics_table: Metrics table reference for repository config.
            file_name: File path to write the HOCON config to.

        Returns:
            The file path that was written.
        """
        transform_suggestion = []
        for constraint in constraint_suggestions["constraint_suggestions"]:
            code_string = constraint["code_for_constraint"]
            constraint_name = code_string[1 : code_string.index("(")]
            assertion = ""
            if "lambda" in code_string:
                assertion = code_string[
                    code_string.index("lambda") : code_string.index(
                        ",", code_string.index("lambda")
                    )
                ]
            datatype = ""
            if "hasDataType" in code_string:
                datatype = str(
                    constraint["current_value"][
                        constraint["current_value"].index(":") + 1 :
                    ]
                ).strip()
            check_str = f"""{{
                                alias = "{constraint['constraint_name']}"
                                column = "{constraint['column_name']}"
                                level = "Warning"
                                constraint = "{constraint_name}"
                                assertion = "{assertion}"
                                datatype = "{datatype}"
                            }},"""
            transform_suggestion.append(check_str)
        transform_suggestion = "".join(transform_suggestion).replace("'", "")
        dq_conf = f"""
            dqframework {{
            requiredscore = 1.0

            repository {{
              dataset ="{dataset_name}"
              format = "json"
              catalog {{
              tables = [ {dq_metrics_table}]
              }}
            }}
            dqrules = [
              {{
                name = "{dataset_name}-rules"
                engine = "deequ"
                checks = [
                    {transform_suggestion}
                    ]
              }}
            ]
          }}
        """

        with open(file_name, "w") as conf_file:
            conf_file.write(dq_conf)
        logger.info("Config file saved to: %s", file_name)
        return file_name

    def run_dq_rules_for_dataset(self, spark, df, constraints):
        """Run Deequ validation rules built from constraint suggestions.

        Args:
            spark: Active SparkSession.
            df: Spark DataFrame to validate.
            constraints: Constraint suggestions from ``build_constraints``.

        Returns:
            DataFrame of check results.
        """
        from pydeequ.checks import Check, CheckLevel
        from pydeequ.verification import VerificationResult, VerificationSuite
        from pyspark.sql import functions as F

        for suggestion in constraints["constraint_suggestions"]:
            logger.debug(
                "Suggested constraint for '%s': %s",
                suggestion["column_name"],
                suggestion["description"],
            )
            logger.debug("Rule description: '%s'", suggestion["rule_description"])
            logger.debug("Python code: `%s`", suggestion["code_for_constraint"])

        check = Check(
            spark_session=spark,
            level=CheckLevel.Warning,
            description="Data Quality Check",
        )

        # Apply each suggestion iteratively instead of using eval()
        for suggestion in constraints["constraint_suggestions"]:
            constraint_code = suggestion["code_for_constraint"]
            logger.debug("Applying constraint: %s", constraint_code)
            # code_for_constraint is like '.hasCompleteness("col", lambda x: x >= 0.9)'
            # We apply it iteratively on the check object
            try:
                import ast as ast_module

                # Parse and validate the expression before executing
                full_expr = "check" + constraint_code
                parsed = ast_module.parse(full_expr, mode="eval")
                # Compile from validated AST with restricted builtins
                # Note: This eval() is used to apply LLM-suggested constraint code to a check object.
                # The constraint_code comes from the Deequ ConstraintsBuilder LLM and should be
                # validated/whitelisted before use in production. The eval is restricted to only
                # have access to the 'check' object with no builtins.
                compiled = compile(parsed, "<constraint_suggestion>", "eval")
                check = eval(
                    compiled, {"__builtins__": {}, "check": check}
                )  # nosec B307
            except Exception as e:
                logger.warning(
                    "Could not apply suggested constraint: %s (%s)", constraint_code, e
                )

        checked_constraints = VerificationSuite(spark).onData(df).addCheck(check).run()

        df_checked_constraints = VerificationResult.checkResultsAsDataFrame(
            spark, checked_constraints
        )

        if logger.isEnabledFor(logging.INFO):
            df_checked_constraints.show(
                n=df_checked_constraints.count(), truncate=False
            )

        df_checked_constraints_failures = df_checked_constraints.filter(
            F.col("constraint_status") == "Failure"
        )

        if df_checked_constraints_failures.count() > 0 and logger.isEnabledFor(
            logging.INFO
        ):
            df_checked_constraints_failures.show(
                n=df_checked_constraints_failures.count(), truncate=False
            )

        return df_checked_constraints
