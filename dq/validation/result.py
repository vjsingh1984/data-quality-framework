# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Validation result classes for schema validation."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ValidationResult:
    """Result of a schema validation check.

    Provides engine-agnostic representation of validation outcomes
    that can be converted to different engine formats (Deequ, etc.).

    Attributes:
        check_name: Name of the validation check.
        constraint: Type of constraint validated.
        success: Whether the validation passed.
        details: Additional details about the validation result.
        assertion: Optional assertion message.
    """

    check_name: str
    constraint: str
    success: bool
    details: Dict[str, Any] = field(default_factory=dict)
    assertion: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "check": self.check_name,
            "constraint": self.constraint,
            "success": self.success,
            "details": self.details,
            "assertion": self.assertion,
        }


@dataclass
class SchemaValidationSummary:
    """Summary of schema validation results.

    Attributes:
        results: List of individual validation results.
        total_checks: Total number of checks performed.
        passed_checks: Number of checks that passed.
        failed_checks: Number of checks that failed.
    """

    results: List[ValidationResult] = field(default_factory=list)

    @property
    def total_checks(self) -> int:
        return len(self.results)

    @property
    def passed_checks(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed_checks(self) -> int:
        return sum(1 for r in self.results if not r.success)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result to the summary."""
        self.results.append(result)

    def to_metric_dicts(
        self, engine_name: str, dataset: str, timestamp_ms: int
    ) -> List[Dict[str, Any]]:
        """Convert results to metric dict format for DQEngine."""

        metrics = []
        for result in self.results:
            metrics.append(
                {
                    "check": result.check_name,
                    "constraint": result.constraint,
                    "success": result.success,
                    "engine": engine_name,
                    "timestamp_ms": timestamp_ms,
                    "dataset": dataset,
                    "details": result.details,
                    "execution_time_ms": None,
                    "assertion": result.assertion,
                }
            )
        return metrics
