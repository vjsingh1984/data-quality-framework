# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Data models for the Data Quality Framework."""
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DQResult:
    """Standardized result from a data quality check.

    Provides a structured representation of check results with backward
    compatibility via ``to_dict()`` for code that expects plain dicts.

    Args:
        check: Name or description of the check.
        success: Whether the check passed.
        details: Additional check-specific information.
        engine: Engine that produced this result.
        rule_name: Name of the rule this check belongs to.
        level: Severity level (e.g. "Error", "Warning").
        ts: Timestamp in milliseconds when the check was run.
        jobid: Spark application ID.
    """

    check: str
    success: bool
    details: Dict[str, Any] = field(default_factory=dict)
    engine: str = ""
    rule_name: str = ""
    level: str = "Error"
    ts: Optional[int] = None
    jobid: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to legacy dict format for backward compatibility.

        Returns:
            Dict with ``check``, ``success``, ``details``, ``ts``, ``jobid``.
        """
        return {
            "check": self.check,
            "success": self.success,
            "details": self.details,
            "ts": self.ts,
            "jobid": self.jobid,
        }

    @classmethod
    def from_legacy_dict(cls, d: dict) -> "DQResult":
        """Create a DQResult from a legacy metric dictionary.

        Args:
            d: Dict with at least ``check`` and ``success`` keys.

        Returns:
            DQResult instance.
        """
        return cls(
            check=d.get("check", ""),
            success=d.get("success", False),
            details=d.get("details", {}),
            engine=d.get("engine", ""),
            rule_name=d.get("rule_name", ""),
            level=d.get("level", "Error"),
            ts=d.get("ts"),
            jobid=d.get("jobid"),
        )
