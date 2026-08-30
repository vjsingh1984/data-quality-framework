# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Result formatting utilities for CLI output."""
from typing import Any, Dict, List


def format_summary(results: List[Dict[str, Any]]) -> str:
    """Format DQ results as a human-readable summary.

    Args:
        results: List of metric dictionaries with ``success`` key.

    Returns:
        Formatted summary string.
    """
    passed = sum(1 for r in results if r.get("success", False))
    failed = len(results) - passed

    lines = [
        "",
        "=" * 60,
        "Data Quality Check Results",
        "=" * 60,
        f"Total checks: {len(results)}",
        f"Passed: {passed}",
        f"Failed: {failed}",
        "=" * 60,
        "",
    ]

    if failed > 0:
        lines.append("Failed checks:")
        for result in results:
            if not result.get("success", False):
                lines.append(f"  - {result.get('check', 'Unknown')}")

    return "\n".join(lines)
