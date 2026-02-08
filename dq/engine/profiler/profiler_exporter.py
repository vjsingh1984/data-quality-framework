# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Union

from dq.engine.profiler.profiler_results import ProfileResult


class ProfileExporter:
    """Export profiling results to various formats.

    Supported formats:
    - **json**: Structured JSON format for programmatic access
    - **html**: Human-readable HTML report with visualizations
    - **markdown**: Markdown format for documentation

    Usage::

        exporter = ProfileExporter()
        profile = profiler_check.profile_dataframe(df, "my_table")

        # Export to JSON file
        exporter.export(profile, "output.json", format="json")

        # Export to HTML file
        exporter.export(profile, "report.html", format="html")

        # Export to Markdown string
        md_content = exporter.to_markdown(profile)
    """

    def export(
        self,
        profile: ProfileResult,
        output_path: Union[str, Path],
        format: str = "json",
    ) -> None:
        """Export profile result to a file.

        Args:
            profile: ProfileResult to export.
            output_path: Path to output file.
            format: Export format ("json", "html", "markdown").

        Raises:
            ValueError: If format is not supported.
        """
        output_path = Path(output_path)
        format = format.lower()

        if format == "json":
            content = self.to_json(profile)
            output_path.write_text(content, encoding="utf-8")
        elif format == "html":
            content = self.to_html(profile)
            output_path.write_text(content, encoding="utf-8")
        elif format in ("markdown", "md"):
            content = self.to_markdown(profile)
            output_path.write_text(content, encoding="utf-8")
        else:
            raise ValueError(
                f"Unsupported export format: {format}. "
                "Supported formats: json, html, markdown"
            )

    def to_json(self, profile: ProfileResult, *, indent: int = 2) -> str:
        """Convert profile result to JSON string.

        Args:
            profile: ProfileResult to convert.
            indent: JSON indentation level.

        Returns:
            JSON string representation of the profile.
        """
        return json.dumps(profile.to_dict(), indent=indent, default=str)

    def to_html(self, profile: ProfileResult) -> str:
        """Convert profile result to HTML report.

        Args:
            profile: ProfileResult to convert.

        Returns:
            HTML string representation of the profile.
        """
        html = StringIO()
        html.write("<!DOCTYPE html>\n")
        html.write('<html lang="en">\n')
        html.write("<head>\n")
        html.write("    <meta charset='UTF-8'>\n")
        html.write(
            "    <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
        )
        html.write(f"    <title>Data Profile: {profile.dataframe_name}</title>\n")
        html.write("    <style>\n")
        html.write(
            "        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }\n"
        )
        html.write(
            "        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }\n"
        )
        html.write(
            "        h1 { color: #333; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }\n"
        )
        html.write(
            "        h2 { color: #555; margin-top: 30px; border-left: 4px solid #4CAF50; padding-left: 10px; }\n"
        )
        html.write(
            "        .meta { color: #666; font-size: 14px; margin-bottom: 20px; }\n"
        )
        html.write(
            "        table { width: 100%; border-collapse: collapse; margin: 20px 0; }\n"
        )
        html.write(
            "        th { background: #4CAF50; color: white; padding: 12px; text-align: left; font-weight: 600; }\n"
        )
        html.write("        td { padding: 10px; border-bottom: 1px solid #ddd; }\n")
        html.write("        tr:hover { background: #f9f9f9; }\n")
        html.write(
            "        .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }\n"
        )
        html.write(
            "        .stat-card { background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #4CAF50; }\n"
        )
        html.write(
            "        .stat-label { font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }\n"
        )
        html.write(
            "        .stat-value { font-size: 24px; font-weight: bold; color: #333; margin-top: 5px; }\n"
        )
        html.write("        .severity-error { border-left-color: #f44336; }\n")
        html.write("        .severity-warning { border-left-color: #ff9800; }\n")
        html.write("        .severity-info { border-left-color: #2196F3; }\n")
        html.write("        .suggestions { margin-top: 20px; }\n")
        html.write(
            "        .suggestion { background: #e3f2fd; padding: 10px; margin: 5px 0; border-radius: 4px; }\n"
        )
        html.write("    </style>\n")
        html.write("</head>\n")
        html.write("<body>\n")
        html.write("    <div class='container'>\n")
        html.write(f"        <h1>Data Profile: {profile.dataframe_name}</h1>\n")
        html.write(
            f"        <div class='meta'>Generated: {profile.timestamp.strftime('%Y-%m-%d %H:%M:%S')} | Profile Type: {profile.profile_type}</div>\n"
        )

        # General Statistics
        html.write("        <h2>General Statistics</h2>\n")
        html.write("        <div class='stat-grid'>\n")
        html.write(
            f"            <div class='stat-card'><div class='stat-label'>Row Count</div><div class='stat-value'>{profile.general.row_count:,}</div></div>\n"
        )
        html.write(
            f"            <div class='stat-card'><div class='stat-label'>Column Count</div><div class='stat-value'>{profile.general.column_count}</div></div>\n"
        )
        html.write(
            f"            <div class='stat-card'><div class='stat-label'>Size</div><div class='stat-value'>{self._format_bytes(profile.general.size_bytes)}</div></div>\n"
        )
        html.write("        </div>\n")

        # Column Profiles
        html.write("        <h2>Column Profiles</h2>\n")
        html.write("        <table>\n")
        html.write("            <thead><tr>\n")
        html.write("                <th>Column</th>\n")
        html.write("                <th>Type</th>\n")
        html.write("                <th>Nullable</th>\n")
        html.write("                <th>Null %</th>\n")
        html.write("                <th>Statistics</th>\n")
        html.write("            </tr></thead>\n")
        html.write("            <tbody>\n")

        for col_name, col_profile in profile.columns.items():
            html.write("            <tr>\n")
            html.write(f"                <td><strong>{col_name}</strong></td>\n")
            html.write(f"                <td>{col_profile.data_type}</td>\n")
            html.write(
                f"                <td>{'Yes' if col_profile.nullable else 'No'}</td>\n"
            )
            html.write(f"                <td>{col_profile.null_percentage:.1f}%</td>\n")
            html.write(
                f"                <td>{self._format_column_stats(col_profile)}</td>\n"
            )
            html.write("            </tr>\n")

        html.write("            </tbody>\n")
        html.write("        </table>\n")

        # Correlations
        if profile.correlations:
            html.write("        <h2>Correlations</h2>\n")
            html.write("        <table>\n")
            html.write(
                "            <thead><tr><th>Pair</th><th>Correlation</th></tr></thead>\n"
            )
            html.write("            <tbody>\n")
            for pair, corr in profile.correlations.items():
                html.write(
                    f"                <tr><td>{pair}</td><td>{corr:.3f}</td></tr>\n"
                )
            html.write("            </tbody>\n")
            html.write("        </table>\n")

        # Suggestions
        if profile.suggestions:
            html.write("        <h2>Rule Suggestions</h2>\n")
            html.write("        <div class='suggestions'>\n")
            for suggestion in profile.suggestions:
                html.write(
                    f"            <div class='suggestion severity-{suggestion.severity}'>\n"
                )
                html.write(
                    f"                <strong>{suggestion.column}:</strong> {suggestion.suggestion}\n"
                )
                html.write(
                    f"                <br><small>{suggestion.constraint} | {suggestion.severity}</small>\n"
                )
                html.write("            </div>\n")
            html.write("        </div>\n")

        html.write("    </div>\n")
        html.write("</body>\n")
        html.write("</html>\n")

        return html.getvalue()

    def to_markdown(self, profile: ProfileResult) -> str:
        """Convert profile result to Markdown report.

        Args:
            profile: ProfileResult to convert.

        Returns:
            Markdown string representation of the profile.
        """
        md = StringIO()

        md.write(f"# Data Profile: {profile.dataframe_name}\n\n")
        md.write(
            f"**Generated:** {profile.timestamp.strftime('%Y-%m-%d %H:%M:%S')}  \n"
        )
        md.write(f"**Profile Type:** {profile.profile_type}  \n\n")

        # General Statistics
        md.write("## General Statistics\n\n")
        md.write(f"- **Row Count:** {profile.general.row_count:,}\n")
        md.write(f"- **Column Count:** {profile.general.column_count}\n")
        md.write(f"- **Size:** {self._format_bytes(profile.general.size_bytes)}\n\n")

        # Schema
        md.write("### Schema\n\n")
        md.write("| Column | Type | Nullable |\n")
        md.write("|--------|------|----------|\n")
        for col_name, col_profile in profile.columns.items():
            md.write(
                f"| {col_name} | {col_profile.data_type} | {'Yes' if col_profile.nullable else 'No'} |\n"
            )
        md.write("\n")

        # Column Profiles
        md.write("## Column Profiles\n\n")
        for col_name, col_profile in profile.columns.items():
            md.write(f"### {col_name}\n\n")
            md.write(f"- **Type:** {col_profile.data_type}\n")
            md.write(
                f"- **Null Count:** {col_profile.null_count} ({col_profile.null_percentage:.1f}%)\n"
            )

            if col_profile.numeric_stats:
                stats = col_profile.numeric_stats
                md.write("- **Statistics:**\n")
                md.write(f"  - Min: {stats.min}\n")
                md.write(f"  - Max: {stats.max}\n")
                md.write(
                    f"  - Mean: {stats.mean:.2f}\n" if stats.mean else "  - Mean: N/A\n"
                )
                md.write(
                    f"  - StdDev: {stats.stddev:.2f}\n"
                    if stats.stddev
                    else "  - StdDev: N/A\n"
                )
                if stats.percentiles:
                    md.write("  - Percentiles:\n")
                    for p_name, p_value in sorted(stats.percentiles.items()):
                        md.write(f"    - {p_name}: {p_value}\n")

            if col_profile.string_stats:
                stats = col_profile.string_stats
                md.write("- **String Statistics:**\n")
                md.write(f"  - Min Length: {stats.min_length}\n")
                md.write(f"  - Max Length: {stats.max_length}\n")
                md.write(
                    f"  - Avg Length: {stats.avg_length:.1f}\n"
                    if stats.avg_length
                    else "  - Avg Length: N/A\n"
                )
                if stats.patterns:
                    md.write("  - Patterns:\n")
                    for pattern, count in stats.patterns.items():
                        md.write(f"    - {pattern}: {count}\n")

            if col_profile.date_stats:
                stats = col_profile.date_stats
                md.write("- **Date Range:**\n")
                md.write(f"  - From: {stats.min_date}\n")
                md.write(f"  - To: {stats.max_date}\n")

            if col_profile.unique_value_stats:
                stats = col_profile.unique_value_stats
                md.write("- **Unique Values:**\n")
                md.write(f"  - Distinct Count: {stats.distinct_count}\n")
                md.write(f"  - Unique %: {stats.unique_percentage:.1f}%\n")

            md.write("\n")

        # Correlations
        if profile.correlations:
            md.write("## Correlations\n\n")
            md.write("| Pair | Correlation |\n")
            md.write("|------|-------------|\n")
            for pair, corr in profile.correlations.items():
                md.write(f"| {pair} | {corr:.3f} |\n")
            md.write("\n")

        # Suggestions
        if profile.suggestions:
            md.write("## Rule Suggestions\n\n")
            for suggestion in profile.suggestions:
                severity_icon = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}.get(
                    suggestion.severity, "•"
                )
                md.write(
                    f"{severity_icon} **{suggestion.column}** ({suggestion.severity}): {suggestion.suggestion}\n"
                )
                md.write(f"   - Constraint: `{suggestion.constraint}`\n")
                md.write(f"   - Issue: `{suggestion.issue}`\n\n")

        return md.getvalue()

    def _format_bytes(self, size_bytes: int) -> str:
        """Format byte size to human readable string.

        Args:
            size_bytes: Size in bytes.

        Returns:
            Formatted string (e.g., "1.23 MB").
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    def _format_column_stats(self, col_profile) -> str:
        """Format column statistics for HTML table.

        Args:
            col_profile: ColumnProfile object.

        Returns:
            HTML string with formatted statistics.
        """
        parts = []

        if col_profile.numeric_stats:
            stats = col_profile.numeric_stats
            if stats.min is not None:
                parts.append(f"Min: {stats.min:.2f}")
            if stats.max is not None:
                parts.append(f"Max: {stats.max:.2f}")
            if stats.mean is not None:
                parts.append(f"Mean: {stats.mean:.2f}")

        if col_profile.string_stats:
            stats = col_profile.string_stats
            if stats.min_length is not None:
                parts.append(f"Min Len: {stats.min_length}")
            if stats.max_length is not None:
                parts.append(f"Max Len: {stats.max_length}")

        if col_profile.unique_value_stats:
            stats = col_profile.unique_value_stats
            parts.append(f"Distinct: {stats.distinct_count}")

        return "<br>".join(parts) if parts else "N/A"
