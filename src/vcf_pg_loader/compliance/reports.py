"""HIPAA compliance report generation."""

import json
from enum import Enum
from html import escape

from .checks import ComplianceReport, ComplianceStatus


class ReportFormat(Enum):
    JSON = "json"
    HTML = "html"
    TEXT = "text"


class ReportExporter:
    def export(self, report: ComplianceReport, format: ReportFormat) -> str:
        if format == ReportFormat.JSON:
            return self._export_json(report)
        elif format == ReportFormat.HTML:
            return self._export_html(report)
        elif format == ReportFormat.TEXT:
            return self._export_text(report)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def get_exit_code(self, report: ComplianceReport) -> int:
        return 0 if report.is_compliant else 1

    def _export_json(self, report: ComplianceReport) -> str:
        return json.dumps(report.to_dict(), indent=2)

    def _export_html(self, report: ComplianceReport) -> str:
        status_colors = {
            ComplianceStatus.PASS: "#28a745",
            ComplianceStatus.FAIL: "#dc3545",
            ComplianceStatus.WARN: "#ffc107",
            ComplianceStatus.SKIP: "#6c757d",
        }

        status_labels = {
            ComplianceStatus.PASS: "PASS",
            ComplianceStatus.FAIL: "FAIL",
            ComplianceStatus.WARN: "WARN",
            ComplianceStatus.SKIP: "SKIP",
        }

        rows = []
        for result in report.results:
            color = status_colors[result.status]
            label = status_labels[result.status]
            remediation = ""
            if result.remediation:
                remediation = f'<div class="remediation">{escape(result.remediation)}</div>'

            rows.append(f"""
            <tr>
                <td><span class="status" style="background-color: {color};">{label}</span></td>
                <td><strong>{escape(result.check.name)}</strong></td>
                <td><code>{escape(result.check.hipaa_reference)}</code></td>
                <td>{escape(result.check.severity.value.upper())}</td>
                <td>{escape(result.message)}{remediation}</td>
            </tr>
            """)

        compliance_status = "Compliant" if report.is_compliant else "Non-Compliant"
        compliance_color = "#28a745" if report.is_compliant else "#dc3545"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HIPAA Compliance Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0 0 10px 0; color: #333; }}
        .timestamp {{ color: #666; font-size: 14px; }}
        .summary {{
            display: flex;
            gap: 20px;
            margin-top: 20px;
        }}
        .summary-item {{
            padding: 10px 20px;
            border-radius: 4px;
            color: #fff;
            font-weight: bold;
        }}
        .summary-passed {{ background: #28a745; }}
        .summary-failed {{ background: #dc3545; }}
        .summary-warned {{ background: #ffc107; color: #333; }}
        .compliance-status {{
            padding: 10px 20px;
            border-radius: 4px;
            color: #fff;
            font-weight: bold;
            font-size: 18px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #fff;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{ background: #333; color: #fff; }}
        tr:hover {{ background: #f8f9fa; }}
        .status {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            color: #fff;
            font-weight: bold;
            font-size: 12px;
        }}
        .remediation {{
            margin-top: 8px;
            padding: 8px;
            background: #fff3cd;
            border-radius: 4px;
            font-size: 13px;
            color: #856404;
        }}
        code {{ background: #e9ecef; padding: 2px 6px; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>HIPAA Compliance Report</h1>
        <div class="timestamp">Generated: {report.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}</div>
        <div class="summary">
            <div class="summary-item summary-passed">Passed: {report.passed_count}</div>
            <div class="summary-item summary-failed">Failed: {report.failed_count}</div>
            <div class="summary-item summary-warned">Warnings: {report.warned_count}</div>
            <div class="compliance-status" style="background: {compliance_color};">{compliance_status}</div>
        </div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Status</th>
                <th>Check</th>
                <th>HIPAA Reference</th>
                <th>Severity</th>
                <th>Details</th>
            </tr>
        </thead>
        <tbody>
            {"".join(rows)}
        </tbody>
    </table>
</body>
</html>"""

    def _export_text(self, report: ComplianceReport) -> str:
        status_symbols = {
            ComplianceStatus.PASS: "✓ PASS",
            ComplianceStatus.FAIL: "✗ FAIL",
            ComplianceStatus.WARN: "! WARN",
            ComplianceStatus.SKIP: "- SKIP",
        }

        lines = [
            "=" * 60,
            "HIPAA Compliance Report",
            f"Generated: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "=" * 60,
            "",
            f"Summary: {report.passed_count} passed, {report.failed_count} failed, {report.warned_count} warnings",
            f"Status: {'COMPLIANT' if report.is_compliant else 'NON-COMPLIANT'}",
            "",
            "-" * 60,
        ]

        for result in report.results:
            symbol = status_symbols[result.status]
            lines.append(f"{symbol} | {result.check.name}")
            lines.append(f"         HIPAA: {result.check.hipaa_reference}")
            lines.append(f"         {result.message}")
            if result.remediation:
                lines.append(f"         Remediation: {result.remediation}")
            lines.append("")

        lines.append("-" * 60)
        return "\n".join(lines)
