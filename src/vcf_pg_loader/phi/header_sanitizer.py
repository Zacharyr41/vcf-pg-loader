"""VCF header sanitization for HIPAA compliance.

HIPAA Reference: 164.514(b) - De-identification Standard

VCF headers may contain PHI in metadata fields (CommandLine paths with patient
info, SAMPLE fields with demographics, institution names, processing dates).
This module sanitizes headers while preserving scientific utility.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID


@dataclass
class SanitizationConfig:
    """Configuration for VCF header sanitization."""

    remove_commandline: bool = True
    remove_sample_metadata: bool = True
    remove_dates: bool = True
    remove_file_paths: bool = True
    remove_institution_patterns: bool = True
    custom_patterns: list[str] = field(default_factory=list)
    preserve_fields: set[str] = field(default_factory=lambda: {"reference", "assembly"})


@dataclass
class SanitizedItem:
    """Record of a sanitized item."""

    line_number: int
    original_value: str
    sanitized_value: str
    pattern_matched: str
    field_type: str


@dataclass
class SanitizedHeader:
    """Result of header sanitization."""

    sanitized_lines: list[str]
    removed_items: list[SanitizedItem]
    phi_detected: bool
    summary: dict[str, int]


@dataclass
class PHIScanResult:
    """Result of scanning for PHI."""

    has_phi: bool
    findings: list[dict[str, str | int]]
    summary: dict[str, int]
    risk_level: str


class VCFHeaderSanitizer:
    """Sanitizes VCF headers to remove PHI."""

    PHI_PATTERNS = [
        (r"(?i)patient[_\s]?id\s*[=:]\s*\S+", "patient_id"),
        (r"(?i)mrn[_\s]?[=:]\s*\S+", "mrn"),
        (r"(?i)ssn[_\s]?[=:]\s*\S+", "ssn"),
        (r"(?i)dob[_\s]?[=:]\s*\S+", "dob"),
        (r"(?i)birth[_\s]?date[_\s]?[=:]\s*\S+", "birth_date"),
        (r"(?i)subject[_\s]?id\s*[=:]\s*\S+", "subject_id"),
        (r"(?i)accession[_\s]?[=:]\s*\S+", "accession"),
    ]

    PATH_PATTERNS = [
        (r"/home/\w+/[^\s,\"']+", "unix_home_path"),
        (r"/Users/\w+/[^\s,\"']+", "macos_home_path"),
        (r"[A-Z]:\\Users\\[^\s,\"']+", "windows_home_path"),
        (r"/data/patients?/[^\s,\"']+", "patient_data_path"),
        (r"/clinical/[^\s,\"']+", "clinical_path"),
        (r"/PHI/[^\s,\"']+", "phi_path"),
    ]

    DATE_PATTERNS = [
        (r"\d{3}-\d{2}-\d{4}", "ssn_format"),
        (r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", "date_slash"),
        (r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", "iso_datetime"),
        (r"\b\d{4}-\d{2}-\d{2}\b", "iso_date"),
    ]

    INSTITUTION_PATTERNS = [
        (r"(?i)\bMayo\s+Clinic\b", "mayo_clinic"),
        (r"(?i)\bJohns\s+Hopkins\b", "johns_hopkins"),
        (r"(?i)\bMGH\b", "mgh"),
        (r"(?i)\bUCSF\b", "ucsf"),
        (r"(?i)\bCleveland\s+Clinic\b", "cleveland_clinic"),
        (r"(?i)\bMD\s+Anderson\b", "md_anderson"),
        (r"(?i)\bMemorial\s+Sloan\b", "memorial_sloan"),
        (r"(?i)\bStanford\s+(Health|Medicine|Hospital)\b", "stanford"),
    ]

    def __init__(self, config: SanitizationConfig | None = None):
        self._config = config or SanitizationConfig()
        self._compiled_patterns: list[tuple[re.Pattern, str]] = []
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        patterns: list[tuple[str, str]] = []
        patterns.extend(self.PHI_PATTERNS)

        if self._config.remove_file_paths:
            patterns.extend(self.PATH_PATTERNS)

        if self._config.remove_dates:
            patterns.extend(self.DATE_PATTERNS)

        if self._config.remove_institution_patterns:
            patterns.extend(self.INSTITUTION_PATTERNS)

        for pattern in self._config.custom_patterns:
            patterns.append((pattern, "custom"))

        self._compiled_patterns = [(re.compile(p), name) for p, name in patterns]

    def sanitize_header(self, vcf_header: str) -> SanitizedHeader:
        """Sanitize VCF header, returning cleaned version and removed items."""
        lines = vcf_header.split("\n")
        sanitized_lines: list[str] = []
        removed_items: list[SanitizedItem] = []
        summary: dict[str, int] = {}

        for line_num, line in enumerate(lines, 1):
            if line.startswith("##"):
                sanitized, items = self._sanitize_meta_line(line, line_num)
                sanitized_lines.append(sanitized)
                removed_items.extend(items)
                for item in items:
                    summary[item.pattern_matched] = summary.get(item.pattern_matched, 0) + 1
            else:
                sanitized_lines.append(line)

        return SanitizedHeader(
            sanitized_lines=sanitized_lines,
            removed_items=removed_items,
            phi_detected=len(removed_items) > 0,
            summary=summary,
        )

    def _sanitize_meta_line(self, line: str, line_num: int) -> tuple[str, list[SanitizedItem]]:
        removed: list[SanitizedItem] = []
        result = line

        if self._config.remove_commandline and "CommandLine" in line:
            original = result
            result = re.sub(r'CommandLine="[^"]*"', 'CommandLine="[REDACTED]"', result)
            result = re.sub(r"CommandLine=<[^>]*>", "CommandLine=<[REDACTED]>", result)
            if result != original:
                removed.append(
                    SanitizedItem(
                        line_number=line_num,
                        original_value=self._extract_value(original, "CommandLine"),
                        sanitized_value="[REDACTED]",
                        pattern_matched="commandline",
                        field_type="META",
                    )
                )
            return result, removed

        for key in self._config.preserve_fields:
            if f"ID={key}" in line.lower() or f"id={key}" in line.lower():
                return line, []

        for pattern, pattern_name in self._compiled_patterns:
            matches = pattern.findall(result)
            for match in matches:
                if match and not self._is_preserved(match):
                    original_value = match
                    result = result.replace(match, "[REDACTED]")
                    removed.append(
                        SanitizedItem(
                            line_number=line_num,
                            original_value=original_value,
                            sanitized_value="[REDACTED]",
                            pattern_matched=pattern_name,
                            field_type=self._get_field_type(line),
                        )
                    )

        return result, removed

    def _extract_value(self, line: str, field: str) -> str:
        match = re.search(rf'{field}="([^"]*)"', line)
        if match:
            return match.group(1)
        match = re.search(rf"{field}=<([^>]*)>", line)
        if match:
            return match.group(1)
        return ""

    def _is_preserved(self, value: str) -> bool:
        value_lower = value.lower()
        for preserved in self._config.preserve_fields:
            if preserved.lower() in value_lower:
                return True
        return False

    def _get_field_type(self, line: str) -> str:
        if line.startswith("##INFO"):
            return "INFO"
        if line.startswith("##FORMAT"):
            return "FORMAT"
        if line.startswith("##FILTER"):
            return "FILTER"
        if line.startswith("##SAMPLE"):
            return "SAMPLE"
        if line.startswith("##contig"):
            return "CONTIG"
        if line.startswith("##fileDate"):
            return "FILEDATE"
        return "META"

    def sanitize_sample_metadata(self, metadata: dict) -> tuple[dict, list[str]]:
        """Remove PHI from SAMPLE metadata dictionary."""
        sanitized = {}
        removed_keys: list[str] = []
        phi_keys = {"patientid", "mrn", "ssn", "dob", "birthdate", "subjectid", "accession"}

        for key, value in metadata.items():
            key_lower = key.lower().replace("_", "").replace("-", "")
            if key_lower in phi_keys:
                removed_keys.append(key)
                continue

            if isinstance(value, str):
                sanitized_value = value
                for pattern, _ in self._compiled_patterns:
                    sanitized_value = pattern.sub("[REDACTED]", sanitized_value)
                sanitized[key] = sanitized_value
            else:
                sanitized[key] = value

        return sanitized, removed_keys


class PHIScanner:
    """Scans VCF files for potential PHI without modifying them."""

    def __init__(self, config: SanitizationConfig | None = None):
        self._sanitizer = VCFHeaderSanitizer(config)

    def scan_vcf_for_phi(self, vcf_path: Path) -> PHIScanResult:
        """Scan VCF for potential PHI before loading."""
        findings: list[dict[str, str | int]] = []
        summary: dict[str, int] = {}

        header_lines = self._read_header(vcf_path)
        header_text = "\n".join(header_lines)

        result = self._sanitizer.sanitize_header(header_text)

        for item in result.removed_items:
            findings.append(
                {
                    "line": item.line_number,
                    "type": item.pattern_matched,
                    "field_type": item.field_type,
                    "value": item.original_value[:50] + "..."
                    if len(item.original_value) > 50
                    else item.original_value,
                }
            )
            summary[item.pattern_matched] = summary.get(item.pattern_matched, 0) + 1

        risk_level = self._calculate_risk_level(findings)

        return PHIScanResult(
            has_phi=len(findings) > 0,
            findings=findings,
            summary=summary,
            risk_level=risk_level,
        )

    def _read_header(self, vcf_path: Path) -> list[str]:
        import gzip

        header_lines = []
        opener = gzip.open if str(vcf_path).endswith(".gz") else open
        with opener(vcf_path, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    header_lines.append(line.rstrip())
                else:
                    break
        return header_lines

    def _calculate_risk_level(self, findings: list[dict]) -> str:
        if not findings:
            return "none"

        high_risk_types = {"ssn", "mrn", "patient_id", "ssn_format", "dob", "birth_date"}
        for finding in findings:
            if finding.get("type") in high_risk_types:
                return "high"

        medium_risk_types = {"patient_data_path", "clinical_path", "phi_path"}
        for finding in findings:
            if finding.get("type") in medium_risk_types:
                return "medium"

        return "low"


@dataclass
class SanitizationReport:
    """Report of sanitization actions for audit logging."""

    load_batch_id: UUID | None
    source_file: str
    items_sanitized: int
    summary: dict[str, int]
    phi_detected: bool
    risk_level: str
    sanitized_items: list[SanitizedItem]

    def to_audit_details(self) -> dict:
        return {
            "source_file": self.source_file,
            "items_sanitized": self.items_sanitized,
            "summary": self.summary,
            "phi_detected": self.phi_detected,
            "risk_level": self.risk_level,
        }
