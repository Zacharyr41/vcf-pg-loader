"""PHI pattern definitions and registry for HIPAA compliance.

HIPAA Reference: 164.514(b) - De-identification Standard

This module provides configurable PHI detection patterns with severity levels,
false positive hints, and support for custom pattern libraries.
"""

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PHIPattern:
    """A pattern for detecting PHI in VCF data."""

    name: str
    pattern: re.Pattern
    severity: str
    description: str
    false_positive_hints: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.severity not in ("critical", "high", "medium", "low"):
            raise ValueError(f"Invalid severity: {self.severity}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PHIPattern":
        pattern_str = data["pattern"]
        flags = 0
        if data.get("case_insensitive", False):
            flags = re.IGNORECASE
        return cls(
            name=data["name"],
            pattern=re.compile(pattern_str, flags),
            severity=data["severity"],
            description=data.get("description", ""),
            false_positive_hints=data.get("false_positive_hints", []),
        )


class PHIPatternRegistry:
    """Registry of PHI detection patterns with severity levels."""

    BUILTIN_PATTERNS: list[PHIPattern] = [
        PHIPattern(
            name="ssn",
            pattern=re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
            severity="critical",
            description="Social Security Number pattern (XXX-XX-XXXX)",
            false_positive_hints=["May be genomic coordinates in unusual format"],
        ),
        PHIPattern(
            name="mrn",
            pattern=re.compile(r"\b(?:MRN|mrn)[:\s]?\d+\b"),
            severity="critical",
            description="Medical Record Number",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="mrn_prefixed",
            pattern=re.compile(
                r"\b(?:patient|subject|case)[_-]?(?:id|num(?:ber)?)[:\s=]?\S+", re.IGNORECASE
            ),
            severity="critical",
            description="Patient/subject identifier pattern",
            false_positive_hints=["May be anonymized sample IDs"],
        ),
        PHIPattern(
            name="email",
            pattern=re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
            severity="high",
            description="Email address",
            false_positive_hints=["May be tool contact info in VCF header"],
        ),
        PHIPattern(
            name="phone",
            pattern=re.compile(r"\b(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
            severity="high",
            description="Phone number (US format)",
            false_positive_hints=["May match genomic coordinates"],
        ),
        PHIPattern(
            name="dob",
            pattern=re.compile(
                r"\b(?:dob|date[_\s]?of[_\s]?birth|birth[_\s]?date)[:\s=]?\S+", re.IGNORECASE
            ),
            severity="critical",
            description="Date of birth field",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="date_mdy",
            pattern=re.compile(r"\b(?:0?[1-9]|1[0-2])/(?:0?[1-9]|[12]\d|3[01])/(?:19|20)\d{2}\b"),
            severity="medium",
            description="Date in MM/DD/YYYY format",
            false_positive_hints=["May be file date or processing date"],
        ),
        PHIPattern(
            name="date_ymd",
            pattern=re.compile(r"\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\b"),
            severity="low",
            description="Date in YYYY-MM-DD format",
            false_positive_hints=["Common in file metadata, typically not PHI alone"],
        ),
        PHIPattern(
            name="credit_card",
            pattern=re.compile(
                r"\b(?:4\d{3}|5[1-5]\d{2}|6011|3[47]\d{2})[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"
            ),
            severity="critical",
            description="Credit card number pattern",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="ip_address",
            pattern=re.compile(
                r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
            ),
            severity="medium",
            description="IPv4 address",
            false_positive_hints=["May be server/tool IP, not patient-related"],
        ),
        PHIPattern(
            name="home_path_unix",
            pattern=re.compile(r"/(?:home|Users)/[a-zA-Z][a-zA-Z0-9._-]*/"),
            severity="medium",
            description="Unix/macOS home directory path",
            false_positive_hints=["Username may reveal operator identity"],
        ),
        PHIPattern(
            name="home_path_windows",
            pattern=re.compile(r"[A-Z]:\\Users\\[a-zA-Z][a-zA-Z0-9._-]*\\"),
            severity="medium",
            description="Windows home directory path",
            false_positive_hints=["Username may reveal operator identity"],
        ),
        PHIPattern(
            name="institution_hospital",
            pattern=re.compile(
                r"\b(?:Mayo\s+Clinic|Johns\s+Hopkins|MGH|Massachusetts\s+General|"
                r"Cleveland\s+Clinic|MD\s+Anderson|Memorial\s+Sloan|Stanford\s+(?:Health|Hospital|Medicine)|"
                r"UCSF|UCLA\s+Health|Mount\s+Sinai|NYU\s+Langone|Cedars[-\s]Sinai)\b",
                re.IGNORECASE,
            ),
            severity="medium",
            description="Major hospital/institution name",
            false_positive_hints=["May be reference to published research"],
        ),
        PHIPattern(
            name="accession",
            pattern=re.compile(r"\b(?:accession|acc)[:\s=]?\S+", re.IGNORECASE),
            severity="high",
            description="Accession number (may link to patient)",
            false_positive_hints=["May be public database accession like dbSNP"],
        ),
        PHIPattern(
            name="name_field",
            pattern=re.compile(
                r"\b(?:patient[_\s]?name|full[_\s]?name|first[_\s]?name|last[_\s]?name|"
                r"given[_\s]?name|family[_\s]?name|surname)[:\s=]?\S+",
                re.IGNORECASE,
            ),
            severity="critical",
            description="Name field identifier",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="address_field",
            pattern=re.compile(
                r"\b(?:street|address|city|state|zip[_\s]?code|postal)[:\s=]?\S+",
                re.IGNORECASE,
            ),
            severity="high",
            description="Address component field",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="fax",
            pattern=re.compile(r"\b(?:fax|facsimile)[:\s=]?\S+", re.IGNORECASE),
            severity="high",
            description="Fax number field",
            false_positive_hints=[],
        ),
        PHIPattern(
            name="device_id",
            pattern=re.compile(
                r"\b(?:device[_\s]?(?:id|serial)|serial[_\s]?(?:number|num|no))[:\s=]?\S+",
                re.IGNORECASE,
            ),
            severity="medium",
            description="Device identifier or serial number",
            false_positive_hints=["May be sequencer ID"],
        ),
    ]

    def __init__(self) -> None:
        self._patterns: dict[str, PHIPattern] = {}
        for pattern in self.BUILTIN_PATTERNS:
            self._patterns[pattern.name] = pattern

    @property
    def patterns(self) -> list[PHIPattern]:
        return list(self._patterns.values())

    def add_pattern(self, pattern: PHIPattern) -> None:
        self._patterns[pattern.name] = pattern

    def remove_pattern(self, name: str) -> bool:
        if name in self._patterns:
            del self._patterns[name]
            return True
        return False

    def get_pattern(self, name: str) -> PHIPattern | None:
        return self._patterns.get(name)

    def get_patterns_by_severity(self, severity: str) -> list[PHIPattern]:
        return [p for p in self._patterns.values() if p.severity == severity]

    def load_custom_patterns(self, config_path: Path) -> int:
        if not config_path.exists():
            raise FileNotFoundError(f"Pattern config not found: {config_path}")

        with open(config_path, "rb") as f:
            data = tomllib.load(f)

        count = 0
        for pattern_data in data.get("patterns", []):
            pattern = PHIPattern.from_dict(pattern_data)
            self.add_pattern(pattern)
            count += 1

        return count

    def clear_custom_patterns(self) -> None:
        builtin_names = {p.name for p in self.BUILTIN_PATTERNS}
        to_remove = [name for name in self._patterns if name not in builtin_names]
        for name in to_remove:
            del self._patterns[name]
