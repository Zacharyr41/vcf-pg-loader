"""PHI detection for VCF data streams.

HIPAA Reference: 164.514(b) - De-identification Standard

Scans VCF records (INFO/FORMAT fields) for potential PHI using configurable
patterns. Supports sampling for large files and produces masked output.
"""

import gzip
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .patterns import PHIPatternRegistry


@dataclass
class PHIDetection:
    """A single PHI detection result."""

    pattern_name: str
    matched_value: str
    location: str
    context: str
    severity: str
    line_number: int | None = None
    false_positive_hints: list[str] = field(default_factory=list)

    @property
    def masked_value(self) -> str:
        if len(self.matched_value) <= 4:
            return "***"
        return (
            self.matched_value[:2] + "*" * (len(self.matched_value) - 4) + self.matched_value[-2:]
        )


@dataclass
class PHIScanReport:
    """Report from scanning a VCF for PHI."""

    detections: list[PHIDetection]
    records_scanned: int
    records_total: int
    sample_rate: float

    @property
    def has_phi(self) -> bool:
        return len(self.detections) > 0

    @property
    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for d in self.detections:
            counts[d.pattern_name] = counts.get(d.pattern_name, 0) + 1
        return counts

    @property
    def severity_summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for d in self.detections:
            counts[d.severity] = counts.get(d.severity, 0) + 1
        return counts

    @property
    def risk_level(self) -> str:
        if not self.detections:
            return "none"
        severities = {d.severity for d in self.detections}
        if "critical" in severities:
            return "critical"
        if "high" in severities:
            return "high"
        if "medium" in severities:
            return "medium"
        return "low"


class PHIDetector:
    """Detects PHI in VCF data using pattern matching."""

    def __init__(self, registry: PHIPatternRegistry | None = None):
        self._registry = registry or PHIPatternRegistry()
        self._detections: list[PHIDetection] = []

    @property
    def registry(self) -> PHIPatternRegistry:
        return self._registry

    @property
    def detections(self) -> list[PHIDetection]:
        return self._detections.copy()

    def clear_detections(self) -> None:
        self._detections.clear()

    def scan_value(
        self, value: str, location: str, line_number: int | None = None
    ) -> list[PHIDetection]:
        detections: list[PHIDetection] = []
        for pattern in self._registry.patterns:
            for match in pattern.pattern.finditer(value):
                matched = match.group(0)
                start = max(0, match.start() - 20)
                end = min(len(value), match.end() + 20)
                context = value[start:end]
                if start > 0:
                    context = "..." + context
                if end < len(value):
                    context = context + "..."

                detection = PHIDetection(
                    pattern_name=pattern.name,
                    matched_value=matched,
                    location=location,
                    context=context,
                    severity=pattern.severity,
                    line_number=line_number,
                    false_positive_hints=pattern.false_positive_hints,
                )
                detections.append(detection)
                self._detections.append(detection)
        return detections

    def scan_vcf_record(self, record: Any, record_number: int | None = None) -> list[PHIDetection]:
        detections: list[PHIDetection] = []

        if hasattr(record, "INFO"):
            info_dict = dict(record.INFO) if hasattr(record.INFO, "__iter__") else {}
            for key, val in info_dict.items():
                if val is not None:
                    val_str = (
                        str(val)
                        if not isinstance(val, list | tuple)
                        else ",".join(str(v) for v in val)
                    )
                    location = f"INFO/{key}"
                    detections.extend(self.scan_value(val_str, location, record_number))

        if hasattr(record, "FORMAT") and record.FORMAT:
            format_fields = (
                record.FORMAT.split(":") if isinstance(record.FORMAT, str) else list(record.FORMAT)
            )
            for fmt_field in format_fields:
                location = f"FORMAT/{fmt_field}"
                detections.extend(self.scan_value(str(fmt_field), location, record_number))

        if hasattr(record, "ID") and record.ID:
            id_str = record.ID if isinstance(record.ID, str) else str(record.ID)
            if id_str and id_str != ".":
                detections.extend(self.scan_value(id_str, "ID", record_number))

        return detections

    def scan_vcf_stream(
        self,
        vcf_path: Path,
        sample_rate: float = 1.0,
        max_records: int | None = None,
        scan_headers: bool = True,
    ) -> PHIScanReport:
        self.clear_detections()
        records_scanned = 0
        records_total = 0

        opener = gzip.open if str(vcf_path).endswith(".gz") else open

        with opener(vcf_path, "rt") as f:
            line_number = 0
            for line in f:
                line_number += 1
                line = line.rstrip()

                if line.startswith("##"):
                    if scan_headers:
                        self.scan_value(line, "HEADER", line_number)
                    continue

                if line.startswith("#CHROM"):
                    parts = line.split("\t")
                    if len(parts) > 9:
                        for i, sample_id in enumerate(parts[9:], start=9):
                            self.scan_value(sample_id, f"SAMPLE_ID[{i-9}]", line_number)
                    continue

                records_total += 1

                if max_records and records_scanned >= max_records:
                    continue

                if sample_rate < 1.0 and random.random() > sample_rate:
                    continue

                records_scanned += 1
                parts = line.split("\t")
                if len(parts) < 8:
                    continue

                variant_id = parts[2] if len(parts) > 2 else ""
                if variant_id and variant_id != ".":
                    self.scan_value(variant_id, "ID", line_number)

                info_field = parts[7] if len(parts) > 7 else ""
                if info_field and info_field != ".":
                    self.scan_value(info_field, "INFO", line_number)

                if len(parts) > 8:
                    format_field = parts[8]
                    self.scan_value(format_field, "FORMAT", line_number)

                if len(parts) > 9:
                    for i, sample_data in enumerate(parts[9:]):
                        self.scan_value(sample_data, f"SAMPLE[{i}]", line_number)

        return PHIScanReport(
            detections=self._detections.copy(),
            records_scanned=records_scanned,
            records_total=records_total,
            sample_rate=sample_rate,
        )

    def mask_phi(self, value: str, detections: list[PHIDetection] | None = None) -> str:
        detections = detections or self._detections
        result = value
        matches_to_mask = []
        for detection in detections:
            for match in self._find_matches(value, detection.matched_value):
                matches_to_mask.append((match[0], match[1], detection.masked_value))

        matches_to_mask.sort(key=lambda x: x[0], reverse=True)
        for start, end, masked in matches_to_mask:
            result = result[:start] + masked + result[end:]
        return result

    def _find_matches(self, text: str, pattern: str) -> list[tuple[int, int]]:
        matches = []
        start = 0
        while True:
            idx = text.find(pattern, start)
            if idx == -1:
                break
            matches.append((idx, idx + len(pattern)))
            start = idx + 1
        return matches
