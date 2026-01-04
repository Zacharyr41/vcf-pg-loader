"""Imputation quality metrics parsing.

Supports extraction of imputation quality scores from:
- Minimac4 / Michigan Imputation Server: R2, IMPUTED, TYPED flags
- Beagle 5.x: DR2, IMP flag
- IMPUTE2: INFO score

Field mapping:
| Source   | Score Field | R² Field | Imputed Flag | Typed Flag |
|----------|-------------|----------|--------------|------------|
| Minimac4 | R2          | R2       | IMPUTED      | TYPED      |
| Beagle   | DR2         | DR2      | IMP          | (inferred) |
| IMPUTE2  | INFO        | INFO     | (inferred)   | -          |
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ImputationSource(Enum):
    """Imputation software source identifier."""

    MINIMAC4 = "minimac4"
    BEAGLE = "beagle"
    IMPUTE2 = "impute2"
    AUTO = "auto"
    UNKNOWN = "unknown"

    @classmethod
    def from_string(cls, value: str) -> "ImputationSource":
        value_lower = value.lower()
        for source in cls:
            if source.value == value_lower:
                return source
        return cls.UNKNOWN


@dataclass
class ImputationMetrics:
    """Imputation quality metrics extracted from a variant."""

    info_score: float | None = None
    imputation_r2: float | None = None
    is_imputed: bool = False
    is_typed: bool = False
    source: str | None = None


@dataclass
class ImputationConfig:
    """Configuration for imputation quality extraction."""

    source: str = "auto"
    min_info_score: float | None = None

    def get_source_enum(self) -> ImputationSource:
        return ImputationSource.from_string(self.source)

    def should_filter_variant(self, info_score: float | None) -> bool:
        if self.min_info_score is None:
            return False
        if info_score is None:
            return False
        return info_score < self.min_info_score


@dataclass
class ImputationHeaderInfo:
    """Information about imputation fields in VCF header."""

    has_r2: bool = False
    has_dr2: bool = False
    has_info_score: bool = False
    has_imputed_flag: bool = False
    has_typed_flag: bool = False
    has_imp_flag: bool = False
    detected_source: ImputationSource | None = None
    source_string: str | None = None


def detect_imputation_source(header: str) -> ImputationSource:
    """Detect imputation source from VCF header content.

    Args:
        header: Raw VCF header string

    Returns:
        Detected ImputationSource enum value
    """
    header_lower = header.lower()

    if "minimac" in header_lower or "michigan imputation server" in header_lower:
        return ImputationSource.MINIMAC4

    if "beagle" in header_lower:
        return ImputationSource.BEAGLE

    if "impute2" in header_lower:
        return ImputationSource.IMPUTE2

    if re.search(r"##INFO=<ID=R2,", header):
        return ImputationSource.MINIMAC4

    if re.search(r"##INFO=<ID=DR2,", header):
        return ImputationSource.BEAGLE

    if re.search(r"##INFO=<ID=INFO,.*Type=Float", header):
        return ImputationSource.IMPUTE2

    return ImputationSource.UNKNOWN


def parse_imputation_header(header: str) -> ImputationHeaderInfo:
    """Parse VCF header to extract imputation-related field information.

    Args:
        header: Raw VCF header string

    Returns:
        ImputationHeaderInfo with detected fields
    """
    info = ImputationHeaderInfo()

    info.has_r2 = bool(re.search(r"##INFO=<ID=R2,", header))
    info.has_dr2 = bool(re.search(r"##INFO=<ID=DR2,", header))
    info.has_info_score = bool(re.search(r"##INFO=<ID=INFO,.*Type=Float", header))
    info.has_imputed_flag = bool(re.search(r"##INFO=<ID=IMPUTED,", header))
    info.has_typed_flag = bool(re.search(r"##INFO=<ID=TYPED,", header))
    info.has_imp_flag = bool(re.search(r"##INFO=<ID=IMP,", header))

    source_match = re.search(r"##source=(.+)", header)
    if source_match:
        info.source_string = source_match.group(1).strip()

    info.detected_source = detect_imputation_source(header)

    return info


def _safe_float(value: Any) -> float | None:
    """Safely convert value to float."""
    if value is None:
        return None

    if isinstance(value, list | tuple):
        if len(value) > 0:
            value = value[0]
        else:
            return None

    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _is_truthy(value: Any) -> bool:
    """Check if a value represents a truthy imputation flag."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def extract_imputation_metrics(
    info: dict[str, Any],
    source: ImputationSource,
) -> ImputationMetrics:
    """Extract imputation quality metrics from variant INFO fields.

    Args:
        info: Dictionary of INFO field values
        source: The imputation source to use for field mapping

    Returns:
        ImputationMetrics with extracted values
    """
    metrics = ImputationMetrics()

    if source == ImputationSource.UNKNOWN:
        return metrics

    if source == ImputationSource.AUTO:
        if "R2" in info:
            source = ImputationSource.MINIMAC4
        elif "DR2" in info:
            source = ImputationSource.BEAGLE
        elif "INFO" in info:
            source = ImputationSource.IMPUTE2
        else:
            return metrics

    if source == ImputationSource.MINIMAC4:
        r2 = _safe_float(info.get("R2"))
        metrics.info_score = r2
        metrics.imputation_r2 = r2
        metrics.is_imputed = _is_truthy(info.get("IMPUTED"))
        metrics.is_typed = _is_truthy(info.get("TYPED"))
        metrics.source = "minimac4"

    elif source == ImputationSource.BEAGLE:
        dr2 = _safe_float(info.get("DR2"))
        metrics.info_score = dr2
        metrics.imputation_r2 = dr2
        metrics.is_imputed = _is_truthy(info.get("IMP"))
        if dr2 is not None and dr2 >= 1.0 and not metrics.is_imputed:
            metrics.is_typed = True
        metrics.source = "beagle"

    elif source == ImputationSource.IMPUTE2:
        info_score = _safe_float(info.get("INFO"))
        metrics.info_score = info_score
        metrics.imputation_r2 = info_score
        if info_score is not None:
            metrics.is_imputed = True
        metrics.source = "impute2"

    return metrics


def filter_by_info_score(
    variants: list[dict[str, Any]],
    min_score: float | None,
) -> tuple[list[dict[str, Any]], int]:
    """Filter variants by minimum info score threshold.

    Args:
        variants: List of variant dictionaries with 'info_score' key
        min_score: Minimum info score threshold (None means no filtering)

    Returns:
        Tuple of (filtered_variants, skipped_count)
    """
    if min_score is None:
        return variants, 0

    filtered = []
    skipped = 0

    for v in variants:
        score = v.get("info_score")
        if score is None or score >= min_score:
            filtered.append(v)
        else:
            skipped += 1

    return filtered, skipped
