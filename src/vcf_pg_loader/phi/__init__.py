"""PHI (Protected Health Information) handling module.

HIPAA Reference: 164.514(b) - De-identification Standard

This module provides sample ID anonymization to remove potentially
identifiable information from VCF sample names while maintaining
secure linkage capability for authorized users.
"""

from .alerting import AlertAction, AlertConfig, AlertEvent, LoadContext, PHIAlertHandler
from .anonymizer import SampleAnonymizer
from .detector import PHIDetection, PHIDetector, PHIScanReport
from .encryption import PHIEncryptor
from .header_sanitizer import (
    PHIScanner,
    PHIScanResult,
    SanitizationConfig,
    SanitizationReport,
    SanitizedHeader,
    VCFHeaderSanitizer,
)
from .patterns import PHIPattern, PHIPatternRegistry
from .schema import PHISchemaManager

__all__ = [
    "AlertAction",
    "AlertConfig",
    "AlertEvent",
    "LoadContext",
    "PHIAlertHandler",
    "PHIDetection",
    "PHIDetector",
    "PHIEncryptor",
    "PHIPattern",
    "PHIPatternRegistry",
    "PHIScanReport",
    "PHIScanResult",
    "PHISchemaManager",
    "SampleAnonymizer",
    "SanitizationConfig",
    "SanitizationReport",
    "SanitizedHeader",
    "VCFHeaderSanitizer",
    "PHIScanner",
]
