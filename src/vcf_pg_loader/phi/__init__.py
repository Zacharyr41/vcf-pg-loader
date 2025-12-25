"""PHI (Protected Health Information) handling module.

HIPAA Reference: 164.514(b) - De-identification Standard

This module provides sample ID anonymization to remove potentially
identifiable information from VCF sample names while maintaining
secure linkage capability for authorized users.
"""

from .alerting import AlertAction, AlertConfig, AlertEvent, LoadContext, PHIAlertHandler
from .anonymizer import SampleAnonymizer, log_re_identification_warning
from .detector import PHIDetection, PHIDetector, PHIScanReport
from .encryption import (
    EncryptionStatus,
    KeyManager,
    KeyRotator,
    KeySource,
    PHIEncryptor,
    check_encryption_status,
)
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
    "EncryptionStatus",
    "KeyManager",
    "KeyRotator",
    "KeySource",
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
    "log_re_identification_warning",
    "SanitizationConfig",
    "SanitizationReport",
    "SanitizedHeader",
    "VCFHeaderSanitizer",
    "PHIScanner",
    "check_encryption_status",
]
