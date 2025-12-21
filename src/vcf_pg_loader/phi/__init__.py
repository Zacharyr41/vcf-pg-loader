"""PHI (Protected Health Information) handling module.

HIPAA Reference: 164.514(b) - De-identification Standard

This module provides sample ID anonymization to remove potentially
identifiable information from VCF sample names while maintaining
secure linkage capability for authorized users.
"""

from .anonymizer import SampleAnonymizer
from .encryption import PHIEncryptor
from .schema import PHISchemaManager

__all__ = [
    "SampleAnonymizer",
    "PHIEncryptor",
    "PHISchemaManager",
]
