"""Reference panel support for PRS analysis."""

from .hapmap3 import HapMap3Loader, match_hapmap3_variant
from .schema import ReferenceSchemaManager

__all__ = [
    "HapMap3Loader",
    "ReferenceSchemaManager",
    "match_hapmap3_variant",
]
