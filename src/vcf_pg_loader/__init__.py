"""VCF to PostgreSQL loader with clinical-grade compliance."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("vcf-pg-loader")
except PackageNotFoundError:
    __version__ = "0.0.0"
