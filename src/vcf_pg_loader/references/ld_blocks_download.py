"""LD block download functionality (Berisa & Pickrell 2016).

Downloads LD block definitions from the authoritative ldetect-data repository.
Supports EUR, AFR, and ASN populations. Data is only available for GRCh37.
"""

import hashlib
import logging
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

VALID_POPULATIONS = ("eur", "afr", "asn")
VALID_BUILDS = ("grch37", "grch38")

DOWNLOAD_URLS = {
    "eur": "https://bitbucket.org/nygcresearch/ldetect-data/raw/master/EUR/fourier_ls-all.bed",
    "afr": "https://bitbucket.org/nygcresearch/ldetect-data/raw/master/AFR/fourier_ls-all.bed",
    "asn": "https://bitbucket.org/nygcresearch/ldetect-data/raw/master/ASN/fourier_ls-all.bed",
}

CHECKSUMS = {
    "eur": "placeholder_checksum_eur_will_be_updated_after_first_download",
    "afr": "placeholder_checksum_afr_will_be_updated_after_first_download",
    "asn": "placeholder_checksum_asn_will_be_updated_after_first_download",
}


def get_default_cache_dir() -> Path:
    """Get the default cache directory for reference data."""
    return Path.home() / ".vcf-pg-loader" / "references"


def get_expected_ld_checksum(population: str) -> str:
    """Get the expected SHA256 checksum for a given population."""
    return CHECKSUMS.get(population.lower(), "")


def verify_ld_checksum(file_path: Path, expected: str) -> bool:
    """Verify SHA256 checksum of a file.

    Args:
        file_path: Path to file to verify
        expected: Expected SHA256 hex digest

    Returns:
        True if checksum matches, False otherwise

    Raises:
        FileNotFoundError: If file does not exist
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest() == expected


def add_headers_to_bed(raw_content: bytes) -> str:
    """Add headers to raw BED content and strip chr prefix.

    The ldetect-data BED files have no header and use 'chr' prefix.
    This function adds headers and normalizes chromosome names.

    Args:
        raw_content: Raw BED file content (bytes)

    Returns:
        Formatted BED content with headers (string)
    """
    lines = raw_content.decode("utf-8").strip().split("\n")
    output_lines = ["chrom\tstart\tend"]

    for line in lines:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            chrom = parts[0]
            if chrom.startswith("chr"):
                chrom = chrom[3:]
            output_lines.append(f"{chrom}\t{parts[1]}\t{parts[2]}")

    return "\n".join(output_lines) + "\n"


@dataclass
class LDBlockDownloadConfig:
    """Configuration for LD block download."""

    population: str = "eur"
    build: str = "grch37"
    cache_dir: Path = field(default_factory=get_default_cache_dir)

    def __post_init__(self):
        if self.population.lower() not in VALID_POPULATIONS:
            raise ValueError(
                f"Invalid population '{self.population}'. Must be one of: {VALID_POPULATIONS}"
            )
        self.population = self.population.lower()

        if self.build.lower() not in VALID_BUILDS:
            raise ValueError(f"Invalid build '{self.build}'. Must be one of: {VALID_BUILDS}")
        self.build = self.build.lower()

        if self.build == "grch38":
            warnings.warn(
                "LD blocks are only natively available for GRCh37. "
                "GRCh38 would require liftover. Using GRCh37 coordinates.",
                UserWarning,
                stacklevel=2,
            )

        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir)

    def get_download_url(self) -> str:
        """Get the download URL for the configured population."""
        return DOWNLOAD_URLS[self.population]

    def get_cache_path(self) -> Path:
        """Get the cache file path for the configured population."""
        return self.cache_dir / f"ld_blocks_{self.population}_{self.build}.bed"


class LDBlockDownloader:
    """Downloads and caches LD block data."""

    def __init__(self, config: LDBlockDownloadConfig | None = None):
        self.config = config or LDBlockDownloadConfig()

    def is_cached(self) -> bool:
        """Check if the LD blocks are already cached."""
        cache_path = self.config.get_cache_path()
        return cache_path.exists() and cache_path.stat().st_size > 0

    async def download(
        self,
        force: bool = False,
        skip_checksum: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download the LD block definitions.

        Args:
            force: Force re-download even if cached
            skip_checksum: Skip checksum verification (for testing)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to the downloaded/cached file
        """
        cache_path = self.config.get_cache_path()

        if self.is_cached() and not force:
            logger.info("Using cached LD blocks: %s", cache_path)
            return cache_path

        cache_path.parent.mkdir(parents=True, exist_ok=True)

        await self._download_file(progress_callback)

        if not skip_checksum:
            expected = get_expected_ld_checksum(self.config.population)
            if expected and not expected.startswith("placeholder"):
                if not verify_ld_checksum(cache_path, expected):
                    cache_path.unlink(missing_ok=True)
                    raise ValueError(
                        f"Checksum verification failed for {cache_path}. "
                        "The file may be corrupted or tampered with."
                    )

        logger.info("Downloaded LD blocks to: %s", cache_path)
        return cache_path

    async def _download_file(
        self, progress_callback: Callable[[int, int], None] | None = None
    ) -> None:
        """Download file from URL to cache path."""
        url = self.config.get_download_url()
        cache_path = self.config.get_cache_path()

        logger.info("Downloading LD blocks from: %s", url)

        async with httpx.AsyncClient(follow_redirects=True, timeout=300.0) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                downloaded = 0
                raw_content = b""

                async for chunk in response.aiter_bytes(chunk_size=8192):
                    raw_content += chunk
                    downloaded += len(chunk)
                    if progress_callback and total_size:
                        progress_callback(downloaded, total_size)

        formatted_content = add_headers_to_bed(raw_content)
        with open(cache_path, "w") as f:
            f.write(formatted_content)

    async def get_or_download(self, force: bool = False) -> Path:
        """Get cached path or download if not available."""
        return await self.download(force=force)
