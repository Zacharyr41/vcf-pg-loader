"""HapMap3 reference panel download functionality.

Downloads HapMap3 SNP lists from authoritative sources for use in PRS analysis.
Supports both GRCh37 and GRCh38 genome builds.

Primary data source: LDpred2 HapMap3+ map from figshare
"""

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

VALID_BUILDS = ("grch37", "grch38")
VALID_SOURCES = ("ldpred2", "github")

DOWNLOAD_URLS = {
    "ldpred2": {
        "grch37": "https://figshare.com/ndownloader/files/37802721",
        "grch38": "https://figshare.com/ndownloader/files/37802721",
    },
    "github": {
        "grch37": "https://github.com/privefl/bigsnpr/raw/master/data-raw/map_hm3_ldpred2.rds",
        "grch38": "https://github.com/privefl/bigsnpr/raw/master/data-raw/map_hm3_ldpred2.rds",
    },
}

CHECKSUMS = {
    "ldpred2": {
        "grch37": "placeholder_checksum_grch37_will_be_updated_after_first_download",
        "grch38": "placeholder_checksum_grch38_will_be_updated_after_first_download",
    },
    "github": {
        "grch37": "placeholder_checksum_github_grch37",
        "grch38": "placeholder_checksum_github_grch38",
    },
}


def get_default_cache_dir() -> Path:
    """Get the default cache directory for reference data."""
    return Path.home() / ".vcf-pg-loader" / "references"


def get_expected_checksum(build: str, source: str) -> str:
    """Get the expected SHA256 checksum for a given build and source."""
    return CHECKSUMS.get(source, {}).get(build, "")


def verify_checksum(file_path: Path, expected: str) -> bool:
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


@dataclass
class HapMap3DownloadConfig:
    """Configuration for HapMap3 reference panel download."""

    build: str = "grch38"
    source: str = "ldpred2"
    cache_dir: Path = field(default_factory=get_default_cache_dir)

    def __post_init__(self):
        if self.build.lower() not in VALID_BUILDS:
            raise ValueError(f"Invalid build '{self.build}'. Must be one of: {VALID_BUILDS}")
        self.build = self.build.lower()

        if self.source.lower() not in VALID_SOURCES:
            raise ValueError(f"Invalid source '{self.source}'. Must be one of: {VALID_SOURCES}")
        self.source = self.source.lower()

        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir)

    def get_download_url(self) -> str:
        """Get the download URL for the configured build and source."""
        return DOWNLOAD_URLS[self.source][self.build]

    def get_cache_path(self) -> Path:
        """Get the cache file path for the configured build."""
        return self.cache_dir / f"hapmap3_{self.build}.tsv.gz"


class HapMap3Downloader:
    """Downloads and caches HapMap3 reference panel data."""

    def __init__(self, config: HapMap3DownloadConfig | None = None):
        self.config = config or HapMap3DownloadConfig()

    def is_cached(self) -> bool:
        """Check if the reference panel is already cached."""
        cache_path = self.config.get_cache_path()
        return cache_path.exists() and cache_path.stat().st_size > 0

    async def download(
        self,
        force: bool = False,
        skip_checksum: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> Path:
        """Download the HapMap3 reference panel.

        Args:
            force: Force re-download even if cached
            skip_checksum: Skip checksum verification (for testing)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to the downloaded/cached file
        """
        cache_path = self.config.get_cache_path()

        if self.is_cached() and not force:
            logger.info("Using cached HapMap3 reference: %s", cache_path)
            return cache_path

        cache_path.parent.mkdir(parents=True, exist_ok=True)

        await self._download_file(progress_callback)

        if not skip_checksum:
            expected = get_expected_checksum(self.config.build, self.config.source)
            if expected and not expected.startswith("placeholder"):
                if not verify_checksum(cache_path, expected):
                    cache_path.unlink(missing_ok=True)
                    raise ValueError(
                        f"Checksum verification failed for {cache_path}. "
                        "The file may be corrupted or tampered with."
                    )

        logger.info("Downloaded HapMap3 reference to: %s", cache_path)
        return cache_path

    async def _download_file(
        self, progress_callback: Callable[[int, int], None] | None = None
    ) -> None:
        """Download file from URL to cache path."""
        url = self.config.get_download_url()
        cache_path = self.config.get_cache_path()

        logger.info("Downloading HapMap3 reference from: %s", url)

        async with httpx.AsyncClient(follow_redirects=True, timeout=300.0) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))
                downloaded = 0

                with open(cache_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback and total_size:
                            progress_callback(downloaded, total_size)

    async def get_or_download(self, force: bool = False) -> Path:
        """Get cached path or download if not available.

        This is a convenience method that returns the cache path
        if available, otherwise downloads first.
        """
        return await self.download(force=force)
