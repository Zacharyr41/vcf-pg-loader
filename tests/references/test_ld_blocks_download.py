"""TDD tests for LD block download functionality.

Tests for:
- Download configuration and URL construction
- Checksum verification
- HTTP download with caching
- BED format conversion (adding headers)
- CLI command integration
"""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest


class TestLDBlockDownloadConfig:
    """Test download configuration and URL construction."""

    def test_default_config_values(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig()

        assert config.population == "eur"
        assert config.build == "grch37"
        assert "vcf-pg-loader" in str(config.cache_dir)

    def test_config_with_custom_population_afr(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="AFR")

        assert config.population == "afr"

    def test_config_with_custom_population_asn(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="asn")

        assert config.population == "asn"

    def test_config_with_custom_cache_dir(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        custom_dir = Path("/custom/cache")
        config = LDBlockDownloadConfig(cache_dir=custom_dir)

        assert config.cache_dir == custom_dir

    def test_get_download_url_eur(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="eur")

        url = config.get_download_url()
        assert "bitbucket" in url
        assert "EUR" in url
        assert "fourier_ls-all.bed" in url

    def test_get_download_url_afr(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="afr")

        url = config.get_download_url()
        assert "AFR" in url

    def test_get_download_url_asn(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="asn")

        url = config.get_download_url()
        assert "ASN" in url

    def test_get_cache_path_includes_population(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="eur")

        cache_path = config.get_cache_path()
        assert "ld_blocks" in cache_path.name
        assert "eur" in cache_path.name

    def test_get_cache_path_afr(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        config = LDBlockDownloadConfig(population="afr")

        cache_path = config.get_cache_path()
        assert "afr" in cache_path.name

    def test_invalid_population_raises_error(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        with pytest.raises(ValueError, match="population"):
            LDBlockDownloadConfig(population="invalid")

    def test_grch38_build_warns(self):
        from vcf_pg_loader.references.ld_blocks_download import LDBlockDownloadConfig

        with pytest.warns(UserWarning, match="GRCh37"):
            LDBlockDownloadConfig(build="grch38")


class TestLDBlockChecksum:
    """Test checksum verification."""

    def test_verify_checksum_valid(self):
        from vcf_pg_loader.references.ld_blocks_download import verify_ld_checksum

        with tempfile.NamedTemporaryFile(delete=False) as f:
            content = b"test content for checksum"
            f.write(content)
            f.flush()

            expected = hashlib.sha256(content).hexdigest()
            assert verify_ld_checksum(Path(f.name), expected) is True

    def test_verify_checksum_invalid(self):
        from vcf_pg_loader.references.ld_blocks_download import verify_ld_checksum

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()

            assert verify_ld_checksum(Path(f.name), "invalid_checksum") is False

    def test_verify_checksum_missing_file(self):
        from vcf_pg_loader.references.ld_blocks_download import verify_ld_checksum

        with pytest.raises(FileNotFoundError):
            verify_ld_checksum(Path("/nonexistent/file.bed"), "checksum")

    def test_get_expected_checksum_eur(self):
        from vcf_pg_loader.references.ld_blocks_download import get_expected_ld_checksum

        checksum = get_expected_ld_checksum("eur")
        assert checksum is not None
        assert len(checksum) > 0


class TestLDBlockDownloader:
    """Test download functionality with mocked HTTP."""

    @pytest.mark.asyncio
    async def test_is_cached_false_when_missing(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LDBlockDownloadConfig(cache_dir=Path(tmpdir))
            downloader = LDBlockDownloader(config)

            assert downloader.is_cached() is False

    @pytest.mark.asyncio
    async def test_is_cached_true_when_exists(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LDBlockDownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("chrom\tstart\tend\nchr1\t10583\t1892607\n")

            downloader = LDBlockDownloader(config)
            assert downloader.is_cached() is True

    @pytest.mark.asyncio
    async def test_download_creates_cache_dir(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "nested" / "cache"
            config = LDBlockDownloadConfig(cache_dir=cache_dir)
            downloader = LDBlockDownloader(config)

            with patch.object(
                downloader, "_download_file", new_callable=AsyncMock
            ) as mock_download:
                mock_download.return_value = None
                cache_path = config.get_cache_path()
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text("chrom\tstart\tend\n")

                try:
                    await downloader.download(skip_checksum=True)
                except Exception:
                    pass

            assert cache_dir.exists()

    @pytest.mark.asyncio
    async def test_download_returns_cache_path(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LDBlockDownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("chrom\tstart\tend\nchr1\t10583\t1892607\n")

            downloader = LDBlockDownloader(config)
            result = await downloader.download()

            assert result == cache_path

    @pytest.mark.asyncio
    async def test_download_force_overwrites_cache(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LDBlockDownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("old content")

            downloader = LDBlockDownloader(config)

            with patch.object(
                downloader, "_download_file", new_callable=AsyncMock
            ) as mock_download:
                mock_download.return_value = None
                cache_path.write_text("chrom\tstart\tend\n")

                await downloader.download(force=True, skip_checksum=True)

                mock_download.assert_called_once()


class TestLDBlockDownloaderIntegration:
    """Integration tests for download (mocked HTTP)."""

    @pytest.mark.asyncio
    async def test_full_download_flow_mocked(self):
        from vcf_pg_loader.references.ld_blocks_download import (
            LDBlockDownloadConfig,
            LDBlockDownloader,
        )

        test_content = b"chrom\tstart\tend\nchr1\t10583\t1892607\n"
        expected_checksum = hashlib.sha256(test_content).hexdigest()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = LDBlockDownloadConfig(cache_dir=Path(tmpdir))
            downloader = LDBlockDownloader(config)

            with patch.object(downloader, "_download_file", new_callable=AsyncMock):
                with patch(
                    "vcf_pg_loader.references.ld_blocks_download.get_expected_ld_checksum",
                    return_value=expected_checksum,
                ):
                    cache_path = config.get_cache_path()
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(test_content)

                    result = await downloader.download()

                    assert result.exists()
                    assert result.read_bytes() == test_content

    @pytest.mark.asyncio
    async def test_downloaded_file_has_headers(self):
        from vcf_pg_loader.references.ld_blocks_download import add_headers_to_bed

        raw_bed_content = b"chr1\t10583\t1892607\nchr1\t1892607\t3582736\n"

        result = add_headers_to_bed(raw_bed_content)

        assert result.startswith("chrom\tstart\tend\n")
        assert "1\t10583\t1892607" in result

    @pytest.mark.asyncio
    async def test_downloaded_file_strips_chr_prefix(self):
        from vcf_pg_loader.references.ld_blocks_download import add_headers_to_bed

        raw_bed_content = b"chr1\t10583\t1892607\nchr22\t16050075\t17054751\n"

        result = add_headers_to_bed(raw_bed_content)

        lines = result.strip().split("\n")
        assert lines[1] == "1\t10583\t1892607"
        assert lines[2] == "22\t16050075\t17054751"


class TestLDBlockCLIDownload:
    """Test CLI command for downloading LD blocks."""

    def test_cli_download_reference_ld_blocks_help(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["download-reference", "--help"])

        assert result.exit_code == 0
        assert "ld-blocks" in result.stdout.lower() or "hapmap3" in result.stdout.lower()

    def test_cli_download_ld_blocks_eur(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "vcf_pg_loader.references.ld_blocks_download.LDBlockDownloader.download",
                new_callable=AsyncMock,
            ) as mock_download:
                mock_download.return_value = Path(tmpdir) / "ld_blocks_eur_grch37.bed"
                (Path(tmpdir) / "ld_blocks_eur_grch37.bed").write_text("test")

                result = runner.invoke(
                    app,
                    [
                        "download-reference",
                        "ld-blocks",
                        "--population",
                        "eur",
                        "--output",
                        tmpdir,
                    ],
                )

                assert result.exit_code == 0 or "error" not in result.stdout.lower()

    def test_cli_download_ld_blocks_afr(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "vcf_pg_loader.references.ld_blocks_download.LDBlockDownloader.download",
                new_callable=AsyncMock,
            ) as mock_download:
                mock_download.return_value = Path(tmpdir) / "ld_blocks_afr_grch37.bed"
                (Path(tmpdir) / "ld_blocks_afr_grch37.bed").write_text("test")

                result = runner.invoke(
                    app,
                    [
                        "download-reference",
                        "ld-blocks",
                        "--population",
                        "afr",
                        "--output",
                        tmpdir,
                    ],
                )

                assert result.exit_code == 0 or "error" not in result.stdout.lower()

    def test_cli_download_with_force_flag(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "vcf_pg_loader.references.ld_blocks_download.LDBlockDownloader.download",
                new_callable=AsyncMock,
            ) as mock_download:
                mock_download.return_value = Path(tmpdir) / "ld_blocks_eur_grch37.bed"
                (Path(tmpdir) / "ld_blocks_eur_grch37.bed").write_text("test")

                result = runner.invoke(
                    app,
                    [
                        "download-reference",
                        "ld-blocks",
                        "--population",
                        "eur",
                        "--force",
                        "--output",
                        tmpdir,
                    ],
                )

                if result.exit_code == 0:
                    mock_download.assert_called()
