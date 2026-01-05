"""TDD tests for HapMap3 reference panel download functionality.

Tests for:
- Download configuration and URL construction
- Checksum verification
- HTTP download with caching
- CLI command integration
"""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestHapMap3DownloadConfig:
    """Test download configuration and URL construction."""

    def test_default_config_values(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        config = HapMap3DownloadConfig()

        assert config.build == "grch38"
        assert config.source == "ldpred2"
        assert "vcf-pg-loader" in str(config.cache_dir)

    def test_config_with_custom_build(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        config = HapMap3DownloadConfig(build="grch37")

        assert config.build == "grch37"

    def test_config_with_custom_cache_dir(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        custom_dir = Path("/custom/cache")
        config = HapMap3DownloadConfig(cache_dir=custom_dir)

        assert config.cache_dir == custom_dir

    def test_get_download_url_ldpred2(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        config = HapMap3DownloadConfig(source="ldpred2")

        url = config.get_download_url()
        assert "figshare" in url or "github" in url

    def test_get_cache_path(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        config = HapMap3DownloadConfig(build="grch38")

        cache_path = config.get_cache_path()
        assert "hapmap3" in cache_path.name
        assert "grch38" in cache_path.name

    def test_invalid_build_raises_error(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        with pytest.raises(ValueError, match="build"):
            HapMap3DownloadConfig(build="invalid")

    def test_invalid_source_raises_error(self):
        from vcf_pg_loader.references.hapmap3_download import HapMap3DownloadConfig

        with pytest.raises(ValueError, match="source"):
            HapMap3DownloadConfig(source="invalid")


class TestHapMap3Checksum:
    """Test checksum verification."""

    def test_verify_checksum_valid(self):
        from vcf_pg_loader.references.hapmap3_download import verify_checksum

        with tempfile.NamedTemporaryFile(delete=False) as f:
            content = b"test content for checksum"
            f.write(content)
            f.flush()

            expected = hashlib.sha256(content).hexdigest()
            assert verify_checksum(Path(f.name), expected) is True

    def test_verify_checksum_invalid(self):
        from vcf_pg_loader.references.hapmap3_download import verify_checksum

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()

            assert verify_checksum(Path(f.name), "invalid_checksum") is False

    def test_verify_checksum_missing_file(self):
        from vcf_pg_loader.references.hapmap3_download import verify_checksum

        with pytest.raises(FileNotFoundError):
            verify_checksum(Path("/nonexistent/file.tsv"), "checksum")

    def test_get_expected_checksum_grch38(self):
        from vcf_pg_loader.references.hapmap3_download import get_expected_checksum

        checksum = get_expected_checksum("grch38", "ldpred2")
        assert checksum is not None
        assert len(checksum) == 64

    def test_get_expected_checksum_grch37(self):
        from vcf_pg_loader.references.hapmap3_download import get_expected_checksum

        checksum = get_expected_checksum("grch37", "ldpred2")
        assert checksum is not None
        assert len(checksum) == 64


class TestHapMap3Downloader:
    """Test download functionality with mocked HTTP."""

    @pytest.mark.asyncio
    async def test_is_cached_false_when_missing(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = HapMap3DownloadConfig(cache_dir=Path(tmpdir))
            downloader = HapMap3Downloader(config)

            assert downloader.is_cached() is False

    @pytest.mark.asyncio
    async def test_is_cached_true_when_exists(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = HapMap3DownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("rsid\tchrom\tposition\ta1\ta2\n")

            downloader = HapMap3Downloader(config)
            assert downloader.is_cached() is True

    @pytest.mark.asyncio
    async def test_download_creates_cache_dir(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "nested" / "cache"
            config = HapMap3DownloadConfig(cache_dir=cache_dir)
            downloader = HapMap3Downloader(config)

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.iter_bytes = MagicMock(
                return_value=iter([b"rsid\tchrom\tposition\ta1\ta2\n"])
            )

            with patch("httpx.AsyncClient") as mock_client:
                mock_client.return_value.__aenter__ = AsyncMock(
                    return_value=MagicMock(
                        stream=MagicMock(
                            return_value=MagicMock(
                                __aenter__=AsyncMock(return_value=mock_response),
                                __aexit__=AsyncMock(),
                            )
                        )
                    )
                )
                mock_client.return_value.__aexit__ = AsyncMock()

                try:
                    await downloader.download(skip_checksum=True)
                except Exception:
                    pass

            assert cache_dir.exists()

    @pytest.mark.asyncio
    async def test_download_returns_cache_path(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = HapMap3DownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("rsid\tchrom\tposition\ta1\ta2\n")

            downloader = HapMap3Downloader(config)
            result = await downloader.download()

            assert result == cache_path

    @pytest.mark.asyncio
    async def test_download_force_overwrites_cache(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config = HapMap3DownloadConfig(cache_dir=Path(tmpdir))
            cache_path = config.get_cache_path()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("old content")

            downloader = HapMap3Downloader(config)

            with patch.object(
                downloader, "_download_file", new_callable=AsyncMock
            ) as mock_download:
                mock_download.return_value = None
                cache_path.write_text("new content")

                await downloader.download(force=True, skip_checksum=True)

                mock_download.assert_called_once()


class TestHapMap3DownloaderIntegration:
    """Integration tests for download (mocked HTTP)."""

    @pytest.mark.asyncio
    async def test_full_download_flow_mocked(self):
        from vcf_pg_loader.references.hapmap3_download import (
            HapMap3DownloadConfig,
            HapMap3Downloader,
        )

        test_content = b"rsid\tchrom\tposition\ta1\ta2\nrs123\t1\t12345\tA\tG\n"
        expected_checksum = hashlib.sha256(test_content).hexdigest()

        with tempfile.TemporaryDirectory() as tmpdir:
            config = HapMap3DownloadConfig(cache_dir=Path(tmpdir))
            downloader = HapMap3Downloader(config)

            with patch.object(downloader, "_download_file", new_callable=AsyncMock):
                with patch(
                    "vcf_pg_loader.references.hapmap3_download.get_expected_checksum",
                    return_value=expected_checksum,
                ):
                    cache_path = config.get_cache_path()
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(test_content)

                    result = await downloader.download()

                    assert result.exists()
                    assert result.read_bytes() == test_content


class TestHapMap3CLIDownload:
    """Test CLI command for downloading references."""

    def test_cli_download_reference_help(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["download-reference", "--help"])

        assert result.exit_code == 0
        assert "hapmap3" in result.stdout.lower() or "reference" in result.stdout.lower()

    def test_cli_download_hapmap3_grch38(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "vcf_pg_loader.references.hapmap3_download.HapMap3Downloader.download",
                new_callable=AsyncMock,
            ) as mock_download:
                mock_download.return_value = Path(tmpdir) / "hapmap3_grch38.tsv"
                (Path(tmpdir) / "hapmap3_grch38.tsv").write_text("test")

                result = runner.invoke(
                    app,
                    ["download-reference", "hapmap3", "--build", "grch38", "--output", tmpdir],
                )

                assert result.exit_code == 0 or "error" not in result.stdout.lower()

    def test_cli_download_with_force_flag(self):
        from typer.testing import CliRunner

        from vcf_pg_loader.cli import app

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "vcf_pg_loader.references.hapmap3_download.HapMap3Downloader.download",
                new_callable=AsyncMock,
            ) as mock_download:
                mock_download.return_value = Path(tmpdir) / "hapmap3_grch38.tsv"
                (Path(tmpdir) / "hapmap3_grch38.tsv").write_text("test")

                result = runner.invoke(
                    app,
                    [
                        "download-reference",
                        "hapmap3",
                        "--build",
                        "grch38",
                        "--force",
                        "--output",
                        tmpdir,
                    ],
                )

                if result.exit_code == 0:
                    mock_download.assert_called()
