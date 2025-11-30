"""Tests for structured logging in VCF loader."""

import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from vcf_pg_loader.loader import LoadConfig, VCFLoader

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestLoaderLogging:
    """Tests for loader logging functionality."""

    def test_loader_has_logger(self):
        """VCFLoader should have a logger attribute."""
        loader = VCFLoader("postgresql://localhost/test")
        assert hasattr(loader, "logger")
        assert isinstance(loader.logger, logging.Logger)

    def test_logger_name_is_module_name(self):
        """Logger should be named after the module."""
        loader = VCFLoader("postgresql://localhost/test")
        assert loader.logger.name == "vcf_pg_loader.loader"

    @pytest.mark.asyncio
    async def test_loader_logs_start_message(self, caplog):
        """Loader should log when starting a load operation."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        loader = VCFLoader("postgresql://localhost/test")

        with patch.object(loader, "connect", new_callable=AsyncMock):
            with patch.object(loader, "pool") as mock_pool:
                mock_pool.acquire.return_value.__aenter__ = AsyncMock()
                mock_pool.acquire.return_value.__aexit__ = AsyncMock()

                with caplog.at_level(logging.INFO):
                    try:
                        await loader.load_vcf(vcf_path)
                    except Exception:
                        pass

        assert any("Starting load" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_loader_logs_completion_message(self, caplog):
        """Loader should log when completing a load operation."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        config = LoadConfig(drop_indexes=False)
        loader = VCFLoader("postgresql://localhost/test", config)

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_conn.execute = AsyncMock()
        mock_conn.copy_records_to_table = AsyncMock()

        with patch.object(loader, "connect", new_callable=AsyncMock):
            with patch.object(loader, "pool") as mock_pool:
                mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_pool.acquire.return_value.__aexit__ = AsyncMock()
                loader.pool = mock_pool

                with caplog.at_level(logging.INFO):
                    try:
                        await loader.load_vcf(vcf_path)
                    except Exception:
                        pass

        assert any("Completed" in record.message or "loaded" in record.message.lower() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_loader_logs_batch_progress(self, caplog):
        """Loader should log progress after each batch."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        config = LoadConfig(batch_size=2, drop_indexes=False)
        loader = VCFLoader("postgresql://localhost/test", config)

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_conn.execute = AsyncMock()
        mock_conn.copy_records_to_table = AsyncMock()

        with patch.object(loader, "connect", new_callable=AsyncMock):
            with patch.object(loader, "pool") as mock_pool:
                mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_pool.acquire.return_value.__aexit__ = AsyncMock()
                loader.pool = mock_pool

                with caplog.at_level(logging.DEBUG):
                    try:
                        await loader.load_vcf(vcf_path)
                    except Exception:
                        pass

        batch_logs = [r for r in caplog.records if "batch" in r.message.lower() or "variants" in r.message.lower()]
        assert len(batch_logs) >= 1

    def test_loader_accepts_custom_logger(self):
        """Loader should accept a custom logger."""
        custom_logger = logging.getLogger("custom.test.logger")
        loader = VCFLoader("postgresql://localhost/test", logger=custom_logger)
        assert loader.logger is custom_logger

    @pytest.mark.asyncio
    async def test_loader_logs_errors(self, caplog):
        """Loader should log errors at ERROR level."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        loader = VCFLoader("postgresql://localhost/test")

        with patch.object(loader, "connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.side_effect = ConnectionError("Test connection error")

            with caplog.at_level(logging.ERROR):
                with pytest.raises(ConnectionError):
                    await loader.load_vcf(vcf_path)

        assert any(record.levelno == logging.ERROR for record in caplog.records)


class TestLogConfig:
    """Tests for logging configuration."""

    def test_load_config_has_log_level(self):
        """LoadConfig should have a log_level option."""
        config = LoadConfig(log_level="DEBUG")
        assert config.log_level == "DEBUG"

    def test_load_config_default_log_level(self):
        """LoadConfig should default to INFO log level."""
        config = LoadConfig()
        assert config.log_level == "INFO"
