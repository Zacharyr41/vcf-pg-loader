"""Tests for progress reporting in VCF loader."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vcf_pg_loader.loader import LoadConfig, VCFLoader

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestProgressCallback:
    """Tests for progress callback functionality."""

    def test_load_config_has_progress_callback(self):
        """LoadConfig should accept a progress_callback option."""
        callback = MagicMock()
        config = LoadConfig(progress_callback=callback)
        assert config.progress_callback is callback

    def test_load_config_default_progress_callback_is_none(self):
        """LoadConfig should default to no progress callback."""
        config = LoadConfig()
        assert config.progress_callback is None

    @pytest.mark.asyncio
    async def test_progress_callback_called_per_batch(self):
        """Progress callback should be called after each batch."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        callback = MagicMock()
        config = LoadConfig(batch_size=2, drop_indexes=False, progress_callback=callback)
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

                try:
                    await loader.load_vcf(vcf_path)
                except Exception:
                    pass

        assert callback.call_count >= 1

    @pytest.mark.asyncio
    async def test_progress_callback_receives_batch_info(self):
        """Progress callback should receive batch count and total loaded."""
        vcf_path = FIXTURES_DIR / "with_annotations.vcf"
        received_calls = []

        def track_progress(batch_num: int, batch_size: int, total_loaded: int):
            received_calls.append({
                "batch_num": batch_num,
                "batch_size": batch_size,
                "total_loaded": total_loaded
            })

        config = LoadConfig(batch_size=2, drop_indexes=False, progress_callback=track_progress)
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

                try:
                    await loader.load_vcf(vcf_path)
                except Exception:
                    pass

        assert len(received_calls) >= 1
        first_call = received_calls[0]
        assert "batch_num" in first_call
        assert "batch_size" in first_call
        assert "total_loaded" in first_call
        assert first_call["batch_num"] == 1
        assert first_call["batch_size"] > 0


class TestProgressCallbackType:
    """Tests for progress callback type hints."""

    def test_progress_callback_type_annotation(self):
        """Progress callback should have proper type annotation."""
        from vcf_pg_loader.loader import ProgressCallback
        assert ProgressCallback is not None

    def test_progress_callback_callable_type(self):
        """Progress callback should be a Callable type."""
        from vcf_pg_loader.loader import ProgressCallback
        assert hasattr(ProgressCallback, "__args__") or callable(ProgressCallback)
