"""Tests for retry logic in VCF loader."""

import asyncio

import pytest

from vcf_pg_loader.retry import RetryConfig, RetryExhaustedError, retry_async


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_retry_config(self):
        """Should have sensible defaults."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0

    def test_custom_retry_config(self):
        """Should accept custom values."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=0.5,
            max_delay=30.0,
            exponential_base=3.0
        )
        assert config.max_attempts == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0


class TestRetryAsync:
    """Tests for the retry_async decorator."""

    @pytest.mark.asyncio
    async def test_succeeds_without_retry(self):
        """Should return result if function succeeds on first try."""
        call_count = 0

        @retry_async()
        async def success_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await success_func()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_on_exception(self):
        """Should retry when function raises exception."""
        call_count = 0

        @retry_async(RetryConfig(max_attempts=3, base_delay=0.01))
        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Temporary failure")
            return "success"

        result = await fail_then_succeed()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_attempts(self):
        """Should raise RetryExhaustedError after max attempts."""
        call_count = 0

        @retry_async(RetryConfig(max_attempts=3, base_delay=0.01))
        async def always_fail():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Persistent failure")

        with pytest.raises(RetryExhaustedError) as exc_info:
            await always_fail()

        assert call_count == 3
        assert "3 attempts" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ConnectionError)

    @pytest.mark.asyncio
    async def test_respects_retry_on_exceptions(self):
        """Should only retry on specified exception types."""
        call_count = 0

        @retry_async(
            RetryConfig(max_attempts=3, base_delay=0.01),
            retry_on=(ConnectionError,)
        )
        async def raise_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        with pytest.raises(ValueError):
            await raise_value_error()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retries_specified_exceptions(self):
        """Should retry on specified exception types."""
        call_count = 0

        @retry_async(
            RetryConfig(max_attempts=3, base_delay=0.01),
            retry_on=(ConnectionError, TimeoutError)
        )
        async def fail_with_timeout():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Timeout")
            return "success"

        result = await fail_with_timeout()
        assert result == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_exponential_backoff(self):
        """Should use exponential backoff between retries (without jitter)."""
        delays = []

        @retry_async(
            RetryConfig(max_attempts=4, base_delay=0.1, exponential_base=2.0, max_delay=10.0, jitter=False)
        )
        async def track_delays():
            if len(delays) < 3:
                delays.append(asyncio.get_event_loop().time())
                raise ConnectionError("Failure")
            delays.append(asyncio.get_event_loop().time())
            return "success"

        await track_delays()

        assert len(delays) == 4
        delay1 = delays[1] - delays[0]
        delay2 = delays[2] - delays[1]
        delay3 = delays[3] - delays[2]

        assert delay1 >= 0.08
        assert delay2 >= delay1 * 1.5
        assert delay3 >= delay2 * 1.5

    @pytest.mark.asyncio
    async def test_max_delay_cap(self):
        """Should cap delay at max_delay (without jitter)."""
        config = RetryConfig(
            max_attempts=10,
            base_delay=1.0,
            exponential_base=10.0,
            max_delay=2.0,
            jitter=False
        )

        delay = config.get_delay(5)
        assert delay <= 2.0

    @pytest.mark.asyncio
    async def test_on_retry_callback(self):
        """Should call on_retry callback before each retry."""
        retry_info = []

        def on_retry(attempt: int, exception: Exception, delay: float):
            retry_info.append({
                "attempt": attempt,
                "exception_type": type(exception).__name__,
                "delay": delay
            })

        @retry_async(
            RetryConfig(max_attempts=3, base_delay=0.01),
            on_retry=on_retry
        )
        async def fail_twice():
            if len(retry_info) < 2:
                raise ConnectionError("Failure")
            return "success"

        await fail_twice()

        assert len(retry_info) == 2
        assert retry_info[0]["attempt"] == 1
        assert retry_info[0]["exception_type"] == "ConnectionError"
        assert retry_info[1]["attempt"] == 2
