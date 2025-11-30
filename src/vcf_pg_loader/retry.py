"""Retry logic with exponential backoff for transient failures."""

import asyncio
import functools
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")

OnRetryCallback = Callable[[int, Exception, float], None]


class RetryExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""

    pass


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number.

        Uses exponential backoff with optional jitter.
        """
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        delay = min(delay, self.max_delay)

        if self.jitter:
            delay = delay * (0.5 + random.random())

        return delay


def retry_async(
    config: RetryConfig | None = None,
    retry_on: tuple[type[Exception], ...] = (Exception,),
    on_retry: OnRetryCallback | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator for async functions with retry logic.

    Args:
        config: Retry configuration. Defaults to RetryConfig().
        retry_on: Tuple of exception types to retry on.
        on_retry: Optional callback called before each retry.

    Returns:
        Decorated function with retry behavior.
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e

                    if attempt == config.max_attempts:
                        break

                    delay = config.get_delay(attempt)

                    if on_retry is not None:
                        on_retry(attempt, e, delay)

                    await asyncio.sleep(delay)
                except Exception:
                    raise

            raise RetryExhaustedError(
                f"Failed after {config.max_attempts} attempts"
            ) from last_exception

        return wrapper

    return decorator
