# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Resilience utilities: retry with exponential backoff and circuit breaker."""
import functools
import logging
import time
from typing import Tuple, Type

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    backoff_base: float = 1.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """Decorator that retries a function with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts (including the first).
        backoff_base: Initial backoff delay in seconds.
        backoff_factor: Multiplier applied to backoff after each retry.
        retryable_exceptions: Tuple of exception types that trigger a retry.

    Returns:
        Decorated function.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = backoff_base
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__,
                            max_attempts,
                            e,
                        )
                        raise
                    logger.warning(
                        "%s attempt %d/%d failed: %s. Retrying in %.1fs...",
                        func.__name__,
                        attempt,
                        max_attempts,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= backoff_factor

        return wrapper

    return decorator


class CircuitBreaker:
    """Simple circuit breaker that opens after consecutive failures.

    Args:
        failure_threshold: Number of consecutive failures before opening.
        recovery_timeout: Seconds to wait before attempting recovery.
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._state = "closed"  # closed, open, half_open

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if self._state == "open":
                if time.time() - self._last_failure_time >= self._recovery_timeout:
                    self._state = "half_open"
                    logger.info("Circuit breaker half-open for %s", func.__name__)
                else:
                    raise RuntimeError(
                        f"Circuit breaker open for {func.__name__}. "
                        f"Try again in {self._recovery_timeout - (time.time() - self._last_failure_time):.0f}s."
                    )

            try:
                result = func(*args, **kwargs)
                if self._state == "half_open":
                    self._state = "closed"
                    self._failure_count = 0
                    logger.info("Circuit breaker closed for %s", func.__name__)
                return result
            except Exception:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self._failure_threshold:
                    self._state = "open"
                    logger.error(
                        "Circuit breaker opened for %s after %d failures",
                        func.__name__,
                        self._failure_count,
                    )
                raise

        return wrapper
