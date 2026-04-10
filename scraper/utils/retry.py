"""
Retry utility with exponential backoff for the Upshift scraper.

Usage:
    from utils.retry import retry_with_backoff, TransientError

    result = retry_with_backoff(
        lambda: requests.get(url, timeout=20),
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
    )
"""

from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar, Tuple, Type

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransientError(Exception):
    """Raised by callers to signal that a failure is retryable."""


def retry_with_backoff(
    fn: Callable[[], T],
    max_retries: int = 3,
    base_delay: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (),
    retryable_check: Callable[[Exception], bool] | None = None,
    label: str = "",
) -> T:
    """
    Call *fn* up to *max_retries + 1* times with exponential backoff.

    Parameters
    ----------
    fn:
        Zero-argument callable to invoke.
    max_retries:
        Maximum number of retry attempts after the first failure.
        Total calls = max_retries + 1.
    base_delay:
        Seconds to wait before the first retry.  Subsequent retries wait
        base_delay * 2^(attempt-1) seconds (capped at 60 s).
    retryable_exceptions:
        Tuple of exception types that should trigger a retry.
        ``TransientError`` is always included.
    retryable_check:
        Optional callable(exc) → bool.  If provided, any exception not
        already matched by *retryable_exceptions* is passed here; return
        True to retry, False to re-raise immediately.
    label:
        Human-readable description used in log messages.

    Returns
    -------
    Whatever *fn* returns on success.

    Raises
    ------
    The last exception raised by *fn* after all retries are exhausted.
    """
    retryable = (TransientError,) + retryable_exceptions
    prefix = f"[retry:{label}] " if label else "[retry] "
    last_exc: Exception = RuntimeError("retry_with_backoff: no attempts made")

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            is_retryable = isinstance(exc, retryable)
            if not is_retryable and retryable_check is not None:
                is_retryable = retryable_check(exc)

            if not is_retryable:
                logger.debug("%sNon-retryable error on attempt %d: %s", prefix, attempt + 1, exc)
                raise

            if attempt >= max_retries:
                logger.warning(
                    "%sGiving up after %d attempt(s): %s",
                    prefix, attempt + 1, exc,
                )
                break

            delay = min(base_delay * (2 ** attempt), 60.0)
            logger.warning(
                "%sTransient error on attempt %d/%d: %s — retrying in %.1fs",
                prefix, attempt + 1, max_retries + 1, exc, delay,
            )
            time.sleep(delay)

    raise last_exc
