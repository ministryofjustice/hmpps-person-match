import asyncio
import random
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


class RetryExecutor:
    """
    Class to execute potentially flaky functions
    """

    MAX_RETRY_COUNT = 3
    MAX_DELAY_MS = 1000
    DELAY_BASE_MS = 50

    JITTER_MIN = 0.8
    JITTER_MAX = 1.2

    @classmethod
    async def retry(
        cls,
        action: Callable[[], T],
        retry_exceptions: tuple = (Exception,),
        max_attempts: int = MAX_RETRY_COUNT,
        base_delay: int = DELAY_BASE_MS,
        max_delay: int = MAX_DELAY_MS,
    ) -> T:
        """
        Retries a function with a jittered backoff strategy.
        """
        for attempt in range(max_attempts):
            try:
                return action()
            except retry_exceptions as exception:
                if attempt == max_attempts - 1:
                    raise exception
                jitter = random.uniform(cls.JITTER_MIN, cls.JITTER_MAX)  # noqa: S311
                delay = min(base_delay * pow(2, attempt), max_delay) * jitter
                await asyncio.sleep(delay / 1000)
        raise Exception("Max retries exceeded")
