import asyncio
from unittest.mock import Mock

import pytest

from hmpps_person_match.utils.retry import RetryExecutor


class TestRetry:
    """
    Test Retry class
    """

    def test_successfully_returns_on_success(self) -> None:
        """
        Test returns result straight away when successful
        """
        mock_func = Mock(return_value="Always succeeds")
        result = asyncio.run(RetryExecutor.retry(mock_func))
        assert result == "Always succeeds"
        mock_func.assert_called_once()

    def test_retries_on_exception(self) -> None:
        """
        Test retries on exceptions until successful
        """
        mock_func = Mock(
            side_effect=[
                ValueError("A value error occurred"),
                ValueError("A value error occurred"),
                "Third time lucky",
            ],
        )
        result = asyncio.run(RetryExecutor.retry(mock_func))
        assert result == "Third time lucky"
        assert mock_func.call_count == 3

    def test_max_retries_reached(self) -> None:
        """
        Test raises exception when max retries reached
        """
        mock_func = Mock(side_effect=ValueError("Always failes"))
        with pytest.raises(ValueError):
            asyncio.run(RetryExecutor.retry(mock_func))
        assert mock_func.call_count == 3

    def test_immediate_failure_on_different_exception(self) -> None:
        """
        Test that different exceptions do not trigger retries.
        """
        mock_func = Mock(side_effect=TypeError("A type error occurred"))

        with pytest.raises(TypeError):
            asyncio.run(RetryExecutor.retry(mock_func, max_attempts=3, retry_exceptions=(ValueError,)))
        mock_func.assert_called_once()
