from abc import ABC, abstractmethod
from typing import Any

from psycopg import Error, InterfaceError, InternalError, OperationalError


class RetryRowStrategy(ABC):
    """Abstract base class that specifies an interface for various strategies to determine if the element should be
    retried.
    """

    @abstractmethod
    def should_retry(self, element: Any, error: Error) -> bool:
        """Returns bool for given element and error if the transform should retry the element.

        Args:
            element: The element that raised the error.
            error: Error raised while running statement for the element.

        Returns:
            True if the element should be retried, False otherwise.
        """
        pass


class AlwaysRetryRowStrategy(RetryRowStrategy):
    """RetryRowStrategy implementation that always returns True."""

    def should_retry(self, element: Any, error: Error) -> bool:
        return True


class RetryRowOnTransientErrorStrategy(RetryRowStrategy):
    """RetryRowStrategy implementation that allows retry of elements with transient errors."""

    _TRANSIENT_ERRORS = [
        OperationalError,
        InternalError,
        InterfaceError,
    ]

    def should_retry(self, element: Any, error: Error) -> bool:
        return type(error) in self._TRANSIENT_ERRORS
