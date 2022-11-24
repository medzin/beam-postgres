import pytest
from psycopg import (
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)
from psycopg.errors import DeadlockDetected

from beam_postgres.io import AlwaysRetryRowStrategy, RetryRowOnTransientErrorStrategy


class TestAlwaysRetryRowStrategy:
    def test_if_returns_true(self):
        strategy = AlwaysRetryRowStrategy()
        assert strategy.should_retry(None, Error())


class TestRetryRowOnTransientErrorStrategy:
    @pytest.mark.parametrize(
        "error",
        [InterfaceError(), InternalError(), OperationalError(), DeadlockDetected()],
    )
    def test_if_returns_true_on_transient_errors(self, error: Error):
        strategy = RetryRowOnTransientErrorStrategy()
        assert strategy.should_retry(None, error)

    @pytest.mark.parametrize(
        "error", [DataError(), IntegrityError(), ProgrammingError()]
    )
    def test_if_returns_false_on_other_errors(self, error: Error):
        strategy = RetryRowOnTransientErrorStrategy()
        assert not strategy.should_retry(None, error)
