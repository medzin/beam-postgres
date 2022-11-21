import itertools
import logging
import time
from dataclasses import astuple, is_dataclass
from typing import Any, Iterator, List, Optional, Tuple

import psycopg
from apache_beam import Create, DoFn, ParDo, PTransform
from apache_beam.transforms.window import GlobalWindows, WindowedValue
from apache_beam.utils.retry import FuzzedExponentialIntervals
from psycopg.rows import Row, RowFactory

from beam_postgres.io.retry import RetryRowOnTransientErrorStrategy, RetryRowStrategy

DEFAULT_MAX_BATCH_WRITE_SIZE = 10000
DEFAULT_WRITE_RETRIES = 1000

_LOGGER = logging.getLogger(__name__)


class _PostgresReadFn(DoFn):
    def __init__(self, conninfo: str, query: str, row_factory: RowFactory[Row]):
        self._conninfo = conninfo
        self._query = query
        self._row_factory = row_factory

    def process(self, element):
        with psycopg.connect(self._conninfo, row_factory=self._row_factory) as conn:
            with conn.cursor() as cur:
                cur.execute(self._query)
                for record in cur:
                    yield record


class ReadAllFromPostgres(PTransform):
    """A PTransform which reads all rows from the Postgres database."""

    def __init__(self, conninfo: str, query: str, row_factory: RowFactory[Row]):
        """Initializes a read operation from the database.

        Args:
            conninfo: Psycopg connection string.
            query: SQL query to be executed.
            row_factory: Psycopg row factory to be used by the connection.
        """
        self._conninfo = conninfo
        self._query = query
        self._row_factory = row_factory

    def expand(self, input_or_inputs):
        postgres_read_fn = _PostgresReadFn(self._conninfo, self._query, self._row_factory)
        return input_or_inputs | Create([1]) | "ReadAllFromPostgres" >> ParDo(postgres_read_fn)


class _PostgresWriteFn(DoFn):
    _pg_conn: psycopg.Connection[Tuple[Any, ...]]
    _rows_buffer: List[Any]

    def __init__(
        self, conninfo: str, statement: str, batch_size: int, retry_strategy: RetryRowStrategy, max_retries: int
    ):
        self._conninfo = conninfo
        self._statement = statement
        self._max_batch_size = batch_size
        self._retry_strategy = retry_strategy
        self._max_retries = max_retries

        self._rows_buffer = []

    def start_bundle(self):
        self._pg_conn = psycopg.connect(self._conninfo)
        self._rows_buffer = []

    def process(self, element) -> Optional[Iterator[WindowedValue]]:
        self._rows_buffer.append(element)
        if len(self._rows_buffer) >= self._max_batch_size:
            return self._flush_batch()
        return None

    def finish_bundle(self) -> Optional[Iterator[WindowedValue]]:
        if self._rows_buffer:
            return self._flush_batch()
        return None

    def _execute_batch(self, batch: List[Any]) -> Optional[Tuple[Any, psycopg.Error]]:
        with self._pg_conn.cursor() as cur:
            for row in batch:
                if is_dataclass(row):
                    params = astuple(row)
                elif isinstance(row, tuple):
                    params = row
                else:
                    raise TypeError("element must be a tuple or dataclass")

                try:
                    cur.execute(self._statement, params, prepare=True)
                except psycopg.Error as err:
                    self._pg_conn.rollback()
                    return (row, err)
        self._pg_conn.commit()
        return None

    def _flush_batch(self) -> Iterator[WindowedValue]:
        batch = self._rows_buffer.copy()
        failed_rows: List[Tuple[Any, psycopg.Error]] = []
        retry_intervals = iter(
            FuzzedExponentialIntervals(initial_delay_secs=1, num_retries=self._max_retries, max_delay_secs=10 * 60)
        )
        while True:
            failed_row = self._execute_batch(batch)
            if not failed_row:
                break

            try:
                sleep_interval = next(retry_intervals)
            except StopIteration:
                raise RuntimeError("cannot process the bundle in the given number of retries")

            if not self._retry_strategy.should_retry(*failed_row):
                failed_rows.append(failed_row)
                batch.remove(failed_row[0])

            _LOGGER.info("Sleeping %s seconds before retrying write.")
            time.sleep(sleep_interval)

        self._rows_buffer = []

        return itertools.chain(
            [GlobalWindows.windowed_value((row, err)) for row, err in failed_rows],
        )


class WriteToPostgres(PTransform):
    """A PTransform which writes rows to the Postgres database.

    The transform will try to split bundles into batches with the configured size and insert them in separate
    transactions (batch after batch). If any row fails, it will use provided RetryRowStrategy implementation to decide
    if it should be retried. Elements that should not be retried are returned from the transform as a tuple with the
    corresponding error.

    There is no guarantee that the row statement will not be executed twice or more in separate transactions, so the
    provided statement should be idempotent (e.g. Postgres upserts).
    """

    def __init__(
        self,
        conninfo: str,
        statement: str,
        batch_size: int = DEFAULT_MAX_BATCH_WRITE_SIZE,
        retry_strategy: RetryRowStrategy = RetryRowOnTransientErrorStrategy(),
        max_retries: int = DEFAULT_WRITE_RETRIES,
    ):
        """Initializes a write operation to the database.

        Args:
            conninfo: Psycopg connection string.
            statement: SQL statement to be executed.
            batch_size: The number of rows to be written to Postgres per transaction.
            retry_strategy: RetryRowStrategy implementation that the transform will use to decide if the row can be
                retried. Defaults to RetryRowOnTransientErrorStrategy.
            max_retries: Max number of retries per bundle before transform will raise an exception that it cannot
                process it.
        """
        self._conninfo = conninfo
        self._statement = statement
        self._batch_size = batch_size
        self._retry_strategy = retry_strategy
        self._max_retries = max_retries

    def expand(self, input_or_inputs):
        postgres_write_fn = _PostgresWriteFn(
            self._conninfo, self._statement, self._batch_size, self._retry_strategy, self._max_retries
        )
        return input_or_inputs | "WriteToPostgres" >> ParDo(postgres_write_fn)
