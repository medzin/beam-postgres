from dataclasses import astuple, is_dataclass
from typing import Any, List, Optional, Tuple

import psycopg
from apache_beam import Create, DoFn, ParDo, PTransform
from psycopg.rows import Row, RowFactory


class _PostgresReadFn(DoFn):
    def __init__(self, conninfo: str, statement: str, row_factory: RowFactory[Row]):
        self._conninfo = conninfo
        self._statement = statement
        self._row_factory = row_factory

    def process(self, element):
        with psycopg.connect(self._conninfo, row_factory=self._row_factory) as conn:
            with conn.cursor() as cur:
                cur.execute(self._statement)
                for record in cur:
                    yield record


class ReadFromPostgres(PTransform):
    def __init__(self, conninfo: str, statement: str, row_factory: RowFactory[Row]):
        self._conninfo = conninfo
        self._statement = statement
        self._row_factory = row_factory

    def expand(self, input_or_inputs):
        postgres_read_fn = _PostgresReadFn(self._conninfo, self._statement, self._row_factory)
        return input_or_inputs | Create([1]) | "ReadFromPostgres" >> ParDo(postgres_read_fn)


class _PostgresWriteFn(DoFn):
    _pg_conn: psycopg.Connection[Tuple[Any, ...]]
    _rows_buffer: List[Any]

    def __init__(self, conninfo: str, statement: str, batch_size: int):
        self._conninfo = conninfo
        self._statement = statement
        self._max_batch_size = batch_size
        self._rows_buffer = []

    def start_bundle(self):
        self._pg_conn = psycopg.connect(self._conninfo)
        self._rows_buffer = []

    def process(self, element):
        self._rows_buffer.append(element)
        if len(self._rows_buffer) >= self._max_batch_size:
            self._flush_batch()

    def finish_bundle(self):
        if self._rows_buffer:
            self._flush_batch()
        self._rows_buffer = []

    def _flush_batch(self):
        with self._pg_conn.cursor() as cur:
            for row in self._rows_buffer:
                if is_dataclass(row):
                    params = astuple(row)
                elif isinstance(row, tuple):
                    params = row
                else:
                    raise TypeError("element must be a tuple or dataclass")
                cur.execute(self._statement, params, prepare=True)
        self._pg_conn.commit()
        self._rows_buffer = []


class WriteToPostgres(PTransform):
    def __init__(self, conninfo: str, statement: str, batch_size: Optional[int] = None):
        self._conninfo = conninfo
        self._statement = statement
        self._batch_size = batch_size or 1000

    def expand(self, input_or_inputs):
        postgres_write_fn = _PostgresWriteFn(self._conninfo, self._statement, self._batch_size)
        return input_or_inputs | "WriteToPostgres" >> ParDo(postgres_write_fn)
