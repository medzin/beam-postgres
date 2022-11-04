from dataclasses import astuple, is_dataclass
from typing import Any, List, Optional, Tuple

import psycopg
from apache_beam import DoFn, ParDo, PTransform


class PostgresWriteFn(DoFn):
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
        postgres_write_fn = PostgresWriteFn(self._conninfo, self._statement, self._batch_size)
        return input_or_inputs | "WriteToPostgres" >> ParDo(postgres_write_fn)
