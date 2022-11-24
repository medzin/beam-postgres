from typing import Tuple
from unittest import mock

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from psycopg import Error, IntegrityError

from beam_postgres.io import WriteToPostgres


class TestWriteToPostgres:
    @mock.patch("psycopg.connect")
    def test_if_writes_are_in_batches(self, mock_connect: mock.Mock):
        mock_conn = mock_connect.return_value

        with TestPipeline() as p:
            data = p | "Reading example records" >> beam.Create(
                [
                    ("test_1",),
                    ("test_2",),
                    ("test_3",),
                    ("test_4",),
                    ("test_5",),
                ]
            )
            data | "Writing example records to database" >> WriteToPostgres(
                "host=localhost dbname=examples user=postgres password=postgres",
                "dummy",
                2,
            )

        assert 3 == mock_conn.commit.call_count

    @mock.patch("psycopg.connect")
    def test_if_write_errors_are_returned(self, mock_connect: mock.Mock):
        mock_connect.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = (
            IntegrityError()
        )

        with TestPipeline() as p:
            data = p | "Reading example records" >> beam.Create([("test_1",)])
            errors = data | "Writing example records to database" >> WriteToPostgres(
                "host=localhost dbname=examples user=postgres password=postgres",
                "dummy",
                2,
            )
            assert_that(
                errors,
                equal_to([(("test_1",), IntegrityError())], failed_rows_equal_fn),
            )

    @mock.patch("psycopg.connect")
    def test_if_works_with_non_global_windows(self, mock_connect: mock.Mock):
        mock_connect.return_value.cursor.return_value.__enter__.return_value.execute.side_effect = (
            IntegrityError()
        )

        with TestPipeline() as p:
            errors = (
                p
                | "Reading example records" >> beam.Create([("test_1",)])
                | beam.WindowInto(window.FixedWindows(1))
                | "Writing example records to database"
                >> WriteToPostgres(
                    "host=localhost dbname=examples user=postgres password=postgres",
                    "dummy",
                )
            )
            assert_that(
                errors,
                equal_to([(("test_1",), IntegrityError())], failed_rows_equal_fn),
            )


def failed_rows_equal_fn(
    expected: Tuple[Tuple[str], Error], actual: Tuple[Tuple[str], Error]
):
    return expected[0] == actual[0] and isinstance(actual[1], type(expected[1]))
