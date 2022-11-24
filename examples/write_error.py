from dataclasses import dataclass

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_postgres.io import RetryRowOnTransientErrorStrategy, WriteToPostgres


@dataclass
class Example:
    data: str


with beam.Pipeline(options=PipelineOptions()) as p:
    data = p | "Reading example records" >> beam.Create(
        [
            Example("x" * 300),
            Example("y" * 100),
        ]
    )
    failed_rows_with_errors = (
        data
        | "Writing example records to database"
        >> WriteToPostgres(
            "host=localhost dbname=examples user=postgres password=postgres",
            "insert into sink (data) values (%s)",
            retry_strategy=RetryRowOnTransientErrorStrategy(),
            max_retries=2,
        )
    )
    failed_rows_with_errors | "Print errors" >> beam.Map(print)
