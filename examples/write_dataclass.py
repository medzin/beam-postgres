from dataclasses import dataclass

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_postgres.io import WriteToPostgres


@dataclass
class Example:
    data: str


with beam.Pipeline(options=PipelineOptions()) as p:
    data = p | "Reading example records" >> beam.Create(
        [
            Example("example1"),
            Example("example2"),
        ]
    )
    data | "Writing example records to database" >> WriteToPostgres(
        "host=localhost dbname=examples user=postgres password=postgres",
        "insert into sink (data) values (%(data)s)",
    )
