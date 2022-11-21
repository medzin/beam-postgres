# beam-postgres

Light IO transforms for Postgres read/write in Apache Beam pipelines.

## Goal

The project aims to provide highly performant and customizable transforms and is
not intended to support many different SQL database engines.

## Features

- `ReadAllFromPostgres` and `WriteToPostgres` transforms
- Records can be mapped to tuples, dictionaries or dataclasses
- Writes are in configurable batches

## Usage

Printing data from the database table:

```python
import apache_beam as beam
from psycopg.rows import dict_row

from beam_postgres.io import ReadAllFromPostgres

with beam.Pipeline() as p:
    data = p | "Reading example records from database" >> ReadAllFromPostgres(
        "host=localhost dbname=examples user=postgres password=postgres",
        "select id, data from source",
        dict_row,
    )
    data | "Writing to stdout" >> beam.Map(print)

```

Writing data to the database table:

```python
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
        "insert into sink (data) values (%s)",
    )

```

See [here][examples] for more examples.

[examples]: https://github.com/medzin/beam-postgres/tree/main/examples
