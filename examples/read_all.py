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
