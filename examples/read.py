import apache_beam as beam
from psycopg.rows import dict_row

from beam_postgres.io import ReadFromPostgres

with beam.Pipeline() as p:
    data = p | "Reading example records from database" >> ReadFromPostgres(
        "host=localhost dbname=examples user=postgres password=postgres",
        "select id, data from source order by id asc",
        dict_row,
        1,
    )
    data | "Writing to stdout" >> beam.Map(print)
