import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from psycopg.rows import dict_row

from beam_postgres.io import ReadFromPostgres

options = PipelineOptions()
options.view_as(SetupOptions).save_main_session = True

with beam.Pipeline(options=options) as p:
    data = p | "Reading example records from database" >> ReadFromPostgres(
        "host=localhost dbname=examples user=postgres password=postgres",
        "select id, data from source",
        dict_row,
    )
    data | "Writing to stdout" >> beam.Map(print)
