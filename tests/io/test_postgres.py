from unittest import mock

from beam_postgres.io.postgres import _PostgresWriteFn


@mock.patch("psycopg.connect")
def test_if_writes_are_in_batches(mock_connect: mock.Mock):
    mock_conn = mock_connect.return_value

    fn = _PostgresWriteFn("conninfo", "statement", 2)
    fn.start_bundle()

    fn.process((1, "data"))
    fn.process((2, "data"))
    fn.process((3, "data"))
    fn.process((4, "data"))
    fn.process((5, "data"))

    assert 2 == mock_conn.commit.call_count
