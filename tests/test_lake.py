from novlake import __version__
from novlake.lake import Lake

def test_version():
    assert __version__ == '0.1.1'


def test_lake_init():
    lake = Lake(user_name="pierre")

    assert lake.athena_output == "s3://aws-athena-query-results-noverde-stg-us-east-1/novlake/user_pierre/"


def test_lake_query():
    lake = Lake(user_name="pierre")

    df = lake.query("select id from dumps.loans LIMIT 10")

    assert len(df) == 10


def test_lake_export():
    lake = Lake(user_name="pierre")

    df = lake.query("select id from dumps.loans LIMIT 10")

    table_path = lake.export(df, "test", force_replace=True)

    assert table_path == "s3://noverde-data-repo/user_pierre/test"


def test_lake_query_and_export():
    lake = Lake(user_name="pierre")

    lake.query_and_export(
        query="select id from dumps.loans LIMIT 10",
        table_name="test_query_and_export",
        force_replace=True)

    assert lake.query("select count(*) AS c FROM user_pierre.test_query_and_export LIMIT 1")["c"][0] == 10
