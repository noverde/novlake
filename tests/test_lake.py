from novlake import __version__
from novlake.lake import Lake


def test_version():
    assert __version__ == '0.1.5'


def test_lake_init():
    lake = Lake(user_name="test")

    assert "s3://" in lake.athena_output


def test_lake_export():
    lake = Lake(user_name="test")

    import pandas as pd
    import numpy as np
    np.random.seed(1)
    random_data = pd.DataFrame(
        {"col_a" : np.random.randint(low=1, high=1000000, size=1000),
        "col_b"  : np.random.normal(0.0, 1.0, size=1000)
        })

    random_data["id"] = random_data.col_a + 2
    
    table_path = lake.export(random_data, "table_a", force_replace=True)

    assert table_path == "s3://novlake-test-data/user_test/table_a"


def test_lake_query():
    lake = Lake(user_name="test")

    df = lake.query("select id from user_test.table_a LIMIT 10")

    assert len(df) == 10


def test_lake_query_and_export():
    lake = Lake(user_name="test")

    lake.query_and_export(
        query="select id from user_test.table_a LIMIT 10",
        table_name="table_b",
        force_replace=True)

    assert lake.query("select count(*) AS c FROM user_test.table_b LIMIT 1")["c"][0] == 10

