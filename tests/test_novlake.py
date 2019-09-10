from novlake import __version__
from novlake.lake import Lake

def test_version():
    assert __version__ == '0.1.0'


def test_lake_init():
    lake = Lake(user_name="pierre")

    df = lake.query("select id from dumps.loans LIMIT 10")

    assert len(df) == 10