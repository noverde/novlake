# novlake

Utils to work with our data lake.

## Install

```bash
python3 -m pip install git+https://github.com/noverde/novlake#egg=novlake

```

## Test

1. Add config of user `test` to `novlake-settings.yaml`

2. Create database `user_test` in Athena.

```
CREATE DATABASE user_test;
```

3. Run pytest

```bash
pytest

```


## Config


Create `.env` file in home directory with the following instruction:

```bash
export NOVLAKE_SETTINGS=s3://<BUCKET_NAME>/novlake-settings.yaml

```


## Exemplo

```python
from novlake.lake import Lake
lake = Lake("camila")

lake.query("SELECT * FROM dumps.loans LIMIT 10")

```


## novlake-settings.yaml

novlake-settings.yaml shall use the following schema:

```yaml
documentation_home: ""
users:
  default:
    notebook_path: s3://sample-notebooks/default/
    athena_schema_name: user_default
    s3_repo: s3://sample-repo/user_default/
    athena_output: s3://aws-athena-query-results-sample/novlake/user_default/
  test:
    notebook_path: s3://novlake-test-data/notebooks/user_test/
    athena_schema_name: user_test
    s3_repo: s3://novlake-test-data/user_test/
    athena_output: s3://novlake-test-data/athena_output/
```