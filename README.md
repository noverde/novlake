# novlake

Utils to work with our data lake.

## Install

```bash
python3 -m pip install git+https://github.com/noverde/novlake#egg=novlake

```


## Config

Create .env file in home directory with:

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
  pierre:
    notebook_path: s3://sample-notebooks/pierre/
    athena_schema_name: user_pierre
    s3_repo: s3://sample-repo/user_pierre/
    athena_output: s3://aws-athena-query-results-sample/novlake/user_pierre/
```