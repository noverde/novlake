import datetime as dt
import os
import subprocess
import boto3
import re
import yaml

import awswrangler
from urllib.parse import urlparse

# The easiest and most common usage consists on calling
# load_dotenv when the application starts, which will load
# environment variables from a file named .env in the current
# directory or any of its parents or from the path specificied
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

class Lake():
    def __init__(self, user_name):
        self.user_name = user_name
        self.s3 = boto3.client('s3')

        NOVLAKE_SETTINGS = os.getenv("NOVLAKE_SETTINGS")

        if not NOVLAKE_SETTINGS:
            raise Exception("Missing NOVLAKE_SETTINGS environment variable")

        settings = urlparse(NOVLAKE_SETTINGS)
        response = self.s3.get_object(Bucket=settings.netloc, Key=settings.path.lstrip('/'))

        try:
            config = yaml.safe_load(response["Body"])
        except yaml.YAMLError as exc:
            raise exc

        if not user_name in config["users"]:
            print(config)
            raise Exception("Unknown user")

        user_config = config["users"][user_name]

        self.notebook_path = user_config["notebook_path"]        
        self.athena_schema_name = user_config["athena_schema_name"]
        self.s3_repo = user_config["s3_repo"]
        self.athena_output = user_config["athena_output"]

        self.documentation_home = config["documentation_home"]

        self.session = awswrangler.Session()
        # self.datastore = self.refresh_datastore()

    # def refresh_datastore(self):
    #     """Read datastore config file and returns it as a dict"""

    #     return dict()

    def query(self, query, database="default"):
        """Queries data using Athena and returns pandas dataframe"""

        if not re.findall(r"limit", query, re.I):
            raise Exception("Use LIMIT in your query")

        if not self.athena_output:
            raise Exception("Missing NOVLAKE_ATHENA_OUTPUT environment variable")
            
        return self.session.pandas.read_sql_athena(
            sql=query,
            database=database,
            s3_output=self.athena_output
        )

    def query_postgres(self, query, db_name="REPLICA"):
        """Query postgres database and returns result as Spark dataframe"""
        
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        return (
            spark.read
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", f'jdbc:postgresql://{os.getenv(f"PG_{db_name}_HOST")}:5432/{os.getenv(f"PG_{db_name}_DATABASE")}')
            .option("dbtable", f"({query}) t")
            .option("user", os.getenv(f"PG_{db_name}_USERNAME"))
            .option("password", os.getenv(f"PG_{db_name}_PASSWORD"))
            .load()
        )

    def export(self, dataframe, table_name, database_name=None, bucket_name=None, force_replace=False, ignore_warning=False):
        if database_name is None:
            database_name = f"user_{self.user_name}"

        if bucket_name is None:
            s3_path = self.s3_repo
        else:
            s3_path = f"s3://{bucket_name}/{database_name}/" 

        table_path = f"{s3_path}{table_name}"
        
        if not ignore_warning and "user_" not in database_name:
            raise Exception("FORBIDDEN database_name. Must be `user_xxxxx`")
            
        # check existing data
        
        try:
            existing_data = show(f"select * from {database_name}.{table_name} limit 5")
        except Exception as e:
            if "does not exist" in str(e):
                existing_data = []
            else:
                raise e
            
        if len(existing_data) > 0 and not force_replace:
            raise Exception(f"Table already contains data. Use 'force_replace=True' in order to overwrite.\nCurrent table data (5 sample rows):\n{str(existing_data)}")
        
        if len(existing_data) > 0 and force_replace:
            session.s3.delete_objects(path=table_path)
            
    
        session.pandas.to_parquet(
            dataframe=dataframe,
            database=database_name,
            path=table_path,
            # partition_cols=["col_name"],
        )
        
        print(f"Successfully exported data to S3 ({table_path}) and registered table to Athena")
        print(f"Preview data with: SELECT * FROM {database_name}.{table_name}")