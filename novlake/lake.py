import datetime as dt
import os
from dotenv import load_dotenv

import subprocess
import boto3

import awswrangler

# The easiest and most common usage consists on calling
# load_dotenv when the application starts, which will load
# environment variables from a file named .env in the current
# directory or any of its parents or from the path specificied
load_dotenv()


class Lake():
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.session = awswrangler.Session()
        self.user_name = os.getenv("NOVLAKE_USERNAME")
        self.data_bucket = os.getenv("NOVLAKE_DATA_BUCKET")
        self.notebook_bucket = os.getenv("NOVLAKE_NOTEBOOK_BUCKET")
        self.athena_output = os.getenv("NOVLAKE_ATHENA_OUTPUT")
        self.datastore_schema = os.getenv("NOVLAKE_DATASTORE_SCHEMA")
        self.documentation_home = os.getenv("NOVLAKE_DOCUMENTATION_HOME")

        self.datastore = self.refresh_datastore()

    def refresh_datastore(self):
        """Read datastore config file and returns it as a dict"""

        return dict()

    def query(self, query, database="default"):
        """Queries data using Athena and returns pandas dataframe"""

        if "limit" not in query or "LIMIT not in query":
            raise Exception("Use LIMIT in your query")
            
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

