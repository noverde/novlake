import datetime as dt
import os
import subprocess
import boto3
import re
import yaml
import pandas as pd

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

    def query(self, query, database=None, no_limit=False):
        """Queries data using Athena and returns pandas dataframe"""

        if not re.findall(r"limit", query, re.I) and re.findall(r"select", query, re.I):
            raise Exception("Use LIMIT in your query")

        if not self.athena_output:
            raise Exception("Missing NOVLAKE_ATHENA_OUTPUT environment variable")
            
        if not database:
            database = self.athena_schema_name

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
        if not self.athena_output:
            raise Exception("Missing NOVLAKE_ATHENA_OUTPUT environment variable")
            
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
            existing_data = self.query(f"select * from {database_name}.{table_name} limit 5")
        except Exception as e:
            if "does not exist" in str(e):
                existing_data = []
            else:
                existing_data = []
                print(f"Ignoring error: {e}")
            
        if len(existing_data) > 0 and not force_replace:
            raise Exception(f"Table already contains data. Use 'force_replace=True' in order to overwrite.\nCurrent table data (5 sample rows):\n{str(existing_data)}")
        
        if force_replace:
            self.session.s3.delete_objects(path=table_path)  

            query_execution_id = self.session.athena.run_query(
                query=f"DROP TABLE IF EXISTS`{database_name}.{table_name}`", database=query_database, s3_output=self.athena_output)

            query_response = self.session.athena.wait_query(
                query_execution_id=query_execution_id)

            if query_response["QueryExecution"]["Status"]["State"] in [
                    "FAILED", "CANCELLED"
            ]:
                reason = query_response["QueryExecution"]["Status"][
                    "StateChangeReason"]
                message_error = f"Query error: {reason}"
                raise AthenaQueryError(message_error)
    
        self.session.pandas.to_parquet(
            dataframe=dataframe,
            database=database_name,
            path=table_path,
            # partition_cols=["col_name"],
        )
        
        print(f"Successfully exported data to S3 ({table_path}) and registered table to Athena")
        print(f"Preview data with: lake.preview('{database_name}.{table_name}')")

        return table_path


    def query_and_export(self, query, table_name, query_database="default", database_name=None, bucket_name=None, force_replace=False, ignore_warning=False):
        """Queries data using Athena and returns pandas dataframe"""

        # TODO: this code was copied from self.export()
        
        if not self.athena_output:
            raise Exception("Missing NOVLAKE_ATHENA_OUTPUT environment variable")
            
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
            existing_data = self.query(f"select * from {database_name}.{table_name} limit 5")
        except Exception as e:
            if "does not exist" in str(e):
                existing_data = []
            else:
                existing_data = []
                print(f"Ignoring error: {e}")
            
        if len(existing_data) > 0 and not force_replace:
            raise Exception(f"Table already contains data. Use 'force_replace=True' in order to overwrite.\nCurrent table data (5 sample rows):\n{str(existing_data)}")
        
        if force_replace:
            self.session.s3.delete_objects(path=table_path)  

            query_execution_id = self.session.athena.run_query(
                query=f"DROP TABLE IF EXISTS`{database_name}.{table_name}`", database=query_database, s3_output=self.athena_output)

            query_response = self.session.athena.wait_query(
                query_execution_id=query_execution_id)

            if query_response["QueryExecution"]["Status"]["State"] in [
                    "FAILED", "CANCELLED"
            ]:
                reason = query_response["QueryExecution"]["Status"][
                    "StateChangeReason"]
                message_error = f"Query error: {reason}"
                raise AthenaQueryError(message_error)

        ctas = f"""
        CREATE TABLE {database_name}.{table_name}
        WITH (
            external_location = '{table_path}',
            format = 'Parquet',
            parquet_compression = 'SNAPPY')
        AS
        { query }
        """
    
        query_execution_id = self.session.athena.run_query(
            query=ctas, database=query_database, s3_output=self.athena_output)

        query_response = self.session.athena.wait_query(
            query_execution_id=query_execution_id)

        if query_response["QueryExecution"]["Status"]["State"] in [
                "FAILED", "CANCELLED"
        ]:
            reason = query_response["QueryExecution"]["Status"][
                "StateChangeReason"]
            message_error = f"Query error: {reason}"
            raise AthenaQueryError(message_error)

        print(f"Successfully exported data to S3 ({table_path}) and registered table to Athena")
        print(f"Preview data with: lake.preview('{database_name}.{table_name}')")

        return table_path


    def dump_pg_table(self, query, database_name, table_name, bucket="noverde-data-repo", ds=None, db_code='REPLICA'):    
        import psycopg2 as pg
                
        if ds is None:            
            ds = str(dt.date.today() - dt.timedelta(days=1))
        
        table_path = f"s3://{bucket}/{database_name}/{table_name}/ds={ds}"
        print(table_path)
        
        self.session.s3.delete_objects(path=table_path)
        
        connection = pg.connect("host='%s' dbname=%s user=%s password='%s'" % (
            os.getenv(f'PG_{db_code}_HOST'),
            os.getenv(f'PG_{db_code}_DATABASE'),
            os.getenv(f'PG_{db_code}_USERNAME'),
            os.getenv(f'PG_{db_code}_PASSWORD'),
        ))

        dataframe = pd.read_sql_query(query, con=connection)
        
        print(dataframe.dtypes)

        self.session.pandas.to_parquet(
            dataframe=dataframe,
            database=database_name,
            table=table_name,
            preserve_index=False,
            path=table_path,
            mode="overwrite"
        )

        print("Dump done!")


    def list(self, table_filter=None):
        import boto3

        client = boto3.client('glue')
        responseGetDatabases = client.get_databases()
        databaseList = responseGetDatabases['DatabaseList']

        table_rows_list = []

        for databaseDict in databaseList:
            databaseName = databaseDict['Name']

            if "user_" not in databaseName or databaseName == self.athena_schema_name:
                paginator = client.get_paginator('get_tables')

                page_iterator = paginator.paginate(
                    DatabaseName=databaseName
                )

                for page in page_iterator:
                    for table in page["TableList"]:
                        if table_filter is None or table_filter in table["Name"] or table_filter in databaseName:
                            if "dim_" not in table["Name"]:
                                table_rows_list.append(f"""
                                    <tr>
                                        <td>{databaseName}</td>
                                        <td>{table['Name']}</td>
                                        <td class='full_name'>lake.preview("{databaseName}.{table['Name']}")</td>
                                    </tr>""")

        table_rows = "\n".join(table_rows_list)

        from IPython.core.display import display, HTML
        display(HTML("""<style type="text/css">
        table.td, tableth {
            /*max-width: none;*/
            white-space: normal;
            line-height: normal;
        }
        .full_name {
            font-size: 40%;
        }
        </style>
        <table style="width:100%">
            <tr>
                <th>Database</th>
                <th>Table</th> 
                <th>Preview</th>
            </tr>""" + table_rows + """
        </table>
        """))

    def help(self):
        from IPython.core.display import display, HTML  
        display(HTML('<h3>Novlake help</h3>'))

    def preview(self, table_name, insert=True, max_columns=100):
        import pandas as pd
        pd.set_option('display.max_columns', max_columns)

        df = self.query(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 10")

        columns_list = ",\n   ".join(df.columns)

        if insert:
            try:
                get_ipython().set_next_input(f'lake.query("""\nSELECT {columns_list} \nFROM {table_name}\nLIMIT 10\n""")')
            except:
                pass

        return df


if __name__ == "__main__":
    lake = Lake("test")
