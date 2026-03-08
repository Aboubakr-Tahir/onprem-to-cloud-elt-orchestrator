from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'aboubakr_tahir',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

TABLES = [
    'Address', 'Customer', 'CustomerAddress', 'Product', 
    'ProductCategory', 'ProductDescription', 'ProductModel', 
    'ProductModelProductDescription', 'SalesOrderDetail', 'SalesOrderHeader'
]

def extract_and_load_azure(table_name):
    """Etape 1 & 2 : SQL -> Azure"""
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_on_prem')
    df = mssql_hook.get_pandas_df(f"SELECT * FROM SalesLT.{table_name};")
    
    local_path = f"/tmp/{table_name}.csv"
    df.to_csv(local_path, index=False)
    
    azure_hook = WasbHook(wasb_conn_id='azure_data_lake')
    azure_hook.load_file(
        file_path=local_path,
        container_name='bronze',
        blob_name=f'adventureworks/{table_name}/{table_name}.csv',
        overwrite=True
    )
    os.remove(local_path)

with DAG(
    dag_id='elt_full_pipeline_azure_snowflake',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    for table in TABLES:
        # Tâche 1 : Ingestion vers Azure
        ingest_azure = PythonOperator(
            task_id=f'ingest_{table}_to_azure',
            python_callable=extract_and_load_azure,
            op_kwargs={'table_name': table}
        )

        # Tâche 2 : Chargement Azure vers Snowflake (Layer Silver)
        copy_to_snowflake = SnowflakeOperator(
            task_id=f'load_{table}_to_snowflake',
            snowflake_conn_id='snowflake_conn',
            sql=f"""
                TRUNCATE TABLE ADVENTUREWORKS_DW.SILVER.{table};
                
                COPY INTO ADVENTUREWORKS_DW.SILVER.{table}
                FROM @ADVENTUREWORKS_DW.BRONZE.my_azure_stage/adventureworks/{table}/{table}.csv
                FILE_FORMAT = (FORMAT_NAME = ADVENTUREWORKS_DW.SILVER.my_csv_format)
                ON_ERROR = 'CONTINUE';
            """
        )

        # La dépendance : Ingestion Azure d'abord, puis Snowflake
        ingest_azure >> copy_to_snowflake