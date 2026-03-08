from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator # <-- NOUVEAU : On importe le BashOperator
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
    dag_id='elt_full_pipeline_azure_snowflake_dbt', # J'ai ajouté "_dbt" au nom pour différencier
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['azure', 'snowflake', 'dbt']
) as dag:

    # NOUVEAU : La tâche dbt définie avant la boucle
    # On utilise exactement le même chemin d'environnement virtuel que tu as testé
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        # On pointe le --profiles-dir vers le dossier partagé !
        bash_command='/home/airflow/dbt_venv/bin/dbt run --project-dir /opt/airflow/transform/adventureworks_transform --profiles-dir /opt/airflow/transform/adventureworks_transform'
    )

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

        # NOUVEAU : La chaîne de dépendance complète
        # Ingestion Azure -> Chargement Snowflake -> Exécution dbt
        ingest_azure >> copy_to_snowflake >> run_dbt_models