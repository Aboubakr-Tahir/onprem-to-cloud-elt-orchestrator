from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import pandas as pd
import os

# Paramètres de base du pipeline
default_args = {
    'owner': 'aboubakr_tahir',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def extract_from_sql_to_azure():
    """
    Extrait la table Customer et l'envoie sur Azure.
    """
    print("1. Connexion à SQL Server...")
    # On utilise l'ID de connexion qu'on a testé tout à l'heure
    sql_hook = MsSqlHook(mssql_conn_id='mssql_on_prem')
    
    # On récupère les données dans un DataFrame Pandas
    query = "SELECT * FROM SalesLT.Customer;"
    df = sql_hook.get_pandas_df(query)
    print(f"Extraction réussie : {len(df)} lignes trouvées.")

    # On sauvegarde temporairement dans le conteneur Airflow
    local_path = "/tmp/customer_raw.csv"
    df.to_csv(local_path, index=False)

    print("2. Envoi vers Azure Data Lake...")
    azure_hook = WasbHook(wasb_conn_id='azure_data_lake')
    
    # On charge le fichier dans le conteneur 'bronze'
    azure_hook.load_file(
        file_path=local_path,
        container_name='bronze',
        blob_name='adventureworks/customer_raw.csv',
        overwrite=True
    )

    # Nettoyage du fichier temporaire
    os.remove(local_path)
    print("3. Terminé !")

# Définition du DAG
with DAG(
    dag_id='ingest_customer_poc',
    default_args=default_args,
    schedule_interval=None, # Lancement manuel
    catchup=False,
    tags=['azure', 'ingestion']
) as dag:

    ingest_task = PythonOperator(
        task_id='extract_and_load_customer',
        python_callable=extract_from_sql_to_azure
    )

    ingest_task