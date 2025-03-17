import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Database configuration
DB_CONN = {
    "dbname": "mydatabase",
    "user": "user",
    "password": "password",
    "host": "postgres_db",
    "port": "5432"
}

# Directory where the files are located
DIRECTORY = "/opt/airflow/data/arquivos_mtrix"

# Mapping file prefixes to their respective tables
TABLE_MAPPING = {
    "VENDASUN": "public.mtrix_vendas",
    "CLIENTESUN": "public.mtrix_clientes",
    "PRODUTOSUN": "public.mtrix_produtos"
}

def connect_to_database():
    """ Opens a connection to the database """
    return psycopg2.connect(**DB_CONN)

def get_table_name(file_name):
    """ Determines the table based on file prefix """
    for prefix, table in TABLE_MAPPING.items():
        if file_name.startswith(prefix):
            return table
    return None  # Return None if no matching table is found

def is_file_processed(file_name):
    """ Checks if the file has already been processed in the correct table """
    table_name = get_table_name(file_name)
    if table_name is None:
        print(f"⚠️ No table found for file: {file_name}")
        return False  # If no valid table, assume it has not been processed

    query = f"SELECT 1 FROM {table_name} WHERE nome_arquivo = %s LIMIT 1;"
    
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute(query, (file_name,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return result is not None  # Returns True if the file has already been processed

def check_and_remove_files():
    """ Checks files in the directory and removes those that have been processed """
    for file in os.listdir(DIRECTORY):
        file_path = os.path.join(DIRECTORY, file)

        if os.path.isfile(file_path):  # Check if it's a file
            if is_file_processed(file):
                os.remove(file_path)
                print(f"🗑️ File removed: {file}")
            else:
                print(f"✅ File not yet processed: {file}")

# Define Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 13),
    'catchup': False
}

with DAG(
    dag_id="mtrix_check_remove_files",
    default_args=default_args,
    schedule_interval=None,  # Disable automatic scheduling
    tags=["file_cleanup", "postgres"]
) as dag:


    task_check_and_remove = PythonOperator(
        task_id="mtrix_check_remove_files",
        python_callable=check_and_remove_files
    )

    task_check_and_remove
