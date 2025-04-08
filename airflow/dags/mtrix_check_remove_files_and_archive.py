import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from contextlib import closing

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

# Archive table creation query
CREATE_ARCHIVE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS mtrix_vendas_archive (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    inclusion_date TIMESTAMP,
    file_content TEXT
);
"""

# Function to connect to the database
def connect_to_database():
    """Opens a connection to the database."""
    return psycopg2.connect(**DB_CONN)

# Function to archive a single processed file
def archive_processed_file(file_name):
    """Archives the processed file's metadata and content into the archive table."""
    full_path = os.path.join(DIRECTORY, file_name)
    print(f"Archiving file: {full_path}")

    # Open and read the file content
    try:
        with open(full_path, 'r') as file:
            file_content = file.read()
    except Exception as e:
        print(f"Failed to read file {file_name}: {e}")
        return

    # Check if the file has already been archived
    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM mtrix_vendas_archive WHERE file_name = %s LIMIT 1;", (file_name,))
            if cur.fetchone():
                print(f"File {file_name} is already archived. Skipping.")
                return  # Skip if file is already archived

            # Insert the file into the archive table
            try:
                cur.execute("""
                    INSERT INTO mtrix_vendas_archive (file_name, inclusion_date, file_content)
                    VALUES (%s, %s, %s);
                """, (file_name, datetime.now(), file_content))
                conn.commit()
                print(f"File {file_name} archived successfully.")
            except Exception as e:
                print(f"Failed to archive {file_name}: {e}")

# Function to check and remove files that have been processed
def check_and_remove_files():
    """Checks files in the directory and removes those that have been processed."""
    for file in os.listdir(DIRECTORY):
        file_path = os.path.join(DIRECTORY, file)

        if os.path.isfile(file_path):  # Check if it's a file
            if is_file_processed(file):
                os.remove(file_path)
                print(f"🗑️ File removed: {file}")
            else:
                print(f"✅ File not yet processed: {file}")

# Function to check if a file has been processed and archived
def is_file_processed(file_name):
    """Checks if the file has already been archived in the archive table."""
    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM mtrix_vendas_archive WHERE file_name = %s LIMIT 1;", (file_name,))
            result = cur.fetchone()
            return result is not None  # Returns True if the file is archived

# Define Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 13),
    'catchup': False
}

with DAG(
    dag_id="mtrix_check_remove_files_and_archive",
    default_args=default_args,
    schedule_interval=None,  # Disable automatic scheduling
    tags=["file_cleanup", "postgres", "archive"]
) as dag:

    # Task to create the archive table if it does not exist
    def create_archive_table():
        """Create the archive table if it doesn't exist."""
        with closing(psycopg2.connect(**DB_CONN)) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_ARCHIVE_TABLE_QUERY)
                conn.commit()
        print("Archive table created (if not exists).")

    # Task to archive processed files (one file at a time)
    def archive_processed_files():
        """Archives all processed files in the directory."""
        files_to_archive = os.listdir(DIRECTORY)
        for file_name in files_to_archive:
            if file_name.startswith("VENDAS") or file_name.startswith("CLIENTES") or file_name.startswith("PRODUTOS"):
                if not is_file_processed(file_name):  # Check if file is already archived
                    archive_processed_file(file_name)

    # Task to remove processed files from the directory
    task_check_and_remove = PythonOperator(
        task_id="mtrix_check_and_remove_files",
        python_callable=check_and_remove_files
    )

    # Task to create the archive table
    task_create_archive_table = PythonOperator(
        task_id="create_archive_table",
        python_callable=create_archive_table
    )

    # Task to archive processed files
    task_archive_processed_files = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files
    )

    # Define the task flow
    task_create_archive_table >> task_archive_processed_files >> task_check_and_remove
