from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from contextlib import closing

# ==============================================================================
# DATABASE CONNECTION CONFIGURATION
# ==============================================================================
DB_CONN = {
    # "dbname": "airflow",
    # "user": "airflow",
    # "password": "airflow",
    # "host": "airflow-postgres-1",  # Name of the service in Airflow's docker-compose
    'dbname': 'mydatabase',
    'user': 'user',
    'password': 'password',
    'host': 'postgres_db',
    "port": "5432"
}

# Directory where the files are located
DIRECTORY = "/opt/airflow/data/arquivos_mtrix"

# ==============================================================================
# FIXED-WIDTH FILE COLUMN SPECIFICATIONS
# (start and end positions for each field)
# ==============================================================================
COL_SPECS = [
    (0, 1),     # record type
    (1, 15),    # distribution agent CNPJ (tax id)
    (15, 33),   # client identification
    (33, 41),   # transaction date
    (41, 61),   # document number
    (61, 75),   # product code
    (75, 95),   # quantity
    (95, 111),  # sale price
    (133, 134), # document type
    (134, 144)  # client zip code
]
COL_NAMES = [
    "tipo_registro",
    "cnpj_agente_distribuicao",
    "identificacao_cliente",
    "data_transacao",
    "numero_documento",
    "codigo_produto",
    "quantidade",
    "preco_venda",
    "tipo_documento",
    "cep_cliente"
]

# ==============================================================================
# SQL QUERY TO CREATE THE TABLE (IF IT DOES NOT EXIST)
# ==============================================================================
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS mtrix_vendas (
    id SERIAL PRIMARY KEY,
    tipo_registro VARCHAR(1),
    cnpj_agente_distribuicao VARCHAR(14),
    identificacao_cliente VARCHAR(18),
    data_transacao DATE,
    numero_documento VARCHAR(20),
    codigo_produto VARCHAR(14),
    quantidade NUMERIC(18,4),
    preco_venda NUMERIC(18,4),
    tipo_documento VARCHAR(1),
    cep_cliente VARCHAR(9),
    nome_arquivo VARCHAR(255),
    data_inclusao TIMESTAMP
);
"""

# ==============================================================================
# SQL QUERY TO CHECK IF THE FILE HAS ALREADY BEEN PROCESSED
# ==============================================================================
CHECK_FILE_PROCESSED_QUERY = """
SELECT 1
FROM public.mtrix_vendas
WHERE nome_arquivo = %s
LIMIT 1;
"""

# ==============================================================================
# SQL QUERY TO INSERT DATA INTO THE TABLE
# ==============================================================================
INSERT_SQL = """
INSERT INTO mtrix_vendas (
    tipo_registro,
    cnpj_agente_distribuicao,
    identificacao_cliente,
    data_transacao,
    numero_documento,
    codigo_produto,
    quantidade,
    preco_venda,
    tipo_documento,
    cep_cliente,
    nome_arquivo,
    data_inclusao
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

def process_txt_to_postgres():
    """
    Process all files in the specified DIRECTORY that start with "VENDAS" and end with ".txt".
    
    For each file:
     - Check if it has already been processed.
     - Read the fixed-width file while skipping the header line.
     - Clean, parse, and convert data (dates, numeric values).
     - If the file is empty after skipping the header (i.e., only the header is present),
       insert a record with all nulls (except the file name and inclusion date) to mark it as processed.
     - Insert valid rows into the PostgreSQL table.
    """
    # List all VENDAS*.txt files in the directory
    txt_files = [
        f for f in os.listdir(DIRECTORY)
        if f.startswith("VENDAS") and f.endswith(".txt")
    ]
    if not txt_files:
        print(f"No VENDAS*.txt files found in: {DIRECTORY}")
        return

    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            # Create the table if it does not exist
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()

            for file_name in txt_files:
                # Check if the file has already been processed
                cur.execute(CHECK_FILE_PROCESSED_QUERY, (file_name,))
                already_processed = cur.fetchone()

                if already_processed:
                    print(f"File {file_name} has already been processed. Skipping.")
                    continue

                full_path = os.path.join(DIRECTORY, file_name)
                print(f"Processing file: {full_path}")

                # Read the fixed-width file while skipping the header line ("H")
                df = pd.read_fwf(
                    full_path,
                    colspecs=COL_SPECS,
                    names=COL_NAMES,
                    dtype=str,
                    skiprows=1
                )

                # Remove extra spaces from string columns
                df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

                # If the DataFrame is empty (file contains only a header), mark the file as processed
                if df.empty:
                    cur.execute(INSERT_SQL, (
                        None, None, None, None, None, None,
                        None, None, None, None,
                        file_name,
                        datetime.now()
                    ))
                    conn.commit()
                    print(f"⚠️ File {file_name} contains no valid data. Marked as processed.")
                    continue

                # Convert fields: transaction date, quantity, and sale price
                df["data_transacao"] = pd.to_datetime(df["data_transacao"], format='%Y%m%d', errors='coerce')
                df["quantidade"] = pd.to_numeric(df["quantidade"], errors='coerce')
                df["preco_venda"] = (
                    df["preco_venda"]
                    .str.replace(",", ".", regex=False)
                    .astype(float, errors="ignore")
                )

                # Replace invalid dates (NaT) with None
                df['data_transacao'] = df['data_transacao'].apply(lambda x: x if pd.notnull(x) else None)
                # Replace NaN with None in numeric fields
                df["quantidade"] = df["quantidade"].where(pd.notnull(df["quantidade"]), None)
                df["preco_venda"] = df["preco_venda"].where(pd.notnull(df["preco_venda"]), None)

                # Replace NaN with None for text fields (if any)
                for col in ["tipo_registro", "cnpj_agente_distribuicao", "identificacao_cliente",
                            "numero_documento", "codigo_produto", "tipo_documento", "cep_cliente"]:
                    df[col] = df[col].where(pd.notnull(df[col]), None)

                # Insert each record into the PostgreSQL table
                for _, row in df.iterrows():
                    data_inclusao = datetime.now()
                    record = (
                        row["tipo_registro"],
                        row["cnpj_agente_distribuicao"],
                        row["identificacao_cliente"],
                        row["data_transacao"],
                        row["numero_documento"],
                        row["codigo_produto"],
                        row["quantidade"],
                        row["preco_venda"],
                        row["tipo_documento"],
                        row["cep_cliente"],
                        file_name,      # file name
                        data_inclusao,  # inclusion date
                    )
                    cur.execute(INSERT_SQL, record)

                conn.commit()
                print(f"✅ Inserted {len(df)} records from {file_name}!")

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
default_args = {
    "owner": "user",
    "start_date": datetime(2025, 3, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="mtrix_vendas_edi",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_process_txt = PythonOperator(
        task_id="process_txt_to_postgres",
        python_callable=process_txt_to_postgres,
    )

    task_process_txt
