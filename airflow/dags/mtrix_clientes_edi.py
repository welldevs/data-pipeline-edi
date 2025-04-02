from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
import chardet
from contextlib import closing
from datetime import datetime

# ------------------------------------------------------------------------------------
# Ajuste os dados conforme seu script anterior.
# Aqui estamos copiando a lógica principal para dentro de uma função `ingest_clientes`.
# ------------------------------------------------------------------------------------

COLSPECS = [
    (0, 1), (1, 15), (15, 33), (33, 73), (73, 113), (113, 143), (143, 152),
    (152, 182), (182, 212), (212, 232), (232, 272), (272, 290), (290, 300),
    (300, 310), (310, 320), (320, 326)
]

DF_COLUMNS = [
    "tipo_registro",
    "cnpj_distribuidor",
    "id_cliente",
    "razao_social",
    "endereco",
    "bairro",
    "cep",
    "cidade",
    "estado",
    "nome_responsavel",
    "telefones",
    "cpf_cnpj_cliente",
    "rota",
    "campo_reservado",
    "tipo_loja",
    "representatividade"
]

# DIRECTORY = "airflow/data/arquivos_mtrix"
DIRECTORY = "/opt/airflow/data/arquivos_mtrix"

DB_CONN = {
    "dbname": "mydatabase",
    "user":"user",
    "password":"password",
    "host":"postgres_db",
    "port":"5432"
}

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS public.mtrix_clientes (
    id SERIAL PRIMARY KEY,
    tipo_registro CHAR(1),
    cnpj_distribuidor VARCHAR(14),
    id_cliente VARCHAR(18),
    razao_social VARCHAR(40),
    endereco VARCHAR(40),
    bairro VARCHAR(30),
    cep VARCHAR(9),
    cidade VARCHAR(30),
    estado VARCHAR(30),
    nome_responsavel VARCHAR(20),
    telefones VARCHAR(40),
    cpf_cnpj_cliente VARCHAR(18),
    rota VARCHAR(10),
    campo_reservado VARCHAR(10),
    tipo_loja VARCHAR(10),
    representatividade VARCHAR(10),
    dtageracaoarquivo TIMESTAMP,
    nome_arquivo VARCHAR(255),
    data_inclusao TIMESTAMP DEFAULT NOW()
);
"""

CHECK_FILE_PROCESSED_QUERY = """
SELECT 1
FROM public.mtrix_clientes
WHERE nome_arquivo = %s
LIMIT 1;
"""

INSERT_QUERY = """
INSERT INTO public.mtrix_clientes (
    tipo_registro,
    cnpj_distribuidor,
    id_cliente,
    razao_social,
    endereco,
    bairro,
    cep,
    cidade,
    estado,
    nome_responsavel,
    telefones,
    cpf_cnpj_cliente,
    rota,
    campo_reservado,
    tipo_loja,
    representatividade,
    dtageracaoarquivo,
    nome_arquivo
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""


def extract_timestamp_from_filename(filename: str):
    """Exemplo de função para extrair data/hora do arquivo."""
    date_str = filename[10:18]  # '12032025'
    time_str = filename[18:24]  # '140133' (6 dígitos para HHMMSS)
    if len(date_str) != 8 or len(time_str) != 6:
        return None
    day = date_str[:2]
    month = date_str[2:4]
    year = date_str[4:]
    hour = time_str[:2]
    minute = time_str[2:4]
    second = time_str[4:]
    dt_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def ingest_clientes():
    """Função que faz a leitura dos arquivos e insere no Postgres."""
    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            # Cria a tabela se não existir
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()

            # Busca todos os arquivos que comecem com 'CLIENTES' e terminem com '.txt'
            files_to_process = [
                f for f in os.listdir(DIRECTORY)
                if f.startswith("CLIENTES") and f.endswith(".txt")
            ]

            for fname in files_to_process:
                full_path = os.path.join(DIRECTORY, fname)

                # Verifica se o arquivo já foi processado
                cur.execute(CHECK_FILE_PROCESSED_QUERY, (fname,))
                exists = cur.fetchone()
                if exists:
                    print(f"Arquivo '{fname}' já foi processado. Pulando.")
                    continue

                dtageracaoarquivo = extract_timestamp_from_filename(fname)


                df = pd.read_fwf(
                    full_path,
                    colspecs=COLSPECS,
                    header=None,
                    encoding='latin-1',
                    dtype=str
                )
                df.columns = DF_COLUMNS

                for _, row in df.iterrows():
                    record_data = (
                        row['tipo_registro'],
                        row['cnpj_distribuidor'],
                        row['id_cliente'],
                        row['razao_social'],
                        row['endereco'],
                        row['bairro'],
                        row['cep'],
                        row['cidade'],
                        row['estado'],
                        row['nome_responsavel'],
                        row['telefones'],
                        row['cpf_cnpj_cliente'],
                        row['rota'],
                        row['campo_reservado'],
                        row['tipo_loja'],
                        row['representatividade'],
                        dtageracaoarquivo,
                        fname
                    )
                    cur.execute(INSERT_QUERY, record_data)

                conn.commit()
                print(f"Arquivo '{fname}' processado com sucesso.")

# ------------------------------------------------------------------------------------
# Aqui vem a definição da DAG do Airflow
# ------------------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mtrix_clientes_edi",
    default_args=default_args,
    schedule_interval=None,  # ou CRON, etc. ex: "0 2 * * *"
    catchup=False
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_clientes_mtrix",
        python_callable=ingest_clientes,
    )

    ingest_task
