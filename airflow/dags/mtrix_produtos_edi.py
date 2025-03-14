from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from contextlib import closing
import re

# Layout baseado no Manual do Programador (Layout 11 – Versão 3.1/3) para o arquivo de PRODUTOS:
# Cada registro de produto (tipo "V") tem 277 caracteres, com os campos:
# 1. tipo_registro            -> X(1)   [posição 1]
# 2. cnpj_fornecedor          -> X(18)  [posição 2 a 19]
# 3. razao_social_fornecedor  -> X(30)  [posição 20 a 49]
# 4. codigo_produto           -> X(14)  [posição 50 a 63]
# 5. tipo_embalagem           -> 9(1)   [posição 64]
# 6. codigo_barras            -> X(14)  [posição 65 a 78]
# 7. tipo_cod_barras          -> 9(1)   [posição 79]
# 8. nome_produto             -> X(100) [posição 80 a 179]
# 9. divisao_produto          -> X(40)  [posição 180 a 219]
# 10. campo_reservado1        -> X(30)  [posição 220 a 249]
# 11. status_produto          -> X(1)   [posição 250]
# 12. campo_reservado2        -> X(27)  [posição 251 a 277]

COLSPECS = [
    (0, 1),    # tipo_registro
    (1, 19),   # cnpj_fornecedor
    (19, 49),  # razao_social_fornecedor
    (49, 63),  # codigo_produto
    (63, 64),  # tipo_embalagem
    (64, 78),  # codigo_barras
    (78, 79),  # tipo_cod_barras
    (79, 179), # nome_produto
    (179, 219),# divisao_produto
    (219, 249),# campo_reservado1
    (249, 250),# status_produto
    (250, 277) # campo_reservado2
]

DF_COLUMNS = [
    "tipo_registro",
    "cnpj_fornecedor",
    "razao_social_fornecedor",
    "codigo_produto",
    "tipo_embalagem",
    "codigo_barras",
    "tipo_cod_barras",
    "nome_produto",
    "divisao_produto",
    "campo_reservado1",
    "status_produto",
    "campo_reservado2"
]

# Diretório onde estão os arquivos .txt
DIRECTORY = "/opt/airflow/data/arquivos_mtrix"

# Configuração da conexão com o PostgreSQL
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "airflow-postgres-1",  # Nome do serviço no docker-compose do Airflow
    "port": "5432"
}

# Criação da tabela de produtos – tamanhos ajustados conforme o layout
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS public.mtrix_produtos(
    id SERIAL PRIMARY KEY,
    tipo_registro CHAR(1),
    cnpj_fornecedor VARCHAR(18),
    razao_social_fornecedor VARCHAR(30),
    codigo_produto VARCHAR(14),
    tipo_embalagem CHAR(1),
    codigo_barras VARCHAR(14),
    tipo_cod_barras CHAR(1),
    nome_produto VARCHAR(100),
    divisao_produto VARCHAR(40),
    campo_reservado1 VARCHAR(30),
    status_produto CHAR(1),
    campo_reservado2 VARCHAR(27),
    dtageracaoarquivo TIMESTAMP,
    nome_arquivo VARCHAR(255),
    data_inclusao TIMESTAMP DEFAULT NOW()
);
"""

CHECK_FILE_PROCESSED_QUERY = """
SELECT 1
FROM public.mtrix_produtos
WHERE nome_arquivo = %s
LIMIT 1;
"""

INSERT_QUERY = """
INSERT INTO public.mtrix_produtos (
    tipo_registro,
    cnpj_fornecedor,
    razao_social_fornecedor,
    codigo_produto,
    tipo_embalagem,
    codigo_barras,
    tipo_cod_barras,
    nome_produto,
    divisao_produto,
    campo_reservado1,
    status_produto,
    campo_reservado2,
    dtageracaoarquivo,
    nome_arquivo
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

def extract_timestamp_from_filename(filename: str):
    """
    Extrai a data e hora do nome do arquivo.
    Exemplo: PRODUTOSGNX30062009153005.txt
    Onde '30062009' é a data (DDMMYYYY) e '153005' é a hora (HHMMSS).
    """
    m = re.search(r'(\d{8})(\d{6})', filename)
    if not m:
        return None
    date_str, time_str = m.groups()
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

def ingest_produtos():
    """Lê os arquivos de produtos e insere os dados no PostgreSQL."""
    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            # Cria a tabela se não existir
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()

            files_to_process = [
                f for f in os.listdir(DIRECTORY)
                if f.startswith("PRODUTOS") and f.endswith(".txt")
            ]

            for fname in files_to_process:
                full_path = os.path.join(DIRECTORY, fname)

                # Verifica se o arquivo já foi processado
                cur.execute(CHECK_FILE_PROCESSED_QUERY, (fname,))
                if cur.fetchone():
                    print(f"Arquivo '{fname}' já foi processado. Pulando.")
                    continue

                dtageracaoarquivo = extract_timestamp_from_filename(fname)

                # Leitura do arquivo: forçamos todas as colunas a serem lidas como string
                df = pd.read_fwf(
                    full_path,
                    colspecs=COLSPECS,
                    header=None,
                    encoding='latin1',
                    dtype=str
                )
                df.columns = DF_COLUMNS

                # Remove espaços em branco extras de todas as colunas
                df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

                # Seleciona somente os registros de produto (tipo "V")
                df = df[df["tipo_registro"] == "V"]

                for _, row in df.iterrows():
                    # Garantindo que cada campo seja tratado como string
                    tipo_registro    = row["tipo_registro"][:1]
                    cnpj_fornecedor  = row["cnpj_fornecedor"]
                    razao_social     = row["razao_social_fornecedor"]
                    codigo_produto   = row["codigo_produto"]
                    tipo_embalagem   = row["tipo_embalagem"][:1]
                    codigo_barras    = row["codigo_barras"]
                    tipo_cod_barras  = row["tipo_cod_barras"][:1]
                    nome_produto     = row["nome_produto"]
                    divisao_produto  = row["divisao_produto"]
                    campo_reservado1 = row["campo_reservado1"]
                    status_produto   = row["status_produto"][:1]
                    campo_reservado2 = row["campo_reservado2"]

                    record_data = (
                        tipo_registro,
                        cnpj_fornecedor,
                        razao_social,
                        codigo_produto,
                        tipo_embalagem,
                        codigo_barras,
                        tipo_cod_barras,
                        nome_produto,
                        divisao_produto,
                        campo_reservado1,
                        status_produto,
                        campo_reservado2,
                        dtageracaoarquivo,
                        fname
                    )
                    cur.execute(INSERT_QUERY, record_data)

                conn.commit()
                print(f"Arquivo '{fname}' processado com sucesso.")

# ==============================================================================
# DEFINIÇÃO DA DAG
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mtrix_produtos_edi",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    ingest_task = PythonOperator(
        task_id="ingest_produtos_mtrix",
        python_callable=ingest_produtos,
    )

    ingest_task
