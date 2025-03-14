from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from contextlib import closing

# ==============================================================================
# CONFIGURAÇÕES DE CONEXÃO
# ==============================================================================
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "airflow-postgres-1",
    "port": "5432"
}

# Diretório onde estão os arquivos
DIRECTORY = "/opt/airflow/data/arquivos_mtrix"

# ==============================================================================
# DEFINIÇÃO DOS CAMPOS
# ==============================================================================
COL_SPECS = [
    (0, 1),     # tipo_registro
    (1, 15),    # cnpj_agente_distribuicao
    (15, 33),   # identificacao_cliente
    (33, 41),   # data_transacao
    (41, 61),   # numero_documento
    (61, 75),   # codigo_produto
    (75, 95),   # quantidade
    (95, 111),  # preco_venda
    (133, 134), # tipo_documento
    (134, 144)  # cep_cliente
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
# CRIAÇÃO DA TABELA
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
# CONSULTA QUE VERIFICA SE O ARQUIVO FOI PROCESSADO
# ==============================================================================
CHECK_FILE_PROCESSED_QUERY = """
SELECT 1
FROM public.mtrix_vendas
WHERE nome_arquivo = %s
LIMIT 1;
"""

# ==============================================================================
# INSERT DOS DADOS
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
    # Lista os arquivos "VENDAS*.txt"
    txt_files = [
        f for f in os.listdir(DIRECTORY)
        if f.startswith("VENDAS") and f.endswith(".txt")
    ]
    if not txt_files:
        print(f"Nenhum arquivo VENDAS*.txt encontrado em: {DIRECTORY}")
        return

    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            # Cria tabela se não existir
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()

            for file_name in txt_files:
                # Antes de processar cada arquivo, checa se já foi processado
                cur.execute(CHECK_FILE_PROCESSED_QUERY, (file_name,))
                already_processed = cur.fetchone()

                if already_processed:
                    print(f"Arquivo {file_name} já foi processado. Pulando.")
                    continue

                full_path = os.path.join(DIRECTORY, file_name)
                print(f"Processando arquivo: {full_path}")

                # Exemplo: VENDASUN12032025140133.txt => se quiser extrair data:
                # file_name_no_ext = file_name.replace(".txt", "")
                # date_str = file_name_no_ext[8:]  # "12032025140133"
                # dt_arquivo = datetime.strptime(date_str, "%d%m%Y%H%M%S")

                # Lê ignorando cabeçalho "H"
                df = pd.read_fwf(
                    full_path,
                    colspecs=COL_SPECS,
                    names=COL_NAMES,
                    dtype=str,
                    skiprows=1
                )

                # Remove espaços extras
                df = df.apply(lambda c: c.str.strip() if c.dtype == "object" else c)

                # Converte data, quantidade, preco
                df["data_transacao"] = pd.to_datetime(df["data_transacao"], format='%Y%m%d', errors='coerce')
                df["quantidade"] = pd.to_numeric(df["quantidade"], errors='coerce')
                df["preco_venda"] = (
                    df["preco_venda"]
                    .str.replace(",", ".", regex=False)
                    .astype(float, errors="ignore")
                )

                # Substituir NaN/NaT para None
                df = df.where(pd.notnull(df), None)

                # Para cada linha do DataFrame, faz INSERT
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
                        file_name,      # nome do arquivo
                        data_inclusao,  # data_inclusao
                    )
                    cur.execute(INSERT_SQL, record)

                conn.commit()
                print(f"✅ Inseridos {len(df)} registros de {file_name}!")

# ==============================================================================
# DEFINIÇÃO DA DAG
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
