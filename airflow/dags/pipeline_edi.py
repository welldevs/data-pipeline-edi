"""
DAG pipeline_edi.py

Exemplo completo de DAG para:
1) Ler arquivo de texto fixo.
2) Extrair data/hora do arquivo a partir do seu nome.
3) Inserir dados (mais data/hora do arquivo) no PostgreSQL.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# Configuração do banco de dados no Airflow (ajuste conforme seu ambiente)
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "airflow-postgres-1",  # Nome do serviço no docker-compose do Airflow
    "port": "5432"
}

def get_data_arquivo_from_filename(file_path: str) -> datetime:
    """
    Extrai a data/hora do arquivo a partir do nome no formato:
      VENDASUNddMMyyyyHHmmss.txt
    Exemplo: VENDASUN12032025140133.txt -> 12/03/2025 14:01:33
    """
    file_name = os.path.basename(file_path)           # "VENDASUN12032025140133.txt"
    file_name_no_ext = file_name.replace(".txt", "")  # "VENDASUN12032025140133"
    # Pula "VENDASUN", sobra "12032025140133"
    date_str = file_name_no_ext[8:]  
    # Ajuste a formatação conforme o real padrão do arquivo
    dt_arquivo = datetime.strptime(date_str, "%d%m%Y%H%M%S")
    return dt_arquivo

def process_txt_to_postgres():
    # Ajuste o caminho se necessário. O arquivo deve existir em /opt/airflow/dags/ dentro do container
    file_path = "/opt/airflow/dags/VENDASUN12032025140133.txt"
    print("🚀 Iniciando processamento do arquivo:", file_path)

    # 1) Extrair a data/hora de geração do arquivo
    dt_arquivo = get_data_arquivo_from_filename(file_path)
    print("🕒 Data/hora do arquivo extraída:", dt_arquivo)

    # 2) Definir a leitura do arquivo de texto fixo
    col_specs = [
        (0, 1), (1, 15), (15, 33), (33, 41), (41, 61),
        (61, 75), (75, 90), (90, 106), (106, 124), (124, 144)
    ]
    col_names = [
        "tipo_registro", "cnpj_agente_distribuicao", "identificacao_cliente", 
        "data_transacao", "numero_documento", "codigo_produto", "quantidade", 
        "preco_venda", "cep_cliente", "tipo_documento"
    ]

    try:
        # 3) Ler o arquivo .txt usando pandas
        df = pd.read_fwf(file_path, colspecs=col_specs, names=col_names, dtype=str, skiprows=1)
        print("📄 Arquivo lido com sucesso! Exemplo de dados:")
        print(df.head(5))

        # 4) Limpar e converter
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df["data_transacao"] = pd.to_datetime(df["data_transacao"], format='%Y%m%d', errors='coerce')

        df["quantidade"] = df["quantidade"].astype(str).str.lstrip('0')
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors='coerce')

        df["preco_venda"] = df["preco_venda"].str.replace(',', '.', regex=False)
        df["preco_venda"] = df["preco_venda"].str.extract(r'(\d+\.\d+|\d+)')
        df["preco_venda"] = pd.to_numeric(df["preco_venda"], errors='coerce')

        df["codigo_vendedor"] = None

        print("🔍 Contagem de valores nulos antes da limpeza:")
        print(df.isna().sum())

        # Remover registros incompletos em colunas essenciais
        df = df.dropna(subset=[
            "tipo_registro", "cnpj_agente_distribuicao", "identificacao_cliente", 
            "data_transacao", "numero_documento", "codigo_produto", 
            "quantidade", "preco_venda", "tipo_documento"
        ])

        print(f"✅ {len(df)} registros válidos após a limpeza!")
        if df.empty:
            print("⚠️ Nenhum dado válido para inserir no banco!")
            return

        # 5) Conectar ao PostgreSQL
        conn = psycopg2.connect(**DB_CONN)
        cursor = conn.cursor()

        # 6) Preparar INSERT. Observe que incluímos a coluna data_arquivo
        insert_sql = """
            INSERT INTO vendas (
                tipo_registro, 
                cnpj_agente_distribuicao, 
                identificacao_cliente, 
                data_transacao, 
                numero_documento, 
                codigo_produto, 
                quantidade, 
                preco_venda, 
                codigo_vendedor, 
                cep_cliente, 
                tipo_documento, 
                data_insercao,
                data_arquivo
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        """

        # 7) Inserir cada registro no banco
        for _, row in df.iterrows():
            row_values = list(row)      # converte a linha em lista
            row_values.append(dt_arquivo)  # adicionamos a data do arquivo
            try:
                cursor.execute(insert_sql, row_values)
            except Exception as e:
                print(f"⚠️ Erro ao inserir linha: {row.to_dict()}")
                print("🚨 Detalhes do erro:", e)

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Dados inseridos com sucesso no banco!")

    except Exception as e:
        print("❌ Erro no processamento:", e)


# ==============================
# DEFINIÇÃO DO DAG NO AIRFLOW
# ==============================
default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 3, 12),
    'catchup': False
}

with DAG(
    dag_id='pipeline_edi',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_process_txt = PythonOperator(
        task_id="process_txt_to_postgres",
        python_callable=process_txt_to_postgres
    )

    task_process_txt
