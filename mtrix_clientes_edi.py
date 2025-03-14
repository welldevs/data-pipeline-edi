import os
import pandas as pd
import psycopg2
from datetime import datetime
from contextlib import closing

# Especificação das colunas (colspecs) conforme fornecido
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

# Pasta onde ficam os arquivos
DIRECTORY = "airflow/dags/arquivos_mtrix"

# Configurações de conexão ao banco
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "airflow-postgres-1",  # Nome do serviço no docker-compose do Airflow
    "port": "5432"
}

# Agora o campo dtageracaoarquivo será TIMESTAMP em vez de DATE
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS public.clientes_mtrix (
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
    dtageracaoarquivo TIMESTAMP,  -- Alterado para TIMESTAMP
    nome_arquivo VARCHAR(255),
    data_inclusao TIMESTAMP DEFAULT NOW()
);
"""

CHECK_FILE_PROCESSED_QUERY = """
SELECT 1
FROM public.clientes_mtrix
WHERE nome_arquivo = %s
LIMIT 1;
"""

# Note que agora incluímos dtageracaoarquivo no INSERT como TIMESTAMP
INSERT_QUERY = """
INSERT INTO public.clientes_mtrix (
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
    """
    Extrai data e hora do arquivo a partir de um padrão do tipo:
        CLIENTESUNDDMMAAAAHHMMSS.txt
    Exemplo: CLIENTESUN12032025140133.txt
             -> 12/03/2025 às 14:01:33
    Retorna um objeto datetime em Python (no fuso local).
    """
    # "CLIENTESUN" tem 9 caracteres, então:
    #   - De [10:18] pegamos '12032025' (dia/mes/ano)
    #   - De [18:24] pegamos '140133'   (hora/minuto/segundo)
    date_str = filename[10:18]  # 8 dígitos
    time_str = filename[18:24]  # 6 dígitos

    # Por segurança, validamos o tamanho antes de processar
    if len(date_str) != 8 or len(time_str) != 6:
        return None  # ou levantar uma exceção

    day = date_str[:2]
    month = date_str[2:4]
    year = date_str[4:]
    hour = time_str[:2]
    minute = time_str[2:4]
    second = time_str[4:]

    # Montamos a string em formato yyyy-mm-dd hh:mm:ss
    dt_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
    
    # Faz o parse para datetime
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def main():
    with closing(psycopg2.connect(**DB_CONN)) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_QUERY)
            conn.commit()

            # Pega todos os arquivos que começam com "CLIENTES" e terminam com ".txt"
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

                # Extrai TIMESTAMP do nome do arquivo
                dtageracaoarquivo = extract_timestamp_from_filename(fname)

                # Leitura do arquivo
                df = pd.read_fwf(
                    full_path,
                    colspecs=COLSPECS,
                    header=None,
                    encoding='utf-8'
                )
                df.columns = DF_COLUMNS

                # Inserção
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
                        dtageracaoarquivo,  # Agora salvamos data e hora
                        fname               # Nome completo do arquivo
                    )
                    cur.execute(INSERT_QUERY, record_data)

                conn.commit()
                print(f"Arquivo '{fname}' processado com sucesso.")

if __name__ == "__main__":
    main()
