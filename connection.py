import pandas as pd
import psycopg2

# Configuração do banco de dados local
DB_CONN = {
    "dbname": "mydatabase",
    "user": "user",
    "password": "password",
    "host": "localhost",  # Mudar para "postgres_db" se estiver rodando dentro do Docker
    "port": "5432"
}

def process_txt_to_postgres():
    file_path = "VENDASUN12032025140133.txt"  # Certifique-se de que o arquivo está no mesmo diretório do script
    print("🚀 Iniciando processamento do arquivo:", file_path)

    # Definição das colunas
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
        # 🔍 Leitura do arquivo TXT
        df = pd.read_fwf(file_path, colspecs=col_specs, names=col_names, dtype=str, skiprows=1)
        print("📄 Arquivo lido com sucesso! Exemplo de dados:")
        print(df.head(5))

        # 🔍 Limpeza de espaços em branco
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # 🔍 Conversão de data
        df["data_transacao"] = pd.to_datetime(df["data_transacao"], format='%Y%m%d', errors='coerce')

        # 🔍 Conversão de quantidade (removendo zeros à esquerda)
        df["quantidade"] = df["quantidade"].astype(str).str.lstrip('0')
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors='coerce')

        # 🔍 Correção do `preco_venda`
        df["preco_venda"] = df["preco_venda"].str.replace(',', '.', regex=False)  # Troca vírgula por ponto
        df["preco_venda"] = df["preco_venda"].str.extract(r'(\d+\.\d+|\d+)')  # Mantém apenas números válidos
        df["preco_venda"] = pd.to_numeric(df["preco_venda"], errors='coerce')

        # 🔍 Preenchendo `codigo_vendedor` com None
        df["codigo_vendedor"] = None  

        # 🔍 Contagem de valores nulos antes da limpeza
        print("🔍 Contagem de valores nulos antes da limpeza:")
        print(df.isna().sum())

        # 🔍 **Não remover `cep_cliente` e `codigo_vendedor`, pois podem ser opcionais**
        df = df.dropna(subset=["tipo_registro", "cnpj_agente_distribuicao", "identificacao_cliente", 
                               "data_transacao", "numero_documento", "codigo_produto", 
                               "quantidade", "preco_venda", "tipo_documento"])

        print(f"✅ {len(df)} registros válidos após a limpeza!")

        if df.empty:
            print("⚠️ Nenhum dado válido para inserir no banco!")
            return

        # 🔍 Conectar ao PostgreSQL
        conn = psycopg2.connect(**DB_CONN)
        cursor = conn.cursor()

        # 🔍 Inserção dos dados no banco
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO vendas (tipo_registro, cnpj_agente_distribuicao, identificacao_cliente, 
                                        data_transacao, numero_documento, codigo_produto, quantidade, 
                                        preco_venda, codigo_vendedor, cep_cliente, tipo_documento, data_insercao)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, tuple(row))
            except Exception as e:
                print(f"⚠️ Erro ao inserir linha: {row.to_dict()}")
                print("🚨 Detalhes do erro:", e)

        # 🔍 Commit e fechamento da conexão
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Dados inseridos com sucesso no banco local!")

    except Exception as e:
        print("❌ Erro no processamento:", e)


# 🔍 Rodar o script diretamente (fora do Airflow)
if __name__ == "__main__":
    process_txt_to_postgres()
