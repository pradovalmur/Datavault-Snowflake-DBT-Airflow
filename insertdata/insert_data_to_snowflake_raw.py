import os
import time
import pandas as pd
import typer
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Conectar ao Snowflake
try:
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
except Exception as e:
    print (e)


# Definir o aplicativo Typer
app = typer.Typer()

@app.command()
def prepare_snowflake():

    user = os.getenv('SNOWFLAKE_USER')
    wh_name = 'dbt_wh'
    wh_size = 'small'
    database_name = 'dbt_db'
    role_name = 'dbt_role'
    schemas = ['bronze', 'silver', 'gold']

    with conn.cursor() as cur:
        
        print("usando account admin")
        cur.execute("use role accountadmin")
        time.sleep(5)
        print("criando warehouse")
        cur.execute(f"create warehouse if not exists {wh_name} with warehouse_size='{wh_size}'")
        time.sleep(5)
        print("criando database")
        cur.execute(f"create database if not exists {database_name}")
        time.sleep(5)
        print("criando role")
        cur.execute(f"create role if not exists {role_name}")
        time.sleep(5)
        print("concedendo acesso ao role")
        cur.execute(f"grant role {role_name} to user {user}")
        time.sleep(5)
        #("usando role")
        #cur.execute(f"use role {role_name}")
        time.sleep(5)
        print("entrando no for do schema")
        
        for schema in schemas:
            print(f"criando schema {schema}")
            cur.execute(f"create schema if not exists {database_name}.{schema}")
            print(f"dando permissoes ao schema {schema}")
            cur.execute(f"grant usage on schema {schema} to {role_name}")

            time.sleep(5)

@app.command()
def insert_investidor():
    
    # Função para mapear tipos de dados do Pandas para Snowflake
    def map_pandas_to_snowflake(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'NUMBER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
        
    # Ler o CSV e filtrar os dados com base na coluna especificada
    df = pd.read_csv("./data/Investidores_Tesouro_Direto.csv", sep= ';',encoding='latin1')

    df['Data de Adesao'] = pd.to_datetime(df['Data de Adesao'], dayfirst=True).dt.strftime('%Y-%m-%d')

    # Aplicar filtro para registros criados após 2015
    df_filtered = df[df['Data de Adesao'] > "2015-01-01"]

    df_filtered.columns = df_filtered.columns.str.replace(' ', '_').str.upper()

    # Gerar a query de criação de tabela dinamicamente
    columns = []
    for column_name, dtype in df_filtered.dtypes.items():
        snowflake_type = map_pandas_to_snowflake(dtype)
        columns.append(f"{column_name} {snowflake_type}")
    columns_sql = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS investidors ({columns_sql})"

    with conn.cursor() as cur:
        cur.execute("use role accountadmin")
        cur.execute(create_table_query)

    # Usar write_pandas para inserir os dados em "bulk"
    success, nchunks, nrows, _ = write_pandas(conn, df_filtered, 'INVESTIDORS')

    if success:
        print(f"Bulk insert realizado com sucesso. {nrows} linhas inseridas.")
    else:
        print("Falha ao realizar o bulk insert.")

    # Fechar a conexão
    conn.close()

    typer.echo("Dados de investidorenviados com sucesso para o snowflake!")

@app.command()
def insert_transactions():
    
    # Função para mapear tipos de dados do Pandas para Snowflake
    def map_pandas_to_snowflake(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'NUMBER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    # Ler o CSV e filtrar os dados com base na coluna especificada
    df = pd.read_csv("./data/Operacoes_Tesouro_Direto.csv", sep= ';',encoding='latin1')

    df['Data da Operacao'] = pd.to_datetime(df['Data da Operacao'], dayfirst=True).dt.strftime('%Y-%m-%d')
    df['Vencimento do Titulo'] = pd.to_datetime(df['Vencimento do Titulo'], dayfirst=True).dt.strftime('%Y-%m-%d')

    # Aplicar filtro para registros criados após 2015
    df_filtered = df[df['Data da Operacao'] > "2020-01-01"]

    df_filtered.columns = df_filtered.columns.str.replace(' ', '_').str.upper()

    # Gerar a query de criação de tabela dinamicamente
    columns = []
    for column_name, dtype in df_filtered.dtypes.items():
        snowflake_type = map_pandas_to_snowflake(dtype)
        columns.append(f"{column_name} {snowflake_type}")
    columns_sql = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS transactions ({columns_sql})"


    with conn.cursor() as cur:
        cur.execute(create_table_query)

    # Usar write_pandas para inserir os dados em "bulk"
    success, nchunks, nrows, _ = write_pandas(conn, df_filtered, 'TRANSACTIONS')

    if success:
        print(f"Bulk insert realizado com sucesso. {nrows} linhas inseridas.")
    else:
        print("Falha ao realizar o bulk insert.")

    # Fechar a conexão
    conn.close()
    
    typer.echo("Dados de transactions enviados com sucesso para o snowflake!")   

@app.command()
def insert_stock():
    
    # Função para mapear tipos de dados do Pandas para Snowflake
    def map_pandas_to_snowflake(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'NUMBER'
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'TIMESTAMP'
        else:
            return 'TEXT'
    
    # Ler o CSV e filtrar os dados com base na coluna especificada
    df = pd.read_csv("./data/EstoqueTesouroDireto.csv", sep= ';',encoding='latin1')

    df['Vencimento do Titulo'] = pd.to_datetime(df['Vencimento do Titulo'], dayfirst=True).dt.strftime('%Y-%m-%d')

    df.columns = df.columns.str.replace(' ', '_').str.upper()

    # Gerar a query de criação de tabela dinamicamente
    columns = []
    for column_name, dtype in df.dtypes.items():
        snowflake_type = map_pandas_to_snowflake(dtype)
        columns.append(f"{column_name} {snowflake_type}")
    columns_sql = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS stock ({columns_sql})"


    with conn.cursor() as cur:
        cur.execute("use role accountadmin")
        cur.execute(create_table_query)

    # Usar write_pandas para inserir os dados em "bulk"
    success, nchunks, nrows, _ = write_pandas(conn, df, 'STOCK')

    if success:
        print(f"Bulk insert realizado com sucesso. {nrows} linhas inseridas.")
    else:
        print("Falha ao realizar o bulk insert.")

    # Fechar a conexão
    conn.close()
    
    typer.echo("Dados de stock enviados com sucesso para o snowflake!") 

# Executar o aplicativo Typer
if __name__ == "__main__":
    app()