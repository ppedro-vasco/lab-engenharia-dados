import sys
from utils import get_spark_session, DB_URL, DB_USER, DB_PASS, DB_DRIVER, BUCKET_SILVER

def save_to_postgres(df, table_name):
    print(f"--> Escrevendo tabela '{table_name}' no Postgres...")
    
    try:
        (df.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", table_name) # Nome da tabela que será criada
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", DB_DRIVER)
            .mode("overwrite") # Se existir, apaga e cria de novo
            .save())
        print("--> Sucesso! Tabela criada no Data Warehouse.")
    except Exception as e:
        print(f"!!! Erro ao salvar no banco: {e}")

def main():
    spark = get_spark_session("OlistGoldLayer")
    
    # 1. Ler da Silver Layer (Parquet)
    source_path = f"s3a://{BUCKET_SILVER}/orders"
    print(f"--> Lendo Parquet de: {source_path}")
    
    try:
        df_silver = spark.read.parquet(source_path)
        print(f"--> Registros lidos: {df_silver.count()}")
    except Exception as e:
        print(f"!!! Erro ao ler Parquet (você rodou o script anterior?): {e}")
        return

    # 2. Salvar na Gold Layer (Postgres)
    save_to_postgres(df_silver, "fact_orders")
    
    spark.stop()

if __name__ == "__main__":
    main()