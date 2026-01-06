import sys
from pyspark.sql import SparkSession

# --- Configurações do MinIO (Origem) ---
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_SILVER = "processing-zone"

# --- Configurações do Postgres (Destino) ---
# Atenção: Usando localhost e a porta 5433 que definimos no Docker
DB_URL = "jdbc:postgresql://localhost:5433/warehouse"
DB_USER = "admin"
DB_PASS = "admin"
DB_DRIVER = "org.postgresql.Driver"

def get_spark_session():
    """
    Sessão Spark com Super-Poderes:
    - Lê S3/MinIO (Drivers Hadoop-AWS)
    - Escreve JDBC (Driver Postgres)
    """
    return (SparkSession.builder
            .appName("OlistGoldLayer")
            .master("local[*]")
            # Carregando Drivers: AWS S3 + PostgreSQL JDBC
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.6.0")
            
            # --- Configs MinIO (Igual ao script anterior) ---
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            
            # --- Configs de Estabilidade (Whac-A-Mole) ---
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.socket.timeout", "5000")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.multipart.purge", "false") 
            .getOrCreate())

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
    spark = get_spark_session()
    
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
    # Vamos salvar na tabela 'fact_orders' (Fato Pedidos)
    save_to_postgres(df_silver, "fact_orders")
    
    spark.stop()

if __name__ == "__main__":
    main()