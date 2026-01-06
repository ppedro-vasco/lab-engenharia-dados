import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# --- Configurações ---
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_RAW = "landing-zone"
BUCKET_SILVER = "processing-zone"

def get_spark_session():
    """
    Cria a sessão Spark.
    Correção aplicada: Lista completa de overrides para evitar erros de formato de tempo ("60s", "24h").
    """
    return (SparkSession.builder
            .appName("OlistProcessing")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540")
            
            # --- Configurações Básicas S3 MinIO ---
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            
            # --- CREDENTIAL PROVIDER (Evita erro ClassNotFound AWS V2) ---
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            
            # --- CORREÇÃO DE FORMATOS DE TEMPO (O "Whac-A-Mole") ---
            # Substituímos tudo que tem 's' (segundos) ou 'h' (horas) por números inteiros
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.socket.timeout", "5000")
            .config("spark.hadoop.fs.s3a.max.total.tasks", "20")
            
            # A CULPADA DO ERRO "24h": Multipart Purge Age (86400 segundos = 24h)
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.multipart.purge", "false") 
            
            .getOrCreate())

def process_orders(spark):
    print("--> Processando Tabela de Pedidos...")
    path = f"s3a://{BUCKET_RAW}/olist/olist_orders_dataset.csv"
    
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        print(f"--> Arquivo lido com sucesso: {path}")
    except Exception as e:
        print(f"!!! Erro ao ler arquivo: {e}")
        # import traceback
        # traceback.print_exc()
        return

    # Transformação
    if "order_purchase_timestamp" in df.columns:
        print("--> Convertendo datas...")
        df_transformed = df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))
    else:
        df_transformed = df

    print("--> Amostra dos dados:")
    df_transformed.show(5)

    target_path = f"s3a://{BUCKET_SILVER}/orders"
    print(f"--> Salvando em: {target_path}")
    
    try:
        df_transformed.write.mode("overwrite").parquet(target_path)
        print("--> Sucesso! Dados salvos na Silver Layer.")
    except Exception as e:
         print(f"!!! Erro ao salvar parquet: {e}")

def main():
    spark = get_spark_session()
    process_orders(spark)
    spark.stop()

if __name__ == "__main__":
    main()