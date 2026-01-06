import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Configurações ---
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_RAW = "landing-zone"
BUCKET_SILVER = "processing-zone"

# Postgres
DB_URL = "jdbc:postgresql://localhost:5433/warehouse"
DB_USER = "admin"
DB_PASS = "admin"
DB_DRIVER = "org.postgresql.Driver"

def get_spark_session():
    return (SparkSession.builder
            .appName("OlistItemsFact")
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.6.0")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.socket.timeout", "5000")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.multipart.purge", "false") 
            .getOrCreate())

def process_items(spark):
    print("--> 1. Lendo CSV de Itens (Raw)...")
    path_raw = f"s3a://{BUCKET_RAW}/olist/olist_order_items_dataset.csv"
    
    try:
        df = spark.read.csv(path_raw, header=True, inferSchema=True)
    except Exception as e:
        print(f"!!! Erro ao ler CSV: {e}")
        return
    
    # Transformação: Garantir tipos numéricos para dinheiro
    print("--> Convertendo tipos...")
    df_clean = df.select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        col("price").cast("double"),
        col("freight_value").cast("double")
    )

    print("--> 2. Salvando Parquet (Silver)...")
    path_silver = f"s3a://{BUCKET_SILVER}/items"
    df_clean.write.mode("overwrite").parquet(path_silver)
    
    print("--> 3. Salvando no Postgres (Gold - fact_items)...")
    try:
        (df_clean.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "fact_items")
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", DB_DRIVER)
            .mode("overwrite")
            .save())
        print("--> Sucesso! Tabela 'fact_items' criada.")
    except Exception as e:
        print(f"!!! Erro ao salvar no Postgres: {e}")

def main():
    spark = get_spark_session()
    process_items(spark)
    spark.stop()

if __name__ == "__main__":
    main()