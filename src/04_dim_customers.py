import sys
from pyspark.sql import SparkSession

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
            .appName("OlistCustomersDim")
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

def process_customers(spark):
    print("--> 1. Lendo CSV de Clientes (Raw)...")
    path_raw = f"s3a://{BUCKET_RAW}/olist/olist_customers_dataset.csv"
    
    df = spark.read.csv(path_raw, header=True, inferSchema=True)
    
    # Pequena transformação: Selecionar e Renomear colunas para ficar bonito
    # customer_zip_code_prefix -> zip_code
    df_clean = df.selectExpr(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix as zip_code",
        "customer_city as city",
        "customer_state as state"
    )

    print("--> 2. Salvando Parquet (Silver)...")
    path_silver = f"s3a://{BUCKET_SILVER}/customers"
    df_clean.write.mode("overwrite").parquet(path_silver)
    
    print("--> 3. Salvando no Postgres (Gold - dim_customers)...")
    (df_clean.write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "dim_customers") # Nome da tabela dimensão
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", DB_DRIVER)
        .mode("overwrite")
        .save())
    
    print("--> Sucesso! Dimensão criada.")

def main():
    spark = get_spark_session()
    process_customers(spark)
    spark.stop()

if __name__ == "__main__":
    main()