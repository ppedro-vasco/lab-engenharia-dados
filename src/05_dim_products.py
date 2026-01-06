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
    # Mantendo a configuração robusta que já validamos
    return (SparkSession.builder
            .appName("OlistProductsDim")
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

def process_products(spark):
    print("--> 1. Lendo CSV de Produtos (Raw)...")
    path_raw = f"s3a://{BUCKET_RAW}/olist/olist_products_dataset.csv"
    
    try:
        df = spark.read.csv(path_raw, header=True, inferSchema=True)
    except Exception as e:
        print(f"!!! Erro ao ler CSV: {e}")
        return
    
    # Transformação: Pegar só o que importa
    print("--> Selecionando colunas...")
    df_clean = df.selectExpr(
        "product_id",
        "product_category_name as category_name"
    )

    # Tratamento de Nulos: Produtos sem categoria viram "outros"
    df_clean = df_clean.fillna({"category_name": "outros"})

    print("--> 2. Salvando Parquet (Silver)...")
    path_silver = f"s3a://{BUCKET_SILVER}/products"
    df_clean.write.mode("overwrite").parquet(path_silver)
    
    print("--> 3. Salvando no Postgres (Gold - dim_products)...")
    try:
        (df_clean.write
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "dim_products")
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", DB_DRIVER)
            .mode("overwrite")
            .save())
        print("--> Sucesso! Tabela 'dim_products' criada.")
    except Exception as e:
        print(f"!!! Erro ao salvar no Postgres: {e}")

def main():
    spark = get_spark_session()
    process_products(spark)
    spark.stop()

if __name__ == "__main__":
    main()