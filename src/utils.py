from pyspark.sql import SparkSession

# --- CONSTANTES GERAIS ---
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# --- BUCKETS ---
BUCKET_RAW = "landing-zone"
BUCKET_SILVER = "processing-zone"

# --- BANCO DE DADOS (Postgres) ---
DB_URL = "jdbc:postgresql://localhost:5433/warehouse"
DB_USER = "admin"
DB_PASS = "admin"
DB_DRIVER = "org.postgresql.Driver"

def get_spark_session(app_name):
    """
    Cria uma sessão Spark configurada para S3 (MinIO) e Postgres.
    Args:
        app_name (str): Nome da aplicação para os logs.
    """
    print(f"🔧 Iniciando Spark App: {app_name}")
    
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.6.0")
            
            # --- Configurações MinIO ---
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            
            # --- Estabilidade ---
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.socket.timeout", "5000")
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
            .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
            .config("spark.hadoop.fs.s3a.multipart.purge", "false") 
            .getOrCreate())