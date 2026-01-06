import sys
from utils import get_spark_session, DB_URL, DB_USER, DB_PASS, DB_DRIVER, BUCKET_RAW, BUCKET_SILVER


def process_customers(spark):
    print("--> 1. Lendo CSV de Clientes (Raw)...")
    path_raw = f"s3a://{BUCKET_RAW}/olist/olist_customers_dataset.csv"
    
    df = spark.read.csv(path_raw, header=True, inferSchema=True)
    
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
    spark = get_spark_session("OlistCustomersDim")
    process_customers(spark)
    spark.stop()

if __name__ == "__main__":
    main()