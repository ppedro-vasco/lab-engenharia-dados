import sys
from utils import get_spark_session, DB_URL, DB_USER, DB_PASS, DB_DRIVER, BUCKET_RAW, BUCKET_SILVER

def process_sellers(spark):
    print("--> 1. Lendo CSV de Vendedores (Raw)...")
    path_raw = f"s3a://{BUCKET_RAW}/olist/olist_sellers_dataset.csv"
    
    df = spark.read.csv(path_raw, header=True, inferSchema=True)

    df_clean = df.selectExpr(
        "seller_id",
        "seller_city",
        "seller_state"
    )

    print("--> 2. Salvando Parquet (Silver)...")
    path_silver = f"s3a://{BUCKET_SILVER}/sellers"
    df_clean.write.mode("overwrite").parquet(path_silver)
    
    print("--> 3. Salvando no Postgres (Gold - dim_sellers)...")
    (df_clean.write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", "dim_sellers") # Nome da tabela dimensão
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", DB_DRIVER)
        .mode("overwrite")
        .save())
    
    print("--> Sucesso! Dimensão criada.")

def main():
    spark = get_spark_session("OlistSellersDim")
    process_sellers(spark)
    spark.stop()

if __name__ == "__main__":
    main()