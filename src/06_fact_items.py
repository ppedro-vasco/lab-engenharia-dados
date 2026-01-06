import sys
from pyspark.sql.functions import col
from utils import get_spark_session, BUCKET_RAW, BUCKET_SILVER, DB_URL, DB_USER, DB_PASS, DB_DRIVER

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
    spark = get_spark_session("OlistItemsFact")
    process_items(spark)
    spark.stop()

if __name__ == "__main__":
    main()