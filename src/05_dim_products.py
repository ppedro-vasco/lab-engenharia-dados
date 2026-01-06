import sys
from utils import get_spark_session, BUCKET_RAW, BUCKET_SILVER, DB_URL, DB_USER, DB_PASS, DB_DRIVER
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
    spark = get_spark_session("OlistProductsDim")
    process_products(spark)
    spark.stop()

if __name__ == "__main__":
    main()