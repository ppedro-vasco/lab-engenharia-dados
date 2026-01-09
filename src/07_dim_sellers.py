import sys
from pyspark.sql.functions import col, avg, lit # o que essas bibliotecas fazem?
from utils import get_spark_session, DB_URL, DB_USER, DB_PASS, DB_DRIVER, BUCKET_RAW, BUCKET_SILVER

def process_sellers(spark):
    print("--> 1. Lendo e Processando Geolocalização (Raw)...")
    path_geo = f"s3a://{BUCKET_RAW}/olist/olist_geolocation_dataset.csv"

    try:
        df_geo = spark.read.csv(path_geo, header=True, inferSchema=True)

        # O dataset de geolocalizao contem duplicatas de CEP.
        # Vou calcular a coornedanada media por CEP para evitar
        # que o join multiplique as linhas de vendedores.
        df_geo_agg = df_geo.groupBy("geolocation_zip_code_prefix") \
            .agg(
                avg("geolocation_lat").alias("lat"),
                avg("geolocation_lng").alias("lng")
            )
    except Exception as e:
        print(f"!!! Erro ao ler Geo: {e}")
        return

    # --- 2. Lendo Sellers ---
    print("--> 2. Lendo CSV de Vendedores (Raw)...")
    path_sellers = f"s3a://{BUCKET_RAW}/olist/olist_sellers_dataset.csv"

    try:
        df_sellers = spark.read.csv(path_sellers, header=True, inferSchema=True)
    except Exception as e:
        print(f"!!! Erro ao ler Sellers: {e}")
        return

    # --- 3. Fazendo o JOIN ---
    print ("--> 3. Cruzando Vendedores com Geolocalização...")

    # LEFT JOIN 
    df_joined = df_sellers.join(
        df_geo_agg,
        df_sellers.seller_zip_code_prefix == df_geo_agg.geolocation_zip_code_prefix,
        "left"
    )

    # Selecao e renome
    df_final = df_joined.select(
        col("seller_id"),
        col("seller_city"),
        col("seller_state"),
        col("lat").alias("seller_lat"),
        col("lng").alias("seller_lng"),
    ).withColumn("country", lit("Brazil"))

    # --- 4. Salvando ---
    print(f"--> 4. Salvando Parquet (Silver)...")
    path_silver = f"s3a.//{BUCKET_SILVER}/sellers"
    df_final.write.mode("overwrite").parquet(path_silver)

    print("--> 5. Persistindo no Postgres (Gold - dim_sellers)...")
    try:
        (df_final.write\
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "dim_sellers")
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", DB_DRIVER)
            .mode("overwrite")
            .save())
        print("--> Sucesso! Dimensão enriquecida criada.")
    except Exception as e:
        print(f"!!! Erro ao persistir informações no banco: {e}")

def main():
    spark = get_spark_session("OlistSellersDim")
    process_sellers(spark)
    spark.stop()

if __name__ == "__main__":
    main()