import sys
from pyspark.sql.functions import col, to_date

from utils import get_spark_session, BUCKET_RAW, BUCKET_SILVER

def process_orders(spark):
    print("--> Processando Tabela de Pedidos...")
    path = f"s3a://{BUCKET_RAW}/olist/olist_orders_dataset.csv"
    
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        print(f"--> Arquivo lido com sucesso: {path}")
    except Exception as e:
        print(f"!!! Erro ao ler arquivo: {e}")
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
    spark = get_spark_session("OlistProcessing")
    process_orders(spark)
    spark.stop()

if __name__ == "__main__":
    main()