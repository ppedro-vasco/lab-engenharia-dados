import boto3
import os
import logging
from botocore.exceptions import ClientError

# --- Configurações ---
# No mundo real, usaríamos variáveis de ambiente (.env) para esconder as senhas
# Como é um lab local, vamos deixar explícito por enquanto.
AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"
ENDPOINT_URL = "http://localhost:9000" # Porta da API (não a do console 9001!)
BUCKET_NAME = "landing-zone"
LOCAL_DATA_FOLDER = "./data/raw"

# Configuração de Logs (Para parecer profissional)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_bucket_if_not_exists(s3_client, bucket_name):
    """Cria o bucket se ele não existir."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' já existe.")
    except ClientError:
        logging.info(f"Criando bucket '{bucket_name}'...")
        s3_client.create_bucket(Bucket=bucket_name)

def upload_files(s3_client, bucket_name, local_folder):
    """Percorre a pasta local e sobe arquivos .csv para o MinIO."""
    if not os.path.exists(local_folder):
        logging.error(f"Pasta {local_folder} não encontrada!")
        return

    files = [f for f in os.listdir(local_folder) if f.endswith('.csv')]
    
    if not files:
        logging.warning("Nenhum arquivo CSV encontrado para upload.")
        return

    logging.info(f"Encontrados {len(files)} arquivos. Iniciando upload...")

    for file_name in files:
        local_path = os.path.join(local_folder, file_name)
        s3_path = f"olist/{file_name}" # Organizando dentro de uma "pasta" no bucket

        try:
            logging.info(f"Enviando: {file_name} -> s3://{bucket_name}/{s3_path}")
            s3_client.upload_file(local_path, bucket_name, s3_path)
        except Exception as e:
            logging.error(f"Erro ao enviar {file_name}: {e}")

    logging.info("Processo finalizado!")

def main():
    # 1. Criar o Cliente S3 (Conectando no MinIO)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

    # 2. Garantir que o Bucket existe
    create_bucket_if_not_exists(s3_client, BUCKET_NAME)

    # 3. Fazer o Upload
    upload_files(s3_client, BUCKET_NAME, LOCAL_DATA_FOLDER)

if __name__ == "__main__":
    main()