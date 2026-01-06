1. Configuração do Sistema (Windows PowerShell)
Comandos executados no PowerShell como Administrador

# 1. Instalar o Subsistema Windows para Linux
wsl --install

# 2. Instalar a distribuição Ubuntu (após reiniciar)
wsl --install -d Ubuntu

# 3. Entrar no Linux (caso não entre automático)
wsl

Nota: Foi necessário habilitar a Virtualização na BIOS para corrigir o erro HCS_E_HYPERV_NOT_INSTALLED.

2. Configuração da Infraestrutura (Docker)
Interface Gráfica do Docker Desktop

Instalação do Docker Desktop.

Settings > Resources > WSL Integration > Enable Ubuntu.

Teste de Verificação (Terminal Ubuntu):
docker run hello-world

3. Configuração do Projeto (Terminal Ubuntu / VS Code)
Local: ~ (Home do usuário Linux)

# 1. Criar estrutura de pastas
cd ~
mkdir lab-engenharia-dados
cd lab-engenharia-dados
mkdir -p data/raw src

# 2. Abrir o VS Code conectado ao WSL
code .

4. Subindo os Serviços (Docker Compose)
Arquivo docker-compose.yaml criado na raiz. Ajuste crítico: Porta do Postgres alterada para 5433 para evitar conflito com Windows.
# 1. Subir os containers (Postgres + MinIO)
docker-compose up -d

# (Comando usado para resetar volumes quando deu erro de senha)
docker-compose down -v

5. Configuração do Ambiente Python (ETL)
Terminal do VS Code (Ubuntu)

# 1. Instalar gerenciador de venv e dependências do sistema
sudo apt update && sudo apt install python3-venv -y

# 2. Criar ambiente virtual
python3 -m venv .venv

# 3. Ativar ambiente virtual
source .venv/bin/activate

# 4. Instalar bibliotecas Python
pip install boto3

6. Ingestão de Dados (Extract)
Processo realizado:

Arquivos CSV do Olist copiados do Windows para lab-engenharia-dados/data/raw.

Script src/01_ingestion.py criado.

Execução:
python src/01_ingestion.py

Resultado: Arquivos visíveis no bucket landing-zone do MinIO (localhost:9001).

7. Preparação para Spark (Última etapa iniciada)
# Instalar Java (Requisito do Spark)
sudo apt install openjdk-17-jdk -y

# Instalar PySpark no ambiente virtual
pip install pyspark