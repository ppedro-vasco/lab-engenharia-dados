# 🇧🇷 Olist End-to-End Data Pipeline

![Status](https://img.shields.io/badge/Status-Em_Desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Spark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C)

Um projeto prático de Engenharia de Dados simulando um ambiente de **Lakehouse** local.
O objetivo é ingerir, processar e analisar dados reais do E-commerce brasileiro (Dataset público da Olist), transformando dados brutos em insights de logística e vendas.

---

## 🏗️ Arquitetura da Solução

O projeto segue a arquitetura **Medalhão (Multi-hop)** dentro de um ambiente containerizado:

```mermaid
graph LR
    A[Arquivos CSV] -->|Ingestion Script| B[(MinIO - Raw)]
    B -->|Spark - Cleaning| C[(MinIO - Silver)]
    C -->|Spark - Business Rules| D[(Postgres - Gold)]
    D -->|SQL/Analytics| E[Data Warehouse]
