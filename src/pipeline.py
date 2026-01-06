import subprocess
import time
import sys

# Lista de etapas do nosso pipeline na ordem de dependência
STEPS = [
    {"name": "Ingestion (Raw)", "script": "src/01_ingestion.py"},
    {"name": "Orders (Silver)", "script": "src/02_transform_silver.py"},
    {"name": "Orders (Gold - Fato)", "script": "src/03_gold_layer.py"},
    {"name": "Customers (Dimensão)", "script": "src/04_dim_customers.py"},
    {"name": "Products (Dimensão)", "script": "src/05_dim_products.py"},
    {"name": "Items (Gold - Fato)", "script": "src/06_fact_items.py"},
    {"name": "Sellers (Dimensão)", "script": "src/07_dim_sellers.py"} # <--- Verifique esta linha
]

def run_step(step_name, script_path):
    print(f"\n{'='*60}")
    print(f"🚀 INICIANDO: {step_name}")
    print(f"{'='*60}")
    start_time = time.time()
    
    try:
        # Executa o script python como se fosse no terminal
        # check=True faz o script parar se der erro no passo anterior
        subprocess.run([sys.executable, script_path], check=True)
        
        elapsed = time.time() - start_time
        print(f" SUCESSO: {step_name} finalizado em {elapsed:.2f} segundos.")
        return True
    except subprocess.CalledProcessError:
        print(f" ERRO CRÍTICO: Falha em {step_name}.")
        return False

def main():
    print("🔧 Inicializando Pipeline de Engenharia de Dados - Olist Lab...")
    total_start = time.time()
    
    for step in STEPS:
        success = run_step(step["name"], step["script"])
        if not success:
            print("\n Pipeline interrompido devido a erro.")
            sys.exit(1) # Sai com código de erro
            
    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"🏁 PIPELINE CONCLUÍDO COM SUCESSO! Tempo Total: {total_elapsed:.2f}s")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()