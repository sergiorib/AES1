import os
import json
import uuid
import random
import sys
import csv
from datetime import datetime

# Descobrir a raiz do projeto e adaptar ao Docker
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('/opt/airflow/datalake'):
    ROOT_DIR = '/opt/airflow'
else:
    ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

# --- CARREGAMENTO DA BASE MESTRE DE CLIENTES ---
CAMINHO_CSV = os.path.join(SCRIPT_DIR, 'aes1_base_clientes.csv')

CLIENTES_BASE = []

def carregar_clientes():
    """Carrega o CSV de clientes para a memória para sorteio rápido."""
    if not os.path.exists(CAMINHO_CSV):
        print(f"❌ Erro fatal: Arquivo de clientes não encontrado em {CAMINHO_CSV}")
        print("Execute 'python aes1_setup.py' ou 'python aes1_gerar_base_clientes.py' primeiro.")
        sys.exit(1)
        
    with open(CAMINHO_CSV, mode='r', encoding='utf-8') as f:
        leitor = csv.DictReader(f)
        for linha in leitor:
            CLIENTES_BASE.append(linha)

# Executa o carregamento assim que o script é chamado
carregar_clientes()
# -----------------------------------------------

def gerar_transacao(data_execucao, is_fraud=False):
    """Gera um único registro de transação aninhado baseado na base mestra."""
    hora_aleatoria = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
    timestamp_brt = f"{data_execucao} {hora_aleatoria}"
    
    fee = round(random.uniform(0.5, 5.0), 2) if random.random() > 0.3 else None
    
    # Sorteia um cliente real da nossa base em memória
    cliente_sorteado = random.choice(CLIENTES_BASE)
    
    # --- LÓGICA DE FRAUDE INJETADA ---
    if is_fraud:
        # Valores absurdamente altos para simular fraude e disparar a regra da Silver
        transaction_amount = round(random.uniform(15000.0, 80000.0), 2)
    else:
        # Valores normais do dia a dia
        transaction_amount = round(random.uniform(10.0, 5000.0), 2)
        
    registro = {
        "event_id": str(uuid.uuid4()),
        "transaction_id": f"TXN-{random.randint(100000, 999999)}",
        "timestamp_brt": timestamp_brt,
        "transaction_amount": str(transaction_amount),
        "fee_charged": fee,
        "status": "pending",
        "transaction_type": random.choice(["pix", "boleto", "ted", "credit_card"]),
        "transaction_category": random.choice(["entertainment", "food", "transport", "utilities"]),
        "customer": {
            "customer_id": cliente_sorteado["customer_id"],
            "full_name": cliente_sorteado["full_name"],
            "document_cpf": cliente_sorteado["document_cpf"],
            "signup_date": cliente_sorteado["signup_date"] # Safra garantida pelo CSV!
        },
        "device_info": {
            "os": random.choice(["iOS", "Android", "Web"]),
            "ip_address": f"192.168.1.{random.randint(1, 255)}"
        }
    }
    return registro

def processar_dia(data_alvo, qtd_registros=5000):
    """Gera e salva o lote de dados para um dia específico (Idempotente)."""
    # Sorteia uma taxa de fraude diária aleatória entre 0% e 5%
    taxa_fraude = random.uniform(0.0, 0.05)
    qtd_fraudes = int(qtd_registros * taxa_fraude)
    
    print(f"⏳ Processando data: {data_alvo} ({qtd_registros} registros | {qtd_fraudes} fraudes injetadas)...", end="")
    
    pasta_destino = os.path.join(ROOT_DIR, f"datalake/raw/aes1_raw_{data_alvo}")
    os.makedirs(pasta_destino, exist_ok=True)
    
    # Gera as transações
    lote = []
    for i in range(qtd_registros):
        # As primeiras 'qtd_fraudes' iterações geram fraudes, o restante gera normais
        is_fraud = True if i < qtd_fraudes else False
        lote.append(gerar_transacao(data_alvo, is_fraud=is_fraud))
        
    # Embaralha a lista para as fraudes não ficarem todas agrupadas na mesma hora
    random.shuffle(lote)
    
    caminho_arquivo = os.path.join(pasta_destino, "aes1_raw_transacoes.jsonl")
    
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        for registro in lote:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
        
    print(f" ✅ Salvo em {caminho_arquivo}")

if __name__ == "__main__":
    # O script espera estritamente UMA data (se não receber nada, usa hoje)
    data_alvo = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    print(f"🚀 Iniciando geração de dados RAW para o dia: {data_alvo} com {len(CLIENTES_BASE)} clientes cadastrados.")
    print("-" * 60)
    
    processar_dia(data_alvo)
    
    print("-" * 60)
    print("🎉 Geração diária concluída!")