import csv
import random
from datetime import datetime, timedelta
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CAMINHO_CSV = os.path.join(SCRIPT_DIR, 'aes1_base_clientes.csv')

nomes = ["João", "Maria", "Carlos", "Ana", "Pedro", "Julia", "Lucas", "Mariana", "Marcos", "Fernanda", "Rafael", "Beatriz", "Thiago", "Camila"]
sobrenomes = ["Silva", "Oliveira", "Santos", "Costa", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira", "Lima", "Gomes", "Ribeiro"]

print("⏳ Gerando base de 10.000 clientes...")

with open(CAMINHO_CSV, mode='w', newline='', encoding='utf-8') as arquivo:
    writer = csv.writer(arquivo)
    # Cabeçalho do CSV
    writer.writerow(["customer_id", "full_name", "document_cpf", "signup_date"])
    
    for i in range(10000):
        # Gera os dados combinados
        nome_completo = f"{random.choice(nomes)} {random.choice(sobrenomes)}"
        cpf = f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
        customer_id = f"CUST-{10000 + i}"
        
        # Define a data de cadastro (Safra) entre 2023 e o início de 2026
        data_base = datetime(2026, 1, 1)
        signup_date = (data_base - timedelta(days=random.randint(0, 1000))).strftime("%d/%m/%Y")
        
        # Escreve a linha
        writer.writerow([customer_id, nome_completo, cpf, signup_date])

print(f"✅ CSV gerado com sucesso em: {CAMINHO_CSV}")