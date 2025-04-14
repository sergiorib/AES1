import os
import shutil

# Descobrir a raiz do projeto (caminho absoluto idêntico ao setup)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
print("*************")
print(ROOT_DIR)
print("*************")
def executar_limpeza():
    print("\n==========================================")
    print("🧹 INICIANDO LIMPEZA DO DATA LAKE")
    print("==========================================")

    camadas = ['datalake/raw', 'datalake/bronze', 'datalake/silver', 'datalake/gold']
    
    for camada in camadas:
        caminho_camada = os.path.join(ROOT_DIR, camada)
        print(f"🗑️  Limpando: {camada}...")
        
        if os.path.exists(caminho_camada):
            # Varre todos os arquivos e pastas dentro da camada
            for item in os.listdir(caminho_camada):
                if item == '.gitkeep':
                    continue # Segurança: Preserva o arquivo do Git
                
                caminho_item = os.path.join(caminho_camada, item)
                try:
                    if os.path.isfile(caminho_item) or os.path.islink(caminho_item):
                        os.unlink(caminho_item) # Deleta arquivo
                    elif os.path.isdir(caminho_item):
                        shutil.rmtree(caminho_item) # Deleta pasta inteira
                except Exception as e:
                    print(f"❌ Erro ao deletar {caminho_item}: {e}")
        else:
            print(f"⚠️ Pasta não encontrada: {caminho_camada}")
            
    print("==========================================")
    print("✅ DATA LAKE ZERADO COM SUCESSO!")
    print("==========================================\n")

if __name__ == "__main__":
    # Permite rodar o script isoladamente pelo terminal
    executar_limpeza()