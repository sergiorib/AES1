import os
import subprocess
import platform # Nova biblioteca para detectar o Sistema Operacional

# Descobrir a raiz do projeto (caminho absoluto)
UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(UTILS_DIR, '..', '..'))
SCRIPT_DIR = os.path.abspath(os.path.join(UTILS_DIR, '..', '..','src','scripts'))

print("*************")
print(UTILS_DIR)
print("*************")
def limpeza_dados():
    """Pergunta ao usuário se ele deseja limpar os dados antes de iniciar o ambiente."""
    resposta = input("⚠️ Deseja apagar todos os dados gerados anteriormente no Data Lake? (s/N): ")
    if resposta.lower() == 's':
        script_limpeza = os.path.join(SCRIPT_DIR, 'aes1_limpar_datalake.py')
        if os.path.exists(script_limpeza):
            try:
                # Chama o script de limpeza em Python de forma cross-platform
                subprocess.run(["python", script_limpeza], check=True)
            except subprocess.CalledProcessError:
                print("❌ Erro ao executar a limpeza do Data Lake.")
                exit(1)
        else:
            print(f"⚠️ Script de limpeza não encontrado em {script_limpeza}. Ignorando.")
    else:
        print("⏩ Limpeza ignorada. Mantendo os dados atuais no Data Lake.\n")

def criar_pastas():
    print("📁 A preparar a estrutura de diretórios do projeto...")
    pastas = [
        'datalake/raw',        
        'datalake/bronze',
        'datalake/silver',
        'datalake/gold',
        'src/dags',
        'src/scripts',
        'src/dbt_aes1'
    ]
    for p in pastas:
        caminho_completo = os.path.join(ROOT_DIR, p)
        os.makedirs(caminho_completo, exist_ok=True)
        # Cria um ficheiro oculto para garantir que o git guarda as pastas
        with open(os.path.join(caminho_completo, '.gitkeep'), 'a') as f:
            pass
    print("✅ Pastas verificadas/criadas com sucesso!")

def permissoes_pasta_datalake():
    # ---------------------------------------------------------
    # NOVO BLOCO: Ajuste automático de permissões (Linux/Mac)
    # ---------------------------------------------------------
    sistema_operacional = platform.system()
    if sistema_operacional in ['Linux', 'Darwin']:
        print(f"🐧 Sistema {sistema_operacional} detectado. A ajustar permissões do Data Lake...")
        caminho_datalake = os.path.join(ROOT_DIR, 'datalake')
        try:
            # Roda o comando chmod -R 777 na pasta datalake
            subprocess.run(["chmod", "-R", "777", caminho_datalake], check=True)
            print("✅ Permissões globais (777) aplicadas com sucesso ao Data Lake!")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Aviso: Não foi possível ajustar as permissões automaticamente. Erro: {e}")
            print("Talvez seja necessário rodar 'sudo chmod -R 777 datalake' manualmente.")

def gerar_base_clientes():
    print("👥 A gerar a base mestre de clientes (Seed)...")
    script_clientes = os.path.join(ROOT_DIR, 'src', 'scripts', 'aes1_gerar_base_clientes.py')
    
    if os.path.exists(script_clientes):
        try:
            # Roda o script de clientes usando o python do ambiente atual
            subprocess.run(["python", script_clientes], check=True)
            print("✅ Base mestre de clientes garantida!")
        except subprocess.CalledProcessError:
            print("❌ Erro ao gerar a base de clientes.")
            exit(1)
    else:
        print(f"⚠️ Aviso: Script não encontrado em {script_clientes}. Ignorando esta etapa.")

def criar_env():
    caminho_env = os.path.join(ROOT_DIR, 'docker', '.env')
    if not os.path.exists(caminho_env):
        with open(caminho_env, 'w') as f:
            f.write("# Variáveis de Ambiente do Docker Compose\n")
        print("✅ Ficheiro .env garantido na pasta docker!")

def iniciar_docker():
    print("🐳 A verificar o estado da infraestrutura Docker...")
    pasta_docker = os.path.join(ROOT_DIR, 'docker')
    
    try:
        # Executa docker compose ps para verificar se há containers deste projeto
        resultado = subprocess.run(
            ["docker", "compose", "ps", "-q"],
            cwd=pasta_docker,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Se resultado.stdout tiver algum texto, significa que os containers existem
        if resultado.stdout.strip():
            print("\n⚠️  Atenção: A infraestrutura do Docker já está criada ou em execução.")
            resposta = input("🔄 Deseja dar um 'reset' (parar e reiniciar do zero)? (s/N): ")
            
            if resposta.lower() == 's':
                print("🛑 A parar e a remover os serviços antigos...")
                subprocess.run(["docker", "compose", "down"], cwd=pasta_docker, check=True)
                print("🚀 A iniciar a infraestrutura novamente...")
                subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=pasta_docker, check=True)
                print("✅ Docker reiniciado com sucesso!")
            else:
                print("⏩ Reset ignorado. Mantendo os containers como estão.")
        else:
            # Se não retornar nada, é a primeira vez rodando (ou tudo já estava em 'down')
            print("🚀 A iniciar a infraestrutura no Docker (pode demorar alguns minutos)...")
            subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=pasta_docker, check=True)
            print("✅ Comandos enviados ao Docker com sucesso!")
            
    except subprocess.CalledProcessError:
        print("❌ Erro ao comunicar com o Docker. Verifique se o Docker Desktop está aberto e a correr.")
        exit(1)

def main():
    print("🚀 A iniciar o ambiente de dados AES1...")
    print("-" * 40)
    criar_pastas()
    permissoes_pasta_datalake()
    gerar_base_clientes() 
    limpeza_dados()   
    criar_env()
    iniciar_docker()
    print("-" * 40)
    print("🎉 Setup concluído!")
    print("⏳ O Airflow está a inicializar a base de dados em segundo plano.")
    print("🌐 Daqui a 1 ou 2 minutos, aceda a: http://localhost:8082")
    print("🔑 Utilizador: admin | Palavra-passe: admin")

if __name__ == "__main__":
    main()