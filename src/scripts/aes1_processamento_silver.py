import sys
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Descobrir a raiz do projeto
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('/opt/airflow/datalake'):
    ROOT_DIR = '/opt/airflow'
else:
    ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

def main(data_alvo):
    print(f"🚀 Iniciando processamento BRONZE -> SILVER para a data: {data_alvo}")
    print("-" * 60)
    
    builder = SparkSession.builder \
    .appName("DeltaPay_Processamento_Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Injeta os pacotes .jar do Delta automaticamente e inicia a sessão
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Caminhos
    caminho_bronze = os.path.join(ROOT_DIR, f"datalake/bronze/aes1_bronze_{data_alvo}")
    caminho_silver = os.path.join(ROOT_DIR, "datalake/silver/aes1_transacoes")

    try:
        # 1. Leitura da Camada Bronze (Delta Lake)
        print("📥 Lendo dados da Bronze...")
        df_bronze = spark.read.format("delta").load(caminho_bronze)
        
        # 2. Limpeza e Tipagem (Qualidade de Dados)
        print("🧹 Limpando dados e aplicando máscaras (LGPD)...")
        df_clean = df_bronze \
            .withColumn("data_hora", F.to_timestamp("timestamp_brt")) \
            .withColumn("valor_transacao", F.col("transaction_amount").cast("decimal(15,2)")) \
            .withColumn("taxa_receita", F.coalesce(F.col("fee_charged").cast("decimal(15,2)"), F.lit(0.0))) \
            .withColumn("nome_cliente", F.trim("customer_full_name")) \
            .withColumn("cpf_limpo", F.regexp_replace("customer_document_cpf", r"[^0-9]", "")) \
            .withColumn("cpf_mascarado", F.expr("concat('***.', substr(cpf_limpo, 4, 3), '.', substr(cpf_limpo, 7, 3), '-**')"))

        # 3. Regras Avançadas de Negócio (Window Functions)
        print("🕵️ Calculando histórico de clientes e janelas de risco (Window Functions)...")
        
        # Janela 1: Histórico do cliente ANTERIOR à transação atual (-1)
        w_historico = Window.partitionBy("customer_id").orderBy("data_hora").rowsBetween(Window.unboundedPreceding, -1)
        df_regras = df_clean.withColumn("valor_medio_historico", F.avg("valor_transacao").over(w_historico))

        # Janela 2: Janela de tempo de 10 minutos (600 segundos) para o mesmo cliente
        w_10min = Window.partitionBy("customer_id").orderBy(F.col("data_hora").cast("long")).rangeBetween(-600, 0)
        
        df_regras = df_regras.withColumn("is_pix", F.when(F.col("transaction_type") == "pix", 1).otherwise(0))
        df_regras = df_regras.withColumn("qtd_pix_10min", F.sum("is_pix").over(w_10min))

        # 4. Aplicação da Flag de Fraude (Motor de Risco Calibrado)
        print("🚨 Aplicando flag de comportamento suspeito...")
        df_final = df_regras.withColumn(
            "flag_suspeita",
            F.when(
                (F.col("qtd_pix_10min") > 3) | 
                (F.col("valor_transacao") > (F.coalesce(F.col("valor_medio_historico"), F.col("valor_transacao")) * 3)) |
                (F.col("valor_transacao") > 10000), # Teto absoluto de R$ 10.000
                True
            ).otherwise(False)
        )

        # Selecionar apenas as colunas que vão para a tabela Silver (Dropando colunas auxiliares)
        df_silver = df_final.select(
            "event_id", "transaction_id", "data_hora", "customer_id", "nome_cliente", 
            "cpf_mascarado", "customer_signup_date", "transaction_type", "transaction_category", "status", 
            "valor_transacao", "taxa_receita", "device_os", "device_ip_address", 
            "valor_medio_historico", "qtd_pix_10min", "flag_suspeita",
            F.lit(data_alvo).alias("data_processamento")
        )

        # 5. Salvar na Camada Silver (Modo Append particionado)
        print("💾 Salvando tabela unificada em Delta na Silver...")
        df_silver.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("data_processamento") \
            .option("mergeSchema", "true") \
            .save(caminho_silver)
            
        print("-" * 60)
        print("✅ Processamento Silver finalizado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante o processamento: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso correto: poetry run python src/scripts/aes1_processamento_silver.py <YYYY-MM-DD>")
        sys.exit(1)
        
    arg_data = sys.argv[1]
    main(arg_data)