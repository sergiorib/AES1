import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

# Descobrir a raiz do projeto (caminho absoluto)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('/opt/airflow/datalake'):
    ROOT_DIR = '/opt/airflow'
else:
    ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

def main(data_alvo):
    print(f"🚀 Iniciando ingestão RAW -> BRONZE para a data: {data_alvo}")
    print("-" * 60)
    
    # Inicializar a sessão do Spark
    builder = SparkSession.builder \
    .appName("DeltaPay_Ingestao_Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Caminhos atualizados com o seu novo padrão de prefixos
    caminho_raw = os.path.join(ROOT_DIR, f"datalake/raw/aes1_raw_{data_alvo}", "*.jsonl")
    caminho_bronze = os.path.join(ROOT_DIR, f"datalake/bronze/aes1_bronze_{data_alvo}")

    try:
        # 1. Leitura dos dados RAW (JSONL)
        print(f"📥 Lendo arquivos JSONL da Raw: aes1_raw_{data_alvo}")
        df_raw = spark.read.json(caminho_raw)
        
        # 2. Achatar (Flatten) as estruturas aninhadas
        print("🔨 Achatando as colunas aninhadas (Flattening)...")
        df_flat = df_raw.select(
            col("event_id"),
            col("transaction_id"),
            col("timestamp_brt"),
            col("transaction_amount"),
            col("fee_charged"),
            col("status"),
            col("transaction_type"),
            col("transaction_category"),
            col("customer.customer_id").alias("customer_id"),
            col("customer.full_name").alias("customer_full_name"),
            col("customer.document_cpf").alias("customer_document_cpf"),
            col("customer.signup_date").alias("customer_signup_date"),
            col("device_info.os").alias("device_os"),
            col("device_info.ip_address").alias("device_ip_address")
        )
        
        # 3. Escrita na camada Bronze (Parquet)
        print(f"💾 Salvando em formato Parquet na pasta Bronze: aes1_bronze_{data_alvo}")
        df_flat.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(caminho_bronze)
            
        print("-" * 60)
        print("✅ Ingestão finalizada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante a ingestão: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso correto: poetry run python src/scripts/aes1_ingestao_bronze.py <YYYY-MM-DD>")
        sys.exit(1)
        
    arg_data = sys.argv[1]
    main(arg_data)