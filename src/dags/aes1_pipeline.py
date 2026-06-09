from numpy import False_

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'engenharia',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'aes1_pipeline_DeltaPay_transactions',
    default_args=default_args,
    description='Pipeline Completo: Raw > Bronze > Silver > Gold (dbt)',
    schedule_interval='@daily',
    catchup=False,
    tags=['fraude', 'aes1', 'deltapay'],
    params={
        "motivo_execucao": "Reprocessamento pontual / Correção de dados",
        "instrucao_importante": "ATENÇÃO: Use o calendário 'Logical date' abaixo para selecionar o dia exato que deseja processar."
    }
) as dag:

    # 1. Camada Raw (Geração dos dados JSONL simulados)
    tarefa_gerar_raw = BashOperator(
        task_id='gerar_dados_raw',
        # Passamos {{ ds }} apenas UMA vez, pois o script agora é especializado em 1 dia
        bash_command='python /opt/airflow/scripts/aes1_gerador_raw.py {{ ds }}'
    )

    # 2. Camada Bronze (Ingestão do JSONL para formato Delta Lake)
    tarefa_spark_bronze = BashOperator(
        task_id='ingestao_bronze_spark',
        bash_command='python /opt/airflow/scripts/aes1_ingestao_bronze.py {{ ds }}'
    )

    # 3. Camada Silver (Limpeza, máscaras LGPD e regras de fraude com Window Functions)
    tarefa_spark_silver = BashOperator(
        task_id='processamento_silver_spark',
        bash_command='python /opt/airflow/scripts/aes1_processamento_silver.py {{ ds }}'
    )

    # 4. Camada Gold (Modelagem Dimensional em Star Schema usando dbt e DuckDB)
    tarefa_dbt_gold = BashOperator(
        task_id='transformar_gold_dbt',
        bash_command='cd /opt/airflow/dbt_aes1 && dbt run'
        # Nota: Se você criou subpastas no dbt (ex: aes1), use '... && dbt run --select aes1'
    )

    # Ordem de execução blindada do pipeline de ponta a ponta
    tarefa_gerar_raw >> tarefa_spark_bronze >> tarefa_spark_silver >> tarefa_dbt_gold