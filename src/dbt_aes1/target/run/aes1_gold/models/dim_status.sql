
  
    
    

    create  table
      "aes1_gold"."main"."dim_status__dbt_tmp"
  
    as (
      

WITH status_unicos AS (
    SELECT DISTINCT status AS cd_status
    FROM read_parquet('/opt/airflow/datalake/silver/aes1_transacoes/*/*.parquet')
)

SELECT
    md5(cd_status) AS id_status_sk,
    cd_status,
    CASE
        WHEN cd_status = 'pending' THEN 'Pendente'
        WHEN cd_status = 'approved' THEN 'Aprovada'
        WHEN cd_status = 'denied' THEN 'Recusada'
        ELSE cd_status
    END AS ds_status,
    
    -- Estrutura SCD Tipo 2
    CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS start_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    TRUE AS is_current
FROM status_unicos
    );
  
  