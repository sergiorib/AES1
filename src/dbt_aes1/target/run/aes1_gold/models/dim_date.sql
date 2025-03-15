
  
    
    

    create  table
      "aes1_gold"."main"."dim_date__dbt_tmp"
  
    as (
      

WITH datas_extraidas AS (
    SELECT DISTINCT 
        CAST(data_hora AS DATE) AS data_completa
    FROM read_parquet('/opt/airflow/datalake/silver/aes1_transacoes/*/*.parquet')
)

SELECT
    CAST(strftime(data_completa, '%Y%m%d') AS INTEGER) AS id_data,
    data_completa AS data,
    EXTRACT(DAY FROM data_completa) AS dia,
    EXTRACT(MONTH FROM data_completa) AS mes,
    strftime(data_completa, '%B') AS mes_nome,
    EXTRACT(YEAR FROM data_completa) AS ano,
    'Q' || EXTRACT(QUARTER FROM data_completa) AS trimestre
FROM datas_extraidas
ORDER BY data_completa
    );
  
  