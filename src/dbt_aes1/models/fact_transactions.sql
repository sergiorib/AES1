{{ config(materialized='table') }}

SELECT
    transaction_id AS id_transacao,
    customer_id AS id_usuario,
    
    -- Criando a FK para a dim_date (Formato YYYYMMDD)
    CAST(strftime(data_hora, '%Y%m%d') AS INTEGER) AS id_data,
    
    -- Criando a FK para a dim_status usando o mesmo hash
    md5(status) AS id_status_sk,
    
    transaction_type AS tipo_transacao,
    transaction_category AS categoria_transacao,
    data_hora,
    valor_transacao,
    taxa_receita,
    flag_suspeita,
    valor_medio_historico
FROM {{ source('datalake_silver', 'aes1_transacoes') }}