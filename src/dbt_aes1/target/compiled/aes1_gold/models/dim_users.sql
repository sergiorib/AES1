

WITH clientes_unicos AS (
    -- Extrai e deduplica os usuários da tabela fato
    SELECT
        customer_id AS id_usuario,
        MAX(nome_cliente) AS nome_usuario, 
        MAX(cpf_mascarado) AS cpf,
        -- Converte a string '15/01/2024' para o tipo DATE
        CAST(strptime(MAX(customer_signup_date), '%d/%m/%Y') AS DATE) AS data_cadastro
    FROM read_parquet('/opt/airflow/datalake/silver/aes1_transacoes/*/*.parquet')
    GROUP BY customer_id
)

SELECT
    id_usuario,
    nome_usuario,
    cpf,
    data_cadastro,
    -- Regra de Negócio: Safra (Cohort)
    strftime(data_cadastro, '%Y-%m') AS safra_mes,
    EXTRACT(YEAR FROM data_cadastro) AS safra_ano,
    
    -- Estrutura de Slowly Changing Dimension (SCD2)
    CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS start_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    TRUE AS is_current
FROM clientes_unicos