

-- O DuckDB consegue ler ficheiros Parquet (do Delta Lake) diretamente com esta função
WITH silver_vendas AS (
    SELECT * FROM read_parquet('/opt/airflow/datalake/silver/vendas_delta/*.parquet')
)

SELECT
    id_produto,
    COUNT(id_venda) as total_vendas,
    SUM(valor) as receita_total,
    MAX(data_venda) as data_ultima_venda
FROM silver_vendas
GROUP BY id_produto