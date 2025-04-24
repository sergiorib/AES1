import streamlit as st
import plotly.express as px
import pandas as pd
import duckdb
import os

# ==========================================
# CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="DeltaPay | Executivo",
    page_icon="🔺",
    layout="wide"
)

# ==========================================
# DEFINIÇÃO DOS CAMINHOS
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('/opt/airflow/datalake'):
    ROOT_DIR = '/opt/airflow'
else:
    ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

DB_PATH = os.path.join(ROOT_DIR, 'datalake', 'gold', 'aes1_gold.duckdb')

# ==========================================
# 🚀 A MÁGICA DA CONCORRÊNCIA: ABRIR E FECHAR
# ==========================================
# ATENÇÃO: Sem decoradores de cache aqui! O objetivo é não segurar o arquivo.
def carregar_dados(query: str) -> pd.DataFrame:
    """
    Abre o banco, extrai os dados como um DataFrame Pandas 
    e fecha IMEDIATAMENTE a conexão para não bloquear o dbt no Airflow.
    """
    if not os.path.exists(DB_PATH):
        st.warning("⚠️ Banco de dados Gold ainda não gerado. Execute a pipeline no Airflow primeiro.")
        return pd.DataFrame()
        
    try:
        # O bloco 'with' garante que o lock seja liberado no fim da execução
        with duckdb.connect(DB_PATH, read_only=True) as con:
            df = con.execute(query).df()
            return df
    except Exception as e:
        st.error(f"Erro ao consultar o banco de dados Gold: {e}")
        return pd.DataFrame()

# ==========================================
# CABEÇALHO DO DASHBOARD
# ==========================================
col1, col2 = st.columns([1, 4])
with col1:
    st.markdown("## 🔺 DeltaPay")
with col2:
    st.markdown("#### Painel Analítico e Monitoramento Operacional")

st.divider()

# ==========================================
# ABAS DE NAVEGAÇÃO
# ==========================================
tab_cohort, tab_fraude = st.tabs(["📊 Visão de Negócio e Safras", "🚨 Monitoramento de Risco (Fraudes)"])

# ==========================================
# ABA 1: VISÃO DE NEGÓCIO E SAFRAS
# ==========================================
with tab_cohort:
    st.subheader("Composição da Receita: Qual safra gera mais volume hoje?")
    
    query_safra = """
        SELECT 
            u.safra_mes,
            COUNT(DISTINCT f.id_usuario) as clientes_ativos_hoje,
            SUM(f.valor_transacao) as volume_transacionado
        FROM fact_transactions f
        JOIN dim_users u ON f.id_usuario = u.id_usuario
        GROUP BY u.safra_mes
        ORDER BY u.safra_mes
    """
    df_safra = carregar_dados(query_safra)
    
    if not df_safra.empty:
        # 1º Gráfico: Volume Financeiro
        with st.container(border=True):
            fig_vol = px.bar(
                df_safra, 
                x='safra_mes', 
                y='volume_transacionado', 
                title="<b>Volume Financeiro por Safra</b>",
                labels={'safra_mes': 'Mês da Safra de Cadastro', 'volume_transacionado': 'Volume Transacionado (R$)'}
            )
            fig_vol.update_traces(marker_color='#00d2ff')
            fig_vol.update_layout(
                plot_bgcolor='#3c096c', paper_bgcolor='#3c096c', font=dict(color='white'),
                margin=dict(l=40, r=40, t=60, b=40), xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor='rgba(255, 255, 255, 0.2)', tickprefix="R$ ")
            )
            st.plotly_chart(fig_vol, use_container_width=True)
            
        # 2º Gráfico: Clientes Ativos
        with st.container(border=True):
            fig_clientes = px.line(
                df_safra, 
                x='safra_mes', 
                y='clientes_ativos_hoje', 
                title="<b>Adesão (Clientes Ativos no Período)</b>",
                labels={'safra_mes': 'Mês da Safra de Cadastro', 'clientes_ativos_hoje': 'Qtd. de Clientes Ativos'},
                markers=True
            )
            fig_clientes.update_traces(
                line=dict(color='#00ff88', width=3),
                marker=dict(size=10, color='white', line=dict(width=2, color='#00ff88'))
            )
            fig_clientes.update_layout(
                plot_bgcolor='#3c096c', paper_bgcolor='#3c096c', font=dict(color='white'),
                margin=dict(l=40, r=40, t=60, b=40), xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor='rgba(255, 255, 255, 0.2)')
            )
            st.plotly_chart(fig_clientes, use_container_width=True)

# ==========================================
# ABA 2: MONITORAMENTO DE RISCO
# ==========================================
with tab_fraude:
    st.subheader("Motor de Anomalias (Camada Silver)")
    
    query_fraudes = """
        SELECT 
            data_hora,
            valor_transacao,
            flag_suspeita,
            tipo_transacao
        FROM fact_transactions
        ORDER BY data_hora DESC
        LIMIT 25000
    """
    df_fraudes = carregar_dados(query_fraudes)
    
    if not df_fraudes.empty:
        # Cálculos de KPI
        total_transacoes = len(df_fraudes)
        alertas = df_fraudes['flag_suspeita'].sum()
        taxa = (alertas / total_transacoes) * 100 if total_transacoes > 0 else 0
        
        # Exibição dos KPIs
        kpi1, kpi2, kpi3 = st.columns(3)
        with st.container(border=True): kpi1.metric("Total de Transações (Amostra)", f"{total_transacoes:,}")
        with st.container(border=True): kpi2.metric("Alertas de Risco", f"{alertas:,}")
        with st.container(border=True): kpi3.metric("Taxa de Suspeita", f"{taxa:.2f}%")
        
        # Gráfico de Dispersão (Scatter)
        with st.container(border=True):
            fig_scatter = px.scatter(
                df_fraudes, 
                x="data_hora", 
                y="valor_transacao", 
                color="flag_suspeita",
                color_discrete_map={False: '#00d2ff', True: '#ff4b4b'}, # Azul para normais, Vermelho para fraudes
                title="<b>Monitoramento de Transações Suspeitas</b>",
                labels={'data_hora': 'Horário da Transação', 'valor_transacao': 'Valor da Transação (R$)', 'flag_suspeita': 'Risco Detectado'}
            )
            fig_scatter.update_layout(
                plot_bgcolor='#3c096c', paper_bgcolor='#3c096c', font=dict(color='white'),
                margin=dict(l=40, r=40, t=60, b=40), xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor='rgba(255, 255, 255, 0.2)', tickprefix="R$ ")
            )
            fig_scatter.update_traces(marker=dict(size=6, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
            st.plotly_chart(fig_scatter, use_container_width=True)