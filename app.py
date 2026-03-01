import streamlit as st
import pandas as pd
import time
from PIL import Image
import plotly.express as px

from getdata.reddit_collector import fetch_reddit_data
from getdata.youtube_collector import fetch_youtube_data
from analise_games_diario import processar_parquet, evolucao_termos


# ==========================================================
# CONFIGURAÇÃO DA PÁGINA
# ==========================================================

im = Image.open(".streamlit/img/logodemid.jpg")

st.set_page_config(
    page_title="Demid Consumption Data",
    page_icon=im,
    layout="wide"
)

# ==========================================================
# MENU LATERAL
# ==========================================================

modo_app = st.sidebar.radio(
    "Escolha o módulo:",
    ["Reddit + YouTube", "Diário Oficial"]
)

# ==========================================================
# MÓDULO REDES SOCIAIS
# ==========================================================

if modo_app == "Reddit + YouTube":

    st.title("🎮 Coletor de Dados – Comunidades Gamer")
    st.markdown("Configure as palavras-chave e credenciais na barra lateral.")
    st.markdown("---")

    # -------------------------
    # SIDEBAR CONFIGURAÇÕES
    # -------------------------

    st.sidebar.header("⚙️ Configurações")

    keywords_input = st.sidebar.text_input(
        "Palavras-Chave (separadas por vírgula)",
        "Elden Ring, WoW, Final Fantasy"
    )

    st.sidebar.subheader("🌐 Fontes")
    use_reddit = st.sidebar.checkbox("Reddit", value=True)
    use_youtube = st.sidebar.checkbox("YouTube", value=True)

    # -------------------------
    # CREDENCIAIS REDDIT
    # -------------------------

    st.sidebar.subheader("🔒 Credenciais Reddit")

    CLIENT_ID = st.sidebar.text_input("Client ID", type="password")
    CLIENT_SECRET = st.sidebar.text_input("Client Secret", type="password")
    USER_AGENT = "DemidDataHub_v1"

    # -------------------------
    # CREDENCIAIS YOUTUBE
    # -------------------------

    st.sidebar.subheader("📺 Credenciais YouTube")

    YOUTUBE_API_KEY = st.sidebar.text_input("YouTube API Key", type="password")

    search_button = st.sidebar.button("🚀 Iniciar Coleta")

    # -------------------------
    # EXECUÇÃO
    # -------------------------

    if search_button:

        keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]

        if not keywords:
            st.error("Insira pelo menos uma palavra-chave.")
        else:

            st.header(f"Resultados para: {', '.join(keywords)}")

            all_results = []

            with st.spinner("Coletando dados..."):

                # =========================
                # REDDIT
                # =========================
                if use_reddit:

                    df_reddit = fetch_reddit_data(
                        client_id=CLIENT_ID,
                        client_secret=CLIENT_SECRET,
                        user_agent=USER_AGENT,
                        keywords=keywords,
                        limit=100
                    )

                    if not df_reddit.empty:
                        df_reddit.to_parquet("data/reddit_raw.parquet", index=False)
                        all_results.append(df_reddit)

                # =========================
                # YOUTUBE
                # =========================
                if use_youtube:

                    df_youtube = fetch_youtube_data(
                        api_key=YOUTUBE_API_KEY,
                        keywords=keywords,
                        max_results=50
                    )

                    if not df_youtube.empty:
                        df_youtube.to_parquet("data/youtube_raw.parquet", index=False)
                        all_results.append(df_youtube)

            # =========================
            # RESULTADO FINAL
            # =========================

            if all_results:

                df_final = pd.concat(all_results, ignore_index=True)

                st.success("Coleta concluída!")
                st.dataframe(df_final, use_container_width=True)

                resumo = df_final["Fonte"].value_counts().rename_axis("Fonte").to_frame("Total")
                st.subheader("📊 Resumo por Fonte")
                st.dataframe(resumo)

                parquet_data = df_final.to_parquet(index=False)

                st.download_button(
                    label="📥 Baixar Dados (Parquet)",
                    data=parquet_data,
                    file_name=f"dados_{time.strftime('%Y%m%d_%H%M%S')}.parquet",
                    mime="application/octet-stream"
                )

            else:
                st.warning("Nenhum dado encontrado.")

# ==========================================================
# MÓDULO DIÁRIO OFICIAL
# ==========================================================

else:

    st.header("🏛️ Painel Analítico – Diário Oficial")

    PARQUET_PATH = "data/doe_raw.parquet"

    try:

        with st.spinner("Processando base do Diário Oficial..."):
            resultados = processar_parquet(PARQUET_PATH, salvar_csv=False)

        df_completo = resultados["df_completo"]
        resumo_anual = resultados["resumo_anual"]
        resumo_mensal = resultados["resumo_mensal"]
        top_diarios = resultados["top_diarios"]

        # =========================
        # CARDS
        # =========================

        total_diarios = len(df_completo)
        total_games = df_completo["flag_games"].sum()
        total_seguranca = df_completo["flag_seguranca"].sum()
        intensidade_total = df_completo["score_games"].sum()

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("📄 Total de Diários", total_diarios)
        col2.metric("🎮 Diários com Games", total_games)
        col3.metric("🔐 Diários com Segurança", total_seguranca)
        col4.metric("🔥 Intensidade Total Games", intensidade_total)

        st.markdown("---")

        # =========================
        # EVOLUÇÃO MENSAL
        # =========================

        st.subheader("📈 Evolução Mensal – Intensidade Games")

        fig_mensal = px.line(
            resumo_mensal,
            x="ano_mes",
            y="intensidade_games",
            markers=True
        )

        st.plotly_chart(fig_mensal, use_container_width=True)

        # =========================
        # EVOLUÇÃO ANUAL
        # =========================

        st.subheader("📊 Evolução Anual – Diários com Games")

        fig_anual = px.bar(
            resumo_anual,
            x="ano",
            y="diarios_com_games"
        )

        st.plotly_chart(fig_anual, use_container_width=True)

        # =========================
        # TOP DIÁRIOS
        # =========================

        st.subheader("🏆 Top 20 Diários mais relevantes")
        st.dataframe(top_diarios, use_container_width=True)

        # =========================
        # EVOLUÇÃO DE TERMOS
        # =========================

        st.subheader("🔎 Evolução de Termos")

        termos_input = st.text_input(
            "Digite termos separados por vírgula:",
            "jogos digitais, unity, esports"
        )

        if termos_input:

            termos_lista = [t.strip() for t in termos_input.split(",") if t.strip()]
            df_evolucao = evolucao_termos(termos_lista, df_completo)

            fig_termos = px.line(
                df_evolucao,
                x="ano_mes",
                y=termos_lista,
                markers=True
            )

            st.plotly_chart(fig_termos, use_container_width=True)

    except FileNotFoundError:
        st.error("Arquivo doe_raw.parquet não encontrado.")
    except Exception as e:
        st.error(f"Erro ao processar painel: {e}")