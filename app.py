import streamlit as st
import pandas as pd
import time
from PIL import Image
import plotly.express as px

from getdata.reddit_collector import fetch_reddit_data, fetch_reddit_pibic
from getdata.youtube_collector import (
    fetch_youtube_data,
    fetch_pibic_data,
    exportar_maxqda,
)
from getdata.twitter_collector import fetch_twitter_data
from getdata.telegram_collector import fetch_telegram_data
from config.canais_pibic import CANAIS_PIBIC, VIDEOS_AVULSOS, listar_categorias
from config.subreddits_pibic import (
    SUBREDDITS_PIBIC,
    listar_categorias as listar_categorias_reddit,
)
import analise_youtube as ay
import painel_analise as painel
from analise_games_diario import processar_parquet, evolucao_termos
from ner_pipeline import extrair_entidades
from collections import Counter
import os


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
    [
        "Mapeamento YouTube",
        "Mapeamento Reddit (opcional)",
        "Redes Sociais (coleta)",
        "Diário Oficial",
    ],
)

# ==========================================================
# MÓDULO REDES SOCIAIS
# ==========================================================

if modo_app == "Redes Sociais":

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
    use_twitter = st.sidebar.checkbox("Twitter/X", value=False)
    use_telegram = st.sidebar.checkbox("Telegram", value=False)

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

    st.sidebar.subheader("📺 YouTube")
    st.sidebar.caption("✅ Funciona sem chave (yt-dlp). API Key é opcional.")
    YOUTUBE_API_KEY = st.sidebar.text_input(
        "YouTube API Key (opcional)", type="password",
        help="Só necessária como fallback se yt-dlp falhar."
    )
    yt_get_comments = st.sidebar.checkbox("Coletar comentários", value=True)
    yt_max_comments = st.sidebar.slider("Comentários por vídeo", 10, 300, 100, step=10)

    # -------------------------
    # CREDENCIAIS TWITTER/X
    # -------------------------

    if use_twitter:
        st.sidebar.subheader("🐦 Credenciais Twitter/X")
        TWITTER_BEARER = st.sidebar.text_input("Bearer Token", type="password")
        st.sidebar.caption("Plano gratuito: até 10 tweets por busca.")
    else:
        TWITTER_BEARER = ""

    # -------------------------
    # CREDENCIAIS TELEGRAM
    # -------------------------

    if use_telegram:
        st.sidebar.subheader("✈️ Credenciais Telegram")
        TELEGRAM_API_ID = st.sidebar.text_input("API ID", type="password")
        TELEGRAM_API_HASH = st.sidebar.text_input("API Hash", type="password")
        telegram_channels_input = st.sidebar.text_input(
            "Canais públicos (sem @, separados por vírgula)",
            "gamesbrasileiros,jogosindiesbr"
        )
        st.sidebar.caption("Apenas canais/grupos públicos. Obtenha credenciais em my.telegram.org")
    else:
        TELEGRAM_API_ID = ""
        TELEGRAM_API_HASH = ""
        telegram_channels_input = ""

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
                        keywords=keywords,
                        max_results=20,
                        get_comments=yt_get_comments,
                        max_comments=yt_max_comments,
                        api_key=YOUTUBE_API_KEY,
                    )

                    if not df_youtube.empty:
                        all_results.append(df_youtube)

                # =========================
                # TWITTER/X
                # =========================
                if use_twitter:
                    df_twitter = fetch_twitter_data(
                        bearer_token=TWITTER_BEARER,
                        keywords=keywords,
                        limit=10
                    )
                    if not df_twitter.empty:
                        all_results.append(df_twitter)
                    else:
                        st.warning("Twitter/X: nenhum tweet encontrado ou credenciais inválidas.")

                # =========================
                # TELEGRAM
                # =========================
                if use_telegram:
                    channels = [
                        c.strip().lstrip("@")
                        for c in telegram_channels_input.split(",")
                        if c.strip()
                    ]
                    df_telegram, err = fetch_telegram_data(
                        api_id=TELEGRAM_API_ID,
                        api_hash=TELEGRAM_API_HASH,
                        channels=channels,
                        keywords=keywords,
                        limit=200
                    )
                    if err:
                        st.warning(f"Telegram: {err}")
                    elif not df_telegram.empty:
                        all_results.append(df_telegram)

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
# MÓDULO MAPEAMENTO PIBIC
# ==========================================================

elif modo_app == "Mapeamento YouTube":

    st.title("🔬 Mapeamento YouTube – Comunidades")
    st.markdown(
        "Coleta dirigida de **canais do YouTube** para mapeamento de comunidades "
        "e análise de discurso. Os dados alimentam a análise de conteúdo (MAXQDA)."
    )

    st.info(
        "ℹ️ **Nota metodológica:** os rótulos de categoria são organizacionais, "
        "não classificações validadas. A caracterização ideológica é resultado da "
        "análise de conteúdo, não um pressuposto da coleta."
    )

    PIBIC_VIDEOS_PATH = "data/pibic_videos.parquet"
    PIBIC_COMMENTS_PATH = "data/pibic_comments.parquet"

    # -------------------------
    # SIDEBAR
    # -------------------------
    st.sidebar.header("⚙️ Coleta YouTube")

    fonte_canais = st.sidebar.radio(
        "Fonte dos canais",
        ["Lista curada (PIBIC)", "Digitar meus canais"],
    )

    categorias = listar_categorias()

    if fonte_canais == "Lista curada (PIBIC)":
        cats_sel = st.sidebar.multiselect(
            "Categorias a coletar", categorias, default=categorias
        )
        canais_filtrados = [c for c in CANAIS_PIBIC if c["categoria"] in cats_sel]
        incluir_avulsos = st.sidebar.checkbox("Incluir vídeos avulsos do documento", value=True)
    else:
        st.sidebar.markdown("**Canais personalizados**")
        st.sidebar.caption("Um por linha. Formato: `URL_ou_@handle | categoria`")
        canais_txt = st.sidebar.text_area(
            "Canais",
            "https://www.youtube.com/@TolarianCommunityCollege | Magic\nhttps://www.youtube.com/@AsmonTV | Streamer",
            height=120,
        )
        canais_filtrados = []
        for linha in canais_txt.splitlines():
            linha = linha.strip()
            if not linha:
                continue
            if "|" in linha:
                url, cat = linha.split("|", 1)
                canais_filtrados.append({"url": url.strip(), "categoria": cat.strip()})
            else:
                canais_filtrados.append({"url": linha, "categoria": "Personalizado"})
        cats_sel = list({c["categoria"] for c in canais_filtrados})
        incluir_avulsos = False

    max_videos = st.sidebar.slider("Vídeos por canal", 5, 100, 20, step=5)
    pibic_comments = st.sidebar.checkbox("Coletar comentários", value=True)
    pibic_max_comments = st.sidebar.slider("Comentários por vídeo", 10, 300, 100, step=10)

    st.subheader("📋 Canais selecionados")
    st.dataframe(pd.DataFrame(canais_filtrados), use_container_width=True, hide_index=True)
    st.caption(f"{len(canais_filtrados)} canais a coletar.")

    coletar_btn = st.sidebar.button("🚀 Iniciar Coleta")

    # -------------------------
    # EXECUÇÃO
    # -------------------------
    if coletar_btn:
        if not canais_filtrados:
            st.error("Defina ao menos um canal.")
        else:
            progress_bar = st.progress(0.0, text="Iniciando coleta...")

            def _cb(idx, total, nome):
                progress_bar.progress(min(idx / total, 1.0),
                                      text=f"Coletando {idx}/{total}: {nome}")

            avulsos = VIDEOS_AVULSOS if incluir_avulsos else None
            if avulsos:
                avulsos = [v for v in avulsos if v["categoria"] in cats_sel]

            with st.spinner("Coletando dados do YouTube (yt-dlp, sem cota)..."):
                resultado = fetch_pibic_data(
                    canais=canais_filtrados,
                    max_videos=max_videos,
                    get_comments=pibic_comments,
                    max_comments=pibic_max_comments,
                    videos_avulsos=avulsos,
                    progress_callback=_cb,
                )
            progress_bar.progress(1.0, text="Coleta concluída!")
            st.success(
                f"Coleta concluída: {len(resultado['videos'])} vídeos e "
                f"{len(resultado['comments'])} comentários."
            )

    # -------------------------
    # PAINEL DE ANÁLISE (compartilhado)
    # -------------------------
    st.markdown("---")
    if not os.path.exists(PIBIC_VIDEOS_PATH):
        st.warning(
            "Nenhuma coleta realizada ainda. Configure os canais na barra lateral "
            "e clique em **Iniciar Coleta**."
        )
    else:
        df_v = pd.read_parquet(PIBIC_VIDEOS_PATH)
        df_c = (
            pd.read_parquet(PIBIC_COMMENTS_PATH)
            if os.path.exists(PIBIC_COMMENTS_PATH) else pd.DataFrame()
        )
        painel.render(df_v, df_c, painel.LABELS_YOUTUBE, prefixo_export="youtube")

# ==========================================================
# MÓDULO MAPEAMENTO REDDIT
# ==========================================================

elif modo_app == "Mapeamento Reddit":

    st.title("🤖 Mapeamento Reddit – Comunidades")
    st.markdown(
        "Coleta dirigida de **subreddits** para mapeamento de comunidades e "
        "análise de discurso. Mesma camada analítica do módulo YouTube."
    )

    st.info(
        "ℹ️ **Nota metodológica:** os rótulos de categoria são organizacionais, "
        "não classificações validadas."
    )

    REDDIT_POSTS_PATH = "data/reddit_pibic_posts.parquet"
    REDDIT_COMMENTS_PATH = "data/reddit_pibic_comments.parquet"

    # -------------------------
    # SIDEBAR
    # -------------------------
    st.sidebar.header("⚙️ Coleta Reddit")

    st.sidebar.subheader("🔒 Credenciais Reddit")
    R_CLIENT_ID = st.sidebar.text_input("Client ID", type="password", key="r_id")
    R_CLIENT_SECRET = st.sidebar.text_input("Client Secret", type="password", key="r_secret")
    R_USER_AGENT = "DemidDataHub_v1"

    fonte_sr = st.sidebar.radio(
        "Fonte dos subreddits",
        ["Lista-semente", "Digitar meus subreddits"],
    )

    if fonte_sr == "Lista-semente":
        cats_sr = listar_categorias_reddit()
        cats_sr_sel = st.sidebar.multiselect("Categorias", cats_sr, default=cats_sr)
        subreddits_sel = [s for s in SUBREDDITS_PIBIC if s["categoria"] in cats_sr_sel]
    else:
        st.sidebar.caption("Um por linha. Formato: `subreddit | categoria`")
        sr_txt = st.sidebar.text_area(
            "Subreddits",
            "magicTCG | Magic\npokemon | Pokémon\ngamesEcultura | Jogos BR",
            height=120,
        )
        subreddits_sel = []
        for linha in sr_txt.splitlines():
            linha = linha.strip()
            if not linha:
                continue
            if "|" in linha:
                nome, cat = linha.split("|", 1)
                subreddits_sel.append({"nome": nome.strip(), "categoria": cat.strip()})
            else:
                subreddits_sel.append({"nome": linha, "categoria": "Personalizado"})

    ordenacao = st.sidebar.selectbox(
        "Ordenação dos posts", ["hot", "top", "new"],
        format_func=lambda x: {"hot": "Em alta (hot)", "top": "Top do ano", "new": "Mais novos"}[x],
    )
    limite_posts = st.sidebar.slider("Posts por subreddit", 5, 100, 30, step=5)
    r_comments = st.sidebar.checkbox("Coletar comentários", value=True)
    r_max_comments = st.sidebar.slider("Comentários por post", 10, 300, 100, step=10)

    st.subheader("📋 Subreddits selecionados")
    st.dataframe(pd.DataFrame(subreddits_sel), use_container_width=True, hide_index=True)
    st.caption(f"{len(subreddits_sel)} subreddits a coletar.")

    coletar_r_btn = st.sidebar.button("🚀 Iniciar Coleta Reddit")

    # -------------------------
    # EXECUÇÃO
    # -------------------------
    if coletar_r_btn:
        if not R_CLIENT_ID or not R_CLIENT_SECRET:
            st.error("Informe as credenciais do Reddit (Client ID e Secret).")
        elif not subreddits_sel:
            st.error("Defina ao menos um subreddit.")
        else:
            progress_bar = st.progress(0.0, text="Iniciando coleta...")

            def _cb_r(idx, total, nome):
                progress_bar.progress(min(idx / total, 1.0),
                                      text=f"Coletando {idx}/{total}: {nome}")

            with st.spinner("Coletando dados do Reddit (PRAW)..."):
                resultado = fetch_reddit_pibic(
                    client_id=R_CLIENT_ID,
                    client_secret=R_CLIENT_SECRET,
                    user_agent=R_USER_AGENT,
                    subreddits=subreddits_sel,
                    limit=limite_posts,
                    ordenacao=ordenacao,
                    get_comments=r_comments,
                    max_comments=r_max_comments,
                    progress_callback=_cb_r,
                )
            progress_bar.progress(1.0, text="Coleta concluída!")

            if resultado.get("erro"):
                st.error(resultado["erro"])
            else:
                st.success(
                    f"Coleta concluída: {len(resultado['videos'])} posts e "
                    f"{len(resultado['comments'])} comentários."
                )

    # -------------------------
    # PAINEL DE ANÁLISE (compartilhado)
    # -------------------------
    st.markdown("---")
    if not os.path.exists(REDDIT_POSTS_PATH):
        st.warning(
            "Nenhuma coleta realizada ainda. Informe as credenciais, configure os "
            "subreddits e clique em **Iniciar Coleta Reddit**."
        )
    else:
        df_v = pd.read_parquet(REDDIT_POSTS_PATH)
        df_c = (
            pd.read_parquet(REDDIT_COMMENTS_PATH)
            if os.path.exists(REDDIT_COMMENTS_PATH) else pd.DataFrame()
        )
        painel.render(df_v, df_c, painel.LABELS_REDDIT, prefixo_export="reddit")

# ==========================================================
# MÓDULO DIÁRIO OFICIAL
# ==========================================================

else:

    st.header("🏛️ Painel Analítico – Diário Oficial")

    PARQUET_PATH = "data/doe_raw.parquet"

    # ==========================================================
    # CACHE NER
    # ==========================================================

    @st.cache_data(show_spinner=False)
    def processar_ner(textos):

        entidades_lista = []

        for texto in textos:

            entidades = extrair_entidades(texto)

            entidades_lista.append(entidades)

        return entidades_lista

    try:

        # ==========================================================
        # PROCESSAMENTO BASE
        # ==========================================================

        with st.spinner("Processando base do Diário Oficial..."):

            resultados = processar_parquet(
                PARQUET_PATH,
                salvar_csv=False
            )

        df_completo = resultados["df_completo"]
        resumo_anual = resultados["resumo_anual"]
        resumo_mensal = resultados["resumo_mensal"]
        top_diarios = resultados["top_diarios"]

        # ==========================================================
        # VALIDAÇÃO
        # ==========================================================

        if "conteudo" not in df_completo.columns:

            st.error("Coluna 'conteudo' não encontrada.")

            st.stop()

        # ==========================================================
        # LIMITE NER
        # ==========================================================

        LIMITE_NER = 200

        df_ner = df_completo.head(LIMITE_NER).copy()

        # ==========================================================
        # NER
        # ==========================================================

        with st.spinner("Extraindo entidades do Diário Oficial..."):

            textos = df_ner["conteudo"].fillna("").tolist()

            entidades_lista = processar_ner(textos)

        df_ner["entidades"] = entidades_lista

        # ==========================================================
        # ORGANIZAÇÃO DAS ENTIDADES
        # ==========================================================

        df_ner["orgs"] = df_ner["entidades"].apply(
            lambda x: x.get("ORG", [])
        )

        df_ner["pessoas"] = df_ner["entidades"].apply(
            lambda x: x.get("PER", []) + x.get("PERSON", [])
        )

        df_ner["locais"] = df_ner["entidades"].apply(
            lambda x: x.get("LOC", [])
        )

        # ==========================================================
        # SALVAR PARQUET ENRIQUECIDO
        # ==========================================================

        df_ner.to_parquet(
            "data/doe_ner.parquet",
            index=False
        )

        # ==========================================================
        # MÉTRICAS
        # ==========================================================

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

        # ==========================================================
        # DOWNLOAD
        # ==========================================================

        st.subheader("📥 Download dos Dados")

        st.write("Tamanho da base:", df_completo.shape)

        csv = df_completo.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="⬇️ Baixar base completa do Diário Oficial (CSV)",
            data=csv,
            file_name="base_diario_oficial_pb.csv",
            mime="text/csv"
        )

        st.markdown("---")

        # ==========================================================
        # EVOLUÇÃO MENSAL
        # ==========================================================

        st.subheader("📈 Evolução Mensal – Intensidade Games")

        fig_mensal = px.line(
            resumo_mensal,
            x="ano_mes",
            y="intensidade_games",
            markers=True
        )

        st.plotly_chart(
            fig_mensal,
            use_container_width=True
        )

        # ==========================================================
        # EVOLUÇÃO ANUAL
        # ==========================================================

        st.subheader("📊 Evolução Anual – Diários com Games")

        fig_anual = px.bar(
            resumo_anual,
            x="ano",
            y="diarios_com_games"
        )

        st.plotly_chart(
            fig_anual,
            use_container_width=True
        )

        # ==========================================================
        # TOP DIÁRIOS
        # ==========================================================

        st.subheader("🏆 Top 20 Diários mais relevantes")

        st.dataframe(
            top_diarios,
            use_container_width=True
        )

        # ==========================================================
        # TOP ÓRGÃOS
        # ==========================================================

        todas_orgs = []

        for lista in df_ner["orgs"]:

            todas_orgs.extend(lista)

        freq_orgs = Counter(todas_orgs)

        top_orgs = pd.DataFrame(
            freq_orgs.most_common(15),
            columns=["orgao", "frequencia"]
        )

        st.subheader("🏢 Top Órgãos Citados")

        fig_orgs = px.bar(
            top_orgs,
            x="orgao",
            y="frequencia"
        )

        st.plotly_chart(
            fig_orgs,
            use_container_width=True
        )

        # ==========================================================
        # ENTIDADES EXTRAÍDAS
        # ==========================================================

        st.subheader("🏛️ Entidades Extraídas")

        st.data_editor(
            df_ner[
                [
                    "data_doe",
                    "orgs",
                    "pessoas",
                    "locais"
                ]
            ],
            use_container_width=True
        )

        # ==========================================================
        # EVOLUÇÃO DE TERMOS
        # ==========================================================

        st.subheader("🔎 Evolução de Termos")

        termos_input = st.text_input(
            "Digite termos separados por vírgula:",
            "jogos digitais, unity, esports"
        )

        if termos_input:

            termos_lista = [
                t.strip()
                for t in termos_input.split(",")
                if t.strip()
            ]

            df_evolucao = evolucao_termos(
                termos_lista,
                df_completo
            )

            fig_termos = px.line(
                df_evolucao,
                x="ano_mes",
                y=termos_lista,
                markers=True
            )

            st.plotly_chart(
                fig_termos,
                use_container_width=True
            )

    except FileNotFoundError:

        st.error("Arquivo doe_raw.parquet não encontrado.")

    except Exception as e:

        st.error(f"Erro ao processar painel: {e}")