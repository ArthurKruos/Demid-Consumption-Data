"""
Painel de análise reutilizável — DEMID / PIBIC
==============================================
Renderiza as abas de análise (público, rede de comunidades, discurso,
marcadores, exportação) a partir de DataFrames no schema unificado.

Usado tanto pelo módulo de YouTube quanto pelo de Reddit. A terminologia
(canal/subreddit, vídeo/post) é parametrizada por `labels`.
"""

import time
import streamlit as st

import analise_youtube as ay
from getdata.youtube_collector import exportar_maxqda


# Conjuntos de rótulos por plataforma
LABELS_YOUTUBE = {
    "unidade": "canal", "unidade_pl": "canais",
    "item": "vídeo", "item_pl": "vídeos", "metrica_pop": "views",
}
LABELS_REDDIT = {
    "unidade": "subreddit", "unidade_pl": "subreddits",
    "item": "post", "item_pl": "posts", "metrica_pop": "score",
}


def render(df_v, df_c, labels=LABELS_YOUTUBE, prefixo_export="pibic"):
    """Renderiza o conjunto completo de abas de análise."""

    if df_v is None or df_v.empty:
        st.warning("Nenhum dado coletado ainda.")
        return

    tab_vg, tab_pub, tab_rede, tab_disc, tab_lex, tab_exp = st.tabs([
        "📊 Visão geral",
        "👥 Público-alvo",
        "🕸️ Mapa de comunidades",
        "🗣️ Discurso (termos & KWIC)",
        "🔍 Marcadores",
        "📤 Exportar",
    ])

    # ===== VISÃO GERAL =====
    with tab_vg:
        c1, c2, c3 = st.columns(3)
        c1.metric(f"🎬 {labels['item_pl'].capitalize()}", len(df_v))
        c2.metric("💬 Comentários", len(df_c))
        c3.metric(
            f"📺 {labels['unidade_pl'].capitalize()}",
            df_v["canal_origem"].nunique() if "canal_origem" in df_v.columns else 0,
        )

        if "categoria_pibic" in df_v.columns:
            st.markdown(f"#### {labels['item_pl'].capitalize()} por categoria")
            st.bar_chart(
                df_v["categoria_pibic"].value_counts()
                .rename_axis("Categoria").to_frame(labels["item_pl"].capitalize())
            )

        st.markdown(f"#### Amostra de {labels['item_pl']}")
        cols = [c for c in ["categoria_pibic", "channel", "title", "view_count",
                            "like_count", "comment_count", "url"] if c in df_v.columns]
        st.dataframe(df_v[cols], use_container_width=True, hide_index=True)

    # ===== PÚBLICO-ALVO =====
    with tab_pub:
        st.markdown("#### Núcleo engajado (comentaristas mais ativos)")
        st.caption(
            f"Autores recorrentes são proxy do núcleo de cada comunidade. "
            f"*canais_distintos* indica quem circula entre {labels['unidade_pl']}."
        )
        st.dataframe(ay.top_comentaristas(df_c, 30),
                     use_container_width=True, hide_index=True)

        st.markdown("#### Perfil de engajamento por categoria")
        perfil = ay.perfil_engajamento(df_v, df_c)
        if not perfil.empty:
            st.dataframe(perfil, use_container_width=True, hide_index=True)
            st.caption(
                "*comentarios_por_autor* alto sugere comunidade concentrada e "
                "participativa; baixo sugere audiência mais dispersa."
            )

    # ===== MAPA DE COMUNIDADES =====
    with tab_rede:
        st.markdown("#### Rede de comunidades por público compartilhado")
        st.caption(
            f"Dois {labels['unidade_pl']} se conectam quando têm comentaristas em "
            "comum. Revela empiricamente quais comunidades se sobrepõem."
        )
        if df_c.empty:
            st.info("Sem comentários para construir a rede.")
        else:
            with st.spinner("Construindo rede..."):
                rede = ay.rede_canais(df_c)

            fig = ay.grafo_para_plotly(rede["grafo"])
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)

            ca, cb = st.columns(2)
            with ca:
                st.markdown(f"**{labels['unidade_pl'].capitalize()} mais centrais**")
                st.dataframe(rede["metricas"], use_container_width=True, hide_index=True)
            with cb:
                st.markdown("**Ligações mais fortes**")
                st.dataframe(rede["edges"], use_container_width=True, hide_index=True)

            st.markdown(
                f"**Autores-ponte** (comentam em ≥2 {labels['unidade_pl']}): "
                f"{len(rede['bridges'])}"
            )
            st.dataframe(rede["bridges"].head(50),
                         use_container_width=True, hide_index=True)

    # ===== DISCURSO =====
    with tab_disc:
        st.markdown("#### Repertório linguístico")
        if df_c.empty:
            st.info("Sem comentários para analisar.")
        else:
            cat_opts = ["(todas)"] + (
                sorted(df_c["categoria_pibic"].dropna().unique())
                if "categoria_pibic" in df_c.columns else []
            )
            cat_sel = st.selectbox("Filtrar por categoria", cat_opts, key="disc_cat")
            df_f = df_c if cat_sel == "(todas)" else df_c[df_c["categoria_pibic"] == cat_sel]

            ct, cn = st.columns(2)
            with ct:
                st.markdown("**Top termos**")
                st.dataframe(ay.frequencia_termos(df_f, 30),
                             use_container_width=True, hide_index=True)
            with cn:
                grau = st.radio("N-grama", [2, 3], horizontal=True, key="disc_ng",
                                format_func=lambda x: "Bigramas" if x == 2 else "Trigramas")
                st.dataframe(ay.ngramas(df_f, n=grau, top_n=30),
                             use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("#### Concordância (KWIC) — termo em contexto")
            st.caption(
                "Localiza um termo e mostra as palavras ao redor, para leitura "
                "qualitativa do uso (como a concordância do MAXQDA)."
            )
            termo = st.text_input("Termo para concordância", "woke", key="disc_kwic")
            if termo:
                kwic = ay.concordancia_kwic(df_f, termo, janela=8, max_resultados=200)
                st.write(f"Ocorrências: **{len(kwic)}**")
                st.dataframe(kwic, use_container_width=True, hide_index=True)

    # ===== MARCADORES =====
    with tab_lex:
        st.markdown("#### Léxico exploratório de marcadores discursivos")
        st.warning(
            "⚠️ **Exploratório, não classificatório.** O léxico apenas localiza "
            "ocorrências de termos para inspeção qualitativa. A interpretação é "
            "do pesquisador, idealmente no MAXQDA."
        )
        if df_c.empty:
            st.info("Sem comentários para analisar.")
        else:
            with st.spinner("Aplicando léxico..."):
                lex = ay.score_lexico(df_c)

            ce, ctr = st.columns(2)
            with ce:
                st.markdown("**Ocorrências por eixo**")
                st.dataframe(lex["resumo_eixos"], use_container_width=True, hide_index=True)
            with ctr:
                st.markdown("**Termos encontrados**")
                st.dataframe(lex["resumo_termos"].head(25),
                             use_container_width=True, hide_index=True)

            if not lex["por_categoria"].empty:
                st.markdown("**Marcadores por categoria**")
                st.dataframe(lex["por_categoria"], use_container_width=True, hide_index=True)

            st.markdown("#### Comentários mais marcados (leitura qualitativa)")
            st.dataframe(ay.comentarios_mais_marcados(lex["por_comentario"], 40),
                         use_container_width=True, hide_index=True)

    # ===== EXPORTAR =====
    with tab_exp:
        st.markdown("#### Exportar para análise de conteúdo (MAXQDA)")
        st.caption(
            "CSV estruturado: uma linha = um documento (post/vídeo ou comentário), "
            "com variáveis de comunidade, categoria, autor, data e engajamento."
        )
        df_maxqda = exportar_maxqda(df_v, df_c)
        st.write(f"Total de documentos: **{len(df_maxqda)}**")

        st.download_button(
            "⬇️ Baixar CSV para MAXQDA",
            data=df_maxqda.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{prefixo_export}_maxqda_{time.strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"dl_csv_{prefixo_export}",
        )
        if not df_c.empty:
            st.download_button(
                "⬇️ Baixar comentários (Parquet)",
                data=df_c.to_parquet(index=False),
                file_name=f"{prefixo_export}_comments.parquet",
                mime="application/octet-stream",
                key=f"dl_pq_{prefixo_export}",
            )
