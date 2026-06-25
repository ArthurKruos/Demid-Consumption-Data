"""
Análise de comunidades e discurso no YouTube — DEMID / PIBIC
============================================================
Camada analítica sobre os comentários e vídeos coletados, voltada aos
objetivos da iniciação científica:

  - Mapeamento de comunidades digitais (núcleo de público, pontes entre canais)
  - Repertórios linguísticos (frequência de termos, n-gramas)
  - Subsídios para análise de discurso (concordância KWIC, léxico customizável)
  - Atores citados (NER sobre comentários)

NOTA METODOLÓGICA: o léxico de marcadores discursivos é EXPLORATÓRIO e
totalmente editável pelo pesquisador. Ele não classifica nem rotula
ideologicamente — apenas localiza ocorrências de termos para inspeção
qualitativa posterior (ex.: no MAXQDA). A interpretação é do pesquisador.
"""

import re
import unicodedata
from collections import Counter, defaultdict

import pandas as pd
import networkx as nx

# ============================================================
# STOPWORDS (português + ruído típico de comentários)
# ============================================================

STOPWORDS_PT = {
    "a", "o", "e", "é", "de", "do", "da", "dos", "das", "em", "no", "na", "nos",
    "nas", "um", "uma", "uns", "umas", "que", "se", "por", "para", "pra", "com",
    "sem", "como", "mas", "mais", "menos", "ou", "ao", "aos", "à", "às", "the",
    "of", "to", "and", "is", "in", "it", "you", "ele", "ela", "eles", "elas",
    "eu", "tu", "voce", "você", "vc", "nós", "nos", "vocês", "isso", "isto",
    "esse", "essa", "este", "esta", "aquele", "aquela", "lá", "la", "aqui", "ali",
    "já", "ja", "não", "nao", "sim", "também", "tambem", "muito", "muita", "pouco",
    "ser", "ter", "estar", "foi", "são", "sao", "tem", "tinha", "vai", "vou",
    "está", "esta", "estão", "estao", "ser", "seu", "sua", "seus", "suas", "meu",
    "minha", "te", "me", "lhe", "nos", "os", "as", "pelo", "pela", "num", "numa",
    "porque", "porquê", "quando", "onde", "qual", "quais", "quem", "todo", "toda",
    "todos", "todas", "cada", "outro", "outra", "mesmo", "mesma", "assim", "então",
    "entao", "ainda", "só", "so", "bem", "vez", "vezes", "agora", "depois", "antes",
    "sobre", "entre", "até", "ate", "desde", "cara", "gente", "coisa", "pq", "tá",
    "ta", "né", "ne", "aí", "ai", "vdd", "kkk", "kkkk", "kkkkk", "rs", "hahaha",
    "pois", "logo", "nem", "tao", "tão", "fez", "faz", "ver", "vi", "dia",
    # Inglês (canais internacionais como Asmongold e Tolarian)
    "the", "and", "for", "are", "but", "not", "you", "all", "any", "can", "had",
    "her", "was", "one", "our", "out", "his", "has", "him", "how", "man", "new",
    "now", "old", "see", "two", "way", "who", "boy", "did", "its", "let", "put",
    "say", "she", "too", "use", "that", "this", "they", "them", "then", "than",
    "with", "have", "from", "your", "what", "when", "will", "would", "there",
    "their", "been", "were", "just", "like", "more", "some", "such", "only",
    "into", "over", "also", "back", "even", "much", "most", "very", "want",
    "well", "make", "made", "know", "good", "get", "got", "because", "about",
    "could", "should", "people", "really", "think", "going", "right", "still",
    "being", "doesnt", "dont", "didnt", "cant", "wont", "thats", "youre",
    "theyre", "isnt", "arent", "yeah", "gonna", "wanna", "lol", "lmao",
}

# Tokens curtos e numéricos costumam ser ruído
TOKEN_RE = re.compile(r"[a-zà-ú]{3,}", re.IGNORECASE)


# ============================================================
# LÉXICO EXPLORATÓRIO DE MARCADORES (editável pelo pesquisador)
# ============================================================
# Agrupado por EIXO temático apenas para organizar a inspeção.
# Termos extraídos/inspirados no contexto do documento de origem do PIBIC.
# NÃO constitui classificação — é um índice de busca para análise qualitativa.

LEXICO_DISCURSO_PADRAO = {
    "marcadores_identitarios": [
        "woke", "wokismo", "mimimi", "lacração", "lacracao", "lacrador",
        "agenda", "pauta identitária", "politicamente correto", "ideologia",
        "doutrinação", "doutrinacao",
    ],
    "marcadores_diversidade": [
        "diversidade", "inclusão", "inclusao", "representatividade",
        "diversidade forçada", "diversidade forcada", "cota", "minoria",
        "lgbt", "lgbtqia", "feminismo", "feminista", "empoderada", "empoderamento",
    ],
    "marcadores_antagonismo": [
        "esquerda", "esquerdista", "comunista", "comunismo", "petista",
        "direita", "conservador", "patriota", "globalista",
    ],
    "marcadores_hostilidade": [
        "lixo", "vergonha", "ridículo", "ridiculo", "patético", "patetico",
        "destruindo", "acabou", "morto", "boicote", "cancelar", "cancelamento",
    ],
}


# ============================================================
# CARREGAMENTO
# ============================================================

def carregar_base(videos_path, comments_path):
    """Carrega vídeos e comentários, devolvendo (df_videos, df_comments)."""
    df_v = pd.read_parquet(videos_path) if _existe(videos_path) else pd.DataFrame()
    df_c = pd.read_parquet(comments_path) if _existe(comments_path) else pd.DataFrame()
    return df_v, df_c


def _existe(path):
    import os
    return bool(path) and os.path.exists(path)


def normalizar(texto):
    """Minúsculas + remoção de acentos."""
    texto = str(texto).lower()
    texto = unicodedata.normalize("NFD", texto)
    return texto.encode("ascii", "ignore").decode("utf-8")


def _tokenizar(texto, extra_stop=None):
    stop = STOPWORDS_PT | (set(extra_stop) if extra_stop else set())
    tokens = TOKEN_RE.findall(str(texto).lower())
    return [t for t in tokens if t not in stop and normalizar(t) not in stop]


def _contar_termo(texto_norm, termo_norm):
    """
    Conta ocorrências de um termo respeitando fronteira de palavra.
    Evita falsos positivos (ex.: 'dei' não casa dentro de 'deixa').
    Para termos multipalavra, usa a expressão completa com fronteiras.
    """
    if not termo_norm:
        return 0
    padrao = r"\b" + re.escape(termo_norm) + r"\b"
    return len(re.findall(padrao, texto_norm))


# ============================================================
# 1. PÚBLICO-ALVO / NÚCLEO DA COMUNIDADE
# ============================================================

def top_comentaristas(df_c, n=25):
    """
    Autores mais ativos — proxy do núcleo engajado de cada comunidade.
    Retorna DataFrame com nº de comentários, canais distintos e curtidas totais.
    """
    if df_c.empty or "author" not in df_c.columns:
        return pd.DataFrame()

    df = df_c.copy()
    df["author"] = df["author"].fillna("(desconhecido)")

    canais_col = "canal_origem" if "canal_origem" in df.columns else None
    likes_col = "like_count" if "like_count" in df.columns else None

    agg = {"comentarios": ("author", "size")}
    grp = df.groupby("author")
    out = grp.size().rename("comentarios").to_frame()

    if canais_col:
        out["canais_distintos"] = grp[canais_col].nunique()
    if likes_col:
        out["curtidas_totais"] = grp[likes_col].sum()

    out = out.reset_index().sort_values("comentarios", ascending=False).head(n)
    return out


def perfil_engajamento(df_v, df_c):
    """Métricas agregadas de público por categoria PIBIC."""
    if df_c.empty:
        return pd.DataFrame()

    cat_col = "categoria_pibic" if "categoria_pibic" in df_c.columns else None
    if not cat_col:
        return pd.DataFrame()

    df = df_c.copy()
    likes = df["like_count"] if "like_count" in df.columns else 0
    df["_likes"] = likes

    resumo = (
        df.groupby(cat_col)
        .agg(
            comentarios=("texto", "size"),
            autores_unicos=("author", "nunique"),
            curtidas_medias=("_likes", "mean"),
        )
        .reset_index()
    )
    resumo["comentarios_por_autor"] = (
        resumo["comentarios"] / resumo["autores_unicos"].replace(0, 1)
    ).round(2)
    return resumo


# ============================================================
# 2. MAPEAMENTO DE COMUNIDADES (rede de co-comentários)
# ============================================================

def rede_canais(df_c, min_autores_ponte=1):
    """
    Constrói a rede de comunidades a partir de autores que comentam em
    múltiplos canais (pontes entre comunidades).

    Retorna dict com:
        - bridges:  DataFrame de autores que cruzam ≥2 canais
        - edges:    DataFrame canal-canal (peso = autores compartilhados)
        - grafo:    networkx.Graph dos canais
        - metricas: DataFrame de centralidade por canal
    """
    if df_c.empty or "author" not in df_c.columns or "canal_origem" not in df_c.columns:
        return {"bridges": pd.DataFrame(), "edges": pd.DataFrame(),
                "grafo": nx.Graph(), "metricas": pd.DataFrame()}

    df = df_c.copy()
    df["author"] = df["author"].fillna("")
    df = df[df["author"].str.strip() != ""]
    df = df[df["author"] != "(desconhecido)"]

    # autor -> conjunto de canais
    autor_canais = df.groupby("author")["canal_origem"].apply(lambda s: set(s))

    # Pontes: autores em ≥2 canais
    bridges = autor_canais[autor_canais.apply(len) >= 2]
    df_bridges = (
        bridges.apply(lambda s: ", ".join(sorted(s)))
        .rename("canais")
        .reset_index()
    )
    if not df_bridges.empty:
        df_bridges["n_canais"] = bridges.apply(len).values
        df_bridges = df_bridges.sort_values("n_canais", ascending=False)

    # Arestas canal-canal: nº de autores compartilhados
    pair_counter = Counter()
    for canais in bridges:
        canais = sorted(canais)
        for i in range(len(canais)):
            for j in range(i + 1, len(canais)):
                pair_counter[(canais[i], canais[j])] += 1

    edges = [
        {"canal_a": a, "canal_b": b, "autores_compartilhados": w}
        for (a, b), w in pair_counter.items()
        if w >= min_autores_ponte
    ]
    df_edges = pd.DataFrame(edges).sort_values(
        "autores_compartilhados", ascending=False
    ) if edges else pd.DataFrame()

    # Grafo + centralidade
    G = nx.Graph()
    todos_canais = df["canal_origem"].dropna().unique()
    G.add_nodes_from(todos_canais)
    for e in edges:
        G.add_edge(e["canal_a"], e["canal_b"], weight=e["autores_compartilhados"])

    metricas = pd.DataFrame()
    if G.number_of_nodes() > 0:
        grau = dict(G.degree())
        try:
            centr = nx.degree_centrality(G)
        except Exception:
            centr = {n: 0 for n in G.nodes()}
        metricas = pd.DataFrame({
            "canal": list(grau.keys()),
            "conexoes": list(grau.values()),
            "centralidade": [round(centr.get(n, 0), 3) for n in grau.keys()],
        }).sort_values("conexoes", ascending=False)

    return {"bridges": df_bridges, "edges": df_edges, "grafo": G, "metricas": metricas}


def grafo_para_plotly(G):
    """
    Converte um networkx.Graph em figura Plotly (layout spring).
    Retorna go.Figure ou None se o grafo estiver vazio/sem arestas.
    """
    import plotly.graph_objects as go

    if G.number_of_nodes() == 0:
        return None

    pos = nx.spring_layout(G, seed=42, k=0.8)

    edge_x, edge_y = [], []
    for a, b in G.edges():
        x0, y0 = pos[a]
        x1, y1 = pos[b]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y, mode="lines",
        line=dict(width=1, color="#888"), hoverinfo="none",
    )

    node_x, node_y, texto, tamanho = [], [], [], []
    for n in G.nodes():
        x, y = pos[n]
        node_x.append(x)
        node_y.append(y)
        nome = str(n).split("/")[-1] or str(n)
        grau = G.degree(n)
        texto.append(f"{nome} ({grau} conexões)")
        tamanho.append(10 + grau * 6)

    node_trace = go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        text=[t.split(" (")[0] for t in texto],
        textposition="top center",
        hovertext=texto, hoverinfo="text",
        marker=dict(size=tamanho, color="#FF4B4B", line=dict(width=1, color="#fff")),
    )

    fig = go.Figure(data=[edge_trace, node_trace])
    fig.update_layout(
        showlegend=False, hovermode="closest",
        margin=dict(b=10, l=10, r=10, t=30),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        title="Rede de comunidades (canais ligados por público compartilhado)",
    )
    return fig


# ============================================================
# 3. REPERTÓRIOS LINGUÍSTICOS
# ============================================================

def frequencia_termos(df_c, top_n=40, extra_stop=None):
    """Top termos nos comentários (após remoção de stopwords)."""
    if df_c.empty or "texto" not in df_c.columns:
        return pd.DataFrame()

    contador = Counter()
    for texto in df_c["texto"].dropna():
        contador.update(_tokenizar(texto, extra_stop))

    return pd.DataFrame(
        contador.most_common(top_n), columns=["termo", "frequencia"]
    )


def ngramas(df_c, n=2, top_n=30, extra_stop=None):
    """Top n-gramas (bigramas por padrão) — capta expressões recorrentes."""
    if df_c.empty or "texto" not in df_c.columns:
        return pd.DataFrame()

    contador = Counter()
    for texto in df_c["texto"].dropna():
        tokens = _tokenizar(texto, extra_stop)
        for i in range(len(tokens) - n + 1):
            contador[" ".join(tokens[i:i + n])] += 1

    label = {2: "bigrama", 3: "trigrama"}.get(n, f"{n}-grama")
    return pd.DataFrame(
        contador.most_common(top_n), columns=[label, "frequencia"]
    )


# ============================================================
# 4. CONCORDÂNCIA KWIC (Keyword In Context) — análise de discurso
# ============================================================

def concordancia_kwic(df_c, termo, janela=8, max_resultados=100):
    """
    Localiza um termo e devolve seu contexto (palavras antes/depois).
    Espelha a ferramenta de concordância usada em análise de discurso/MAXQDA.

    Retorna DataFrame: contexto_esquerda | TERMO | contexto_direita | autor | canal
    """
    if df_c.empty or "texto" not in df_c.columns or not termo:
        return pd.DataFrame()

    termo_norm = normalizar(termo).strip()
    resultados = []

    for _, row in df_c.iterrows():
        texto = str(row.get("texto", ""))
        palavras = texto.split()
        palavras_norm = [normalizar(p) for p in palavras]

        for i, pn in enumerate(palavras_norm):
            if termo_norm in pn:
                ini = max(0, i - janela)
                fim = min(len(palavras), i + janela + 1)
                resultados.append({
                    "contexto_esquerda": " ".join(palavras[ini:i]),
                    "termo": palavras[i],
                    "contexto_direita": " ".join(palavras[i + 1:fim]),
                    "autor": row.get("author", ""),
                    "canal": row.get("canal_origem", ""),
                    "categoria": row.get("categoria_pibic", ""),
                })
                if len(resultados) >= max_resultados:
                    return pd.DataFrame(resultados)

    return pd.DataFrame(resultados)


# ============================================================
# 5. LÉXICO DE MARCADORES (scoring exploratório)
# ============================================================

def score_lexico(df_c, lexico=None):
    """
    Conta ocorrências dos marcadores do léxico nos comentários, por eixo.
    Retorna dict:
        - por_comentario: df_c com colunas de score por eixo
        - resumo_eixos:   total de ocorrências por eixo
        - resumo_termos:  ocorrências por termo individual
        - por_categoria:  ocorrências por eixo × categoria_pibic
    """
    lexico = lexico or LEXICO_DISCURSO_PADRAO

    if df_c.empty or "texto" not in df_c.columns:
        return {"por_comentario": pd.DataFrame(), "resumo_eixos": pd.DataFrame(),
                "resumo_termos": pd.DataFrame(), "por_categoria": pd.DataFrame()}

    df = df_c.copy()
    textos_norm = df["texto"].fillna("").apply(normalizar)

    termo_counter = Counter()
    for eixo, termos in lexico.items():
        col = f"score_{eixo}"
        termos_norm = [(t, normalizar(t)) for t in termos]
        scores = []
        for tn in textos_norm:
            s = 0
            for termo, termo_norm in termos_norm:
                c = _contar_termo(tn, termo_norm)
                s += c
                termo_counter[termo] += c
            scores.append(s)
        df[col] = scores

    score_cols = [f"score_{e}" for e in lexico.keys()]
    df["score_total"] = df[score_cols].sum(axis=1)

    resumo_eixos = (
        df[score_cols].sum().rename("ocorrencias")
        .rename_axis("eixo").reset_index()
        .sort_values("ocorrencias", ascending=False)
    )

    resumo_termos = (
        pd.DataFrame(termo_counter.most_common(), columns=["termo", "ocorrencias"])
        .query("ocorrencias > 0")
    )

    por_categoria = pd.DataFrame()
    if "categoria_pibic" in df.columns:
        por_categoria = (
            df.groupby("categoria_pibic")[score_cols + ["score_total"]]
            .sum().reset_index()
        )

    return {"por_comentario": df, "resumo_eixos": resumo_eixos,
            "resumo_termos": resumo_termos, "por_categoria": por_categoria}


def comentarios_mais_marcados(df_scored, top_n=30):
    """Comentários com maior score_total — candidatos a leitura qualitativa."""
    if df_scored.empty or "score_total" not in df_scored.columns:
        return pd.DataFrame()
    cols = [c for c in ["categoria_pibic", "canal_origem", "author", "texto",
                        "like_count", "score_total"] if c in df_scored.columns]
    return (
        df_scored[df_scored["score_total"] > 0]
        .sort_values("score_total", ascending=False)[cols]
        .head(top_n)
    )


# ============================================================
# 6. ATORES CITADOS (NER sobre comentários)
# ============================================================

def entidades_comentarios(df_c, limite=500):
    """
    Extrai pessoas/organizações citadas nos comentários (via ner_pipeline).
    Limita a `limite` comentários por desempenho. Retorna dict de Counters.
    """
    if df_c.empty or "texto" not in df_c.columns:
        return {"PER": pd.DataFrame(), "ORG": pd.DataFrame(), "LOC": pd.DataFrame()}

    try:
        from ner_pipeline import extrair_entidades
    except Exception:
        return {"PER": pd.DataFrame(), "ORG": pd.DataFrame(), "LOC": pd.DataFrame()}

    # Prioriza comentários mais curtidos (mais relevantes para o debate)
    df = df_c.copy()
    if "like_count" in df.columns:
        df = df.sort_values("like_count", ascending=False)
    df = df.head(limite)

    cont = {"PER": Counter(), "ORG": Counter(), "LOC": Counter()}
    for texto in df["texto"].dropna():
        ents = extrair_entidades(texto)
        for tipo, lista in ents.items():
            chave = "PER" if tipo in ("PER", "PERSON") else tipo
            if chave in cont:
                cont[chave].update(lista)

    saida = {}
    for tipo, counter in cont.items():
        saida[tipo] = pd.DataFrame(
            counter.most_common(20), columns=[tipo.lower(), "frequencia"]
        )
    return saida
