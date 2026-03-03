# ============================================================
# LENS DATA-HUB
# Análise Exploratória de Políticas Públicas sobre Games
# Unidade de análise: Diário Oficial (por edição)
# ============================================================

import pandas as pd
import unicodedata

# ============================================================
# 1. CONFIGURAÇÃO DE PALAVRAS-CHAVE
# ============================================================

palavras_games_core = [
    "jogo digital",
    "jogos digitais",
    "desenvolvimento de jogos",
    "desenvolvedora de jogos",
    "estudio de jogos",
    "industria de jogos",
    "industria de games",
    "producao de jogos",
    "criacao de jogos",
    "gamificacao",
    "game dev",
    "game development"
]

palavras_games_tecnicas = [
    "unity",
    "unreal",
    "engine de jogos",
    "motor grafico",
    "game design",
    "design de jogos",
    "programacao de jogos",
    "realidade virtual",
    "realidade aumentada",
    "xr",
    "esports",
    "e-sports"
]

palavras_seguranca_games = [
    "seguranca digital",
    "ciberseguranca",
    "protecao de dados",
    "crime digital",
    "jogos online",
    "controle parental",
    "plataformas digitais"
]

# ============================================================
# 2. NORMALIZAÇÃO
# ============================================================

def normalizar_texto(texto):
    texto = str(texto).lower()
    texto = unicodedata.normalize("NFD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto

# ============================================================
# 3. FUNÇÕES DE SCORE (COM NORMALIZAÇÃO)
# ============================================================

def score_games_avancado(texto):
    texto = normalizar_texto(texto)
    score = 0
    
    for p in palavras_games_core:
        score += texto.count(normalizar_texto(p)) * 3
        
    for p in palavras_games_tecnicas:
        score += texto.count(normalizar_texto(p)) * 2
        
    return score


def score_seguranca(texto):
    texto = normalizar_texto(texto)
    score = 0
    
    for p in palavras_seguranca_games:
        score += texto.count(normalizar_texto(p)) * 2
        
    return score


# ============================================================
# 4. FUNÇÃO PRINCIPAL
# ============================================================

def processar_parquet(caminho_parquet, salvar_csv=True):

    df = pd.read_parquet(caminho_parquet)
    
    df["data_doe"] = pd.to_datetime(df["data_doe"], errors="coerce")
    df["ano"] = df["data_doe"].dt.year
    df["mes"] = df["data_doe"].dt.month
    df["ano_mes"] = df["data_doe"].dt.to_period("M").astype(str)

    df["conteudo"] = df["conteudo"].fillna("")

    df["score_games"] = df["conteudo"].apply(score_games_avancado)
    df["score_seguranca"] = df["conteudo"].apply(score_seguranca)

    df["flag_games"] = df["score_games"] > 0
    df["flag_seguranca"] = df["score_seguranca"] > 0

    # ============================
    # Resumo Anual
    # ============================

    resumo_anual = (
        df.groupby("ano")
        .agg(
            diarios_com_games=("flag_games", "sum"),
            intensidade_games=("score_games", "sum"),
            diarios_com_seguranca=("flag_seguranca", "sum"),
            intensidade_seguranca=("score_seguranca", "sum")
        )
        .reset_index()
    )

    # ============================
    # Resumo Mensal
    # ============================

    resumo_mensal = (
        df.groupby("ano_mes")
        .agg(
            diarios_com_games=("flag_games", "sum"),
            intensidade_games=("score_games", "sum")
        )
        .reset_index()
    )

    # ============================
    # Top Diários
    # ============================

    top_diarios = (
        df.sort_values("score_games", ascending=False)
        [["data_doe", "nome_pdf", "score_games", "score_seguranca"]]
        .head(20)
    )

    if salvar_csv:
        resumo_anual.to_csv("resumo_anual_games.csv", index=False)
        resumo_mensal.to_csv("resumo_mensal_games.csv", index=False)
        top_diarios.to_csv("top_diarios_games.csv", index=False)

    return {
        "df_completo": df,
        "resumo_anual": resumo_anual,
        "resumo_mensal": resumo_mensal,
        "top_diarios": top_diarios
    }

# ============================================================
# 5. EVOLUÇÃO DE TERMOS (MENSAL)
# ============================================================

def evolucao_termos(lista_termos, df):

    df = df.copy()
    df['conteudo'] = df['conteudo'].fillna("")
    df['ano_mes'] = df['data_doe'].dt.to_period("M").astype(str)

    evolucao = []

    for periodo in sorted(df['ano_mes'].unique()):
        textos_periodo = " ".join(df[df['ano_mes'] == periodo]['conteudo'])
        textos_periodo = normalizar_texto(textos_periodo)

        linha = {"ano_mes": periodo}

        for termo in lista_termos:
            termo_norm = normalizar_texto(termo)
            linha[termo] = textos_periodo.count(termo_norm)

        evolucao.append(linha)

    return pd.DataFrame(evolucao)


# ============================================================
# 6. EVOLUÇÃO ANUAL DE TERMOS
# ============================================================

def evolucao_termos_anual(lista_termos, df):

    df = df.copy()
    df['conteudo'] = df['conteudo'].fillna("")

    evolucao = []

    for ano in sorted(df['ano'].unique()):
        textos_ano = " ".join(df[df['ano'] == ano]['conteudo'])
        textos_ano = normalizar_texto(textos_ano)

        linha = {"ano": ano}

        for termo in lista_termos:
            termo_norm = normalizar_texto(termo)
            linha[termo] = textos_ano.count(termo_norm)

        evolucao.append(linha)

    return pd.DataFrame(evolucao)

# ============================================================
# Uso no Streamlit:
#
# from analise_games_diario import processar_parquet, evolucao_termos
#
# resultados = processar_parquet("seu_arquivo.parquet", salvar_csv=False)
# df_evolucao = evolucao_termos(["jogos digitais", "unity"], resultados["df_completo"])
#
# ============================================================