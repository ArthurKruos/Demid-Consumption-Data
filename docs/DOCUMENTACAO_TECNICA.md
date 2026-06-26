# Documentação Técnica — Plataforma DEMID

*Para desenvolvedores: arquitetura, fluxo de dados, módulos, esquema de dados e como estender.*

---

## 1. Visão geral da arquitetura

A plataforma é uma aplicação **Streamlit** (Python) organizada em duas camadas desacopladas:

```
┌─────────────────────────────────────────────────────────────┐
│                         app.py (UI)                          │
│   menu lateral → 4 módulos → renderizam painéis/abas         │
└───────────────┬──────────────────────────┬──────────────────┘
                │                          │
   CAMADA DE COLETA               CAMADA DE ANÁLISE
   (getdata/*.py)                 (analise_youtube.py,
        │                          analise_games_diario.py,
        ▼                          ner_pipeline.py)
   APIs / yt-dlp / PRAW                    │
        │                                  ▼
        ▼                         lê parquet → calcula
   data/*.parquet  ◄───── persiste ───── (não coleta nada novo)
```

**Princípio central:** coleta e análise são **etapas independentes**. A análise opera exclusivamente sobre os arquivos `.parquet` já persistidos. Nada é baixado da rede durante a análise.

---

## 2. Stack

| Camada | Tecnologia |
|--------|-----------|
| UI | Streamlit |
| Coleta YouTube | **yt-dlp** (sem API key, sem cota) — fallback opcional para YouTube Data API v3 |
| Coleta Reddit | PRAW |
| Coleta Twitter/X | Tweepy v2 |
| Coleta Telegram | Telethon |
| NLP / NER | spaCy `pt_core_news_lg` |
| Rede de comunidades | NetworkX |
| Visualização | Plotly |
| Armazenamento | Parquet (PyArrow) |
| Detecção de idioma | langdetect |

Dependências em `requirements.txt`. Ambiente testado em **Python 3.11** (venv em `.venv/`).

---

## 3. Estrutura de arquivos

```
Demid-Consumption-Data/
├── app.py                      # UI principal; roteia os 4 módulos
├── painel_analise.py           # Painel reutilizável (6 abas) p/ YouTube e Reddit
├── analise_youtube.py          # Camada de análise (comunidades + discurso)
├── analise_games_diario.py     # Scoring de games no Diário Oficial
├── ner_pipeline.py             # NER em português (spaCy)
├── config/
│   ├── canais_pibic.py         # Lista curada de canais YouTube + vídeos avulsos
│   └── subreddits_pibic.py     # Lista-semente de subreddits
├── getdata/
│   ├── youtube_collector.py    # Coleta yt-dlp (keyword + canal) + export MAXQDA
│   ├── reddit_collector.py     # PRAW (keyword + subreddit PIBIC)
│   ├── twitter_collector.py    # Tweepy v2
│   ├── telegram_collector.py   # Telethon (canais públicos)
│   └── crawler_DU.py           # Crawler do Diário Oficial (PDF)
├── data/                       # Parquets de dados (não versionar)
│   ├── pibic_videos.parquet
│   ├── pibic_comments.parquet
│   ├── reddit_pibic_posts.parquet
│   ├── reddit_pibic_comments.parquet
│   └── doe_raw.parquet / doe_ner.parquet
├── docs/                       # Esta documentação
├── requirements.txt
└── run.sh                      # Atalho de inicialização
```

---

## 4. Módulos da UI (`app.py`)

O menu lateral (`st.sidebar.radio`) roteia 4 ramos:

| Rótulo no menu | Bloco em `app.py` | Coletor | Painel |
|----------------|-------------------|---------|--------|
| Mapeamento YouTube | `if modo_app == "Mapeamento YouTube"` | `fetch_pibic_data` | `painel.render(..., LABELS_YOUTUBE)` |
| Mapeamento Reddit (opcional) | `elif ... "Mapeamento Reddit"` | `fetch_reddit_pibic` | `painel.render(..., LABELS_REDDIT)` |
| Redes Sociais (coleta) | `if modo_app == "Redes Sociais (coleta)"` | `fetch_*_data` por keyword | tabela simples |
| Diário Oficial | `else` | `crawler_DU` (offline) | painel próprio |

Os módulos YouTube e Reddit **compartilham o mesmo painel de análise** (`painel_analise.render`), apenas trocando a terminologia via dicionários `LABELS_YOUTUBE` / `LABELS_REDDIT`.

---

## 5. Camada de coleta

### 5.1 YouTube (`getdata/youtube_collector.py`)

Coletor principal via **yt-dlp** (sem credenciais, sem cota). Funções-chave:

- `collect_ytdlp(query, max_results, get_comments, max_comments)` — busca por palavra-chave (`ytsearchN:`).
- `collect_channel(channel_url, categoria, max_videos, get_comments, max_comments, filter_pt)` — **coleta por canal** em 2 passos:
  1. extração *flat* da aba `/videos` → lista de IDs (rápido);
  2. extração completa de cada vídeo (metadados + comentários).
  `filter_pt=False` por padrão nos canais curados (analisados na íntegra, incluindo canais em inglês).
- `fetch_pibic_data(canais, max_videos, get_comments, max_comments, videos_avulsos, progress_callback)` — orquestra a lista de canais, persiste em `pibic_videos.parquet` / `pibic_comments.parquet` (append incremental com dedup por `video_id`/`comment_id`) e retorna `{"videos": df, "comments": df}`.
- `exportar_maxqda(df_videos, df_comments)` — consolida em um DataFrame "1 linha = 1 documento" para o MAXQDA.
- `collect_official_api(...)` — **fallback** via YouTube Data API v3 (usado só se yt-dlp indisponível; cota 10.000 un./dia).

### 5.2 Reddit (`getdata/reddit_collector.py`)

- `fetch_reddit_data(...)` — busca por keyword em `r/all` (uso geral).
- `collect_subreddit_pibic(client, subreddit_name, categoria, limit, ordenacao, get_comments, max_comments)` — coleta posts+comentários de um subreddit **capturando o autor** (essencial para a rede).
- `fetch_reddit_pibic(...)` — orquestra subreddits, **normaliza para o mesmo esquema do YouTube** (`canal_origem` = subreddit, `like_count` = score, posts mapeados como "vídeos"), persiste em `reddit_pibic_*.parquet`. Retorna `{"videos", "comments", "erro"}`.

> A normalização de esquema é o que permite reaproveitar `analise_youtube.py` e `painel_analise.py` sem alteração.

### 5.3 Outros coletores
- `twitter_collector.py` — Tweepy v2 (`fetch_twitter_data`); plano free limita a 10 tweets/req.
- `telegram_collector.py` — Telethon (`fetch_telegram_data`), canais públicos.
- `crawler_DU.py` — crawler incremental de PDFs do Diário Oficial da PB.

---

## 6. Esquema de dados (parquet)

### `pibic_videos.parquet` / `reddit_pibic_posts.parquet`
| Coluna | Descrição |
|--------|-----------|
| `video_id` | ID do vídeo (YouTube) ou post (Reddit) |
| `channel` | Nome do canal / subreddit |
| `title`, `description` | Texto |
| `tags`, `duration_sec` | Metadados (YouTube) |
| `published_at` | Data de publicação |
| `view_count`, `like_count`, `comment_count` | Engajamento (`view_count`=0 no Reddit) |
| `url`, `collected_at`, `collector` | Procedência |
| `categoria_pibic` | Rótulo de categoria (organizacional) |
| `canal_origem` | Chave de comunidade usada nas análises |

### `pibic_comments.parquet` / `reddit_pibic_comments.parquet`
| Coluna | Descrição |
|--------|-----------|
| `comment_id`, `video_id`, `parent_id` | Identificadores / estrutura de resposta |
| `author` | **Autor** (base da rede de comunidades) |
| `texto` | Conteúdo do comentário |
| `like_count` | Curtidas / score |
| `published_at`, `collected_at` | Datas |
| `categoria_pibic`, `canal_origem` | Metadados de pesquisa |

---

## 7. Camada de análise (`analise_youtube.py`)

Todas as funções recebem `df_c` (comentários) e/ou `df_v` (vídeos) no esquema acima.

| Função | O que faz | Técnica |
|--------|-----------|---------|
| `top_comentaristas(df_c, n)` | Núcleo engajado | `groupby("author")` + contagem, canais distintos, curtidas |
| `perfil_engajamento(df_v, df_c)` | Métricas por categoria | agregação por `categoria_pibic` |
| `rede_canais(df_c)` | **Mapa de comunidades** | autor→conjunto de `canal_origem`; pontes = autores em ≥2 canais; arestas canal-canal ponderadas por autores compartilhados; `networkx.Graph` + centralidade de grau |
| `grafo_para_plotly(G)` | Visualização da rede | `spring_layout` → traços Plotly |
| `frequencia_termos(df_c, top_n, extra_stop)` | Top termos | tokenização regex + `STOPWORDS_PT` + `Counter` |
| `ngramas(df_c, n, top_n)` | Bigramas/trigramas | janelas de n tokens |
| `concordancia_kwic(df_c, termo, janela, max_resultados)` | **KWIC** | localiza token e recorta `±janela` palavras |
| `score_lexico(df_c, lexico)` | Marcadores | contagem por **fronteira de palavra** (`\bregex\b`) por eixo |
| `comentarios_mais_marcados(df_scored, top_n)` | Trechos p/ leitura | ordena por `score_total` |
| `entidades_comentarios(df_c, limite)` | Atores citados | reusa `ner_pipeline.extrair_entidades` (spaCy) |

### Notas de implementação
- **Stopwords** (`STOPWORDS_PT`): inclui PT **e** EN (canais internacionais como Asmongold/Tolarian) + ruído de comentários (kkk, lol…).
- **Contagem por fronteira de palavra** (`_contar_termo`): usa `\b...\b` para evitar falsos positivos (ex.: "dei" não casa dentro de "deixa"). Substituiu uma contagem por substring que inflava resultados.
- **Léxico** (`LEXICO_DISCURSO_PADRAO`): dict editável `{eixo: [termos]}`. É **exploratório, não classificatório** — apenas localiza ocorrências; a interpretação é do pesquisador.

---

## 8. Painel reutilizável (`painel_analise.py`)

`render(df_v, df_c, labels, prefixo_export)` desenha as **6 abas** (Visão geral, Público-alvo, Mapa de comunidades, Discurso, Marcadores, Exportar). A terminologia (canal/subreddit, vídeo/post) vem de `labels` (`LABELS_YOUTUBE` ou `LABELS_REDDIT`). Importa `exportar_maxqda` para a aba de exportação.

---

## 9. Como rodar

```bash
# Ambiente (Python 3.11 via uv recomendado)
uv venv .venv --python 3.11
uv pip install -r requirements.txt
.venv/bin/python -m spacy download pt_core_news_lg

# Iniciar
.venv/bin/streamlit run app.py     # ou ./run.sh
```
App em `http://localhost:8501`.

---

## 10. Como estender

**Adicionar um canal/subreddit:** use a UI ("Digitar meus canais/subreddits") ou edite `config/canais_pibic.py` / `config/subreddits_pibic.py`.

**Adicionar uma nova plataforma ao mapeamento:** escreva um coletor que produza o **esquema normalizado** (seção 6) e chame `painel.render(df_v, df_c, LABELS_X)`. Nenhuma mudança na análise é necessária.

**Editar os marcadores de discurso:** altere `LEXICO_DISCURSO_PADRAO` em `analise_youtube.py` (ou passe um `lexico` customizado a `score_lexico`).

**Adicionar uma nova análise:** crie a função em `analise_youtube.py` (recebendo `df_c`/`df_v`) e adicione uma aba em `painel_analise.render`.

---

## 11. Limitações conhecidas

- A análise reflete **apenas o que foi coletado**; canais não coletados não aparecem.
- yt-dlp pode falhar em canais específicos (ex.: sem aba `/videos` ou bloqueio temporário) — recoletar resolve na maioria dos casos.
- Detecção de idioma desligada nos canais curados (coleta integral, inclusive conteúdo em inglês).
- O léxico de marcadores é um **índice de busca**, não um classificador validado.

---

## 12. Considerações éticas (resumo)

Coleta de dados **públicos** para fins **acadêmicos não-comerciais**. Os rótulos de categoria são hipóteses de pesquisa, não classificações. Recomenda-se documentar na metodologia os parâmetros éticos (privacidade, não-desanonimização, respeito a conteúdo removido) — alinhados às políticas das plataformas.
