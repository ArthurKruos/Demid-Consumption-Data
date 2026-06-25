# DEMID — Plataforma de Análise de Comunidades Gamer e Políticas Digitais

**Projeto de Iniciação Científica — Universidade Federal da Paraíba (UFPB)**

O DEMID é uma plataforma de pesquisa que combina coleta de dados de redes sociais com análise de documentos oficiais para:

- Mapear comunidades gamers em plataformas digitais (Reddit, YouTube, Twitter/X, Telegram)
- Monitorar políticas públicas sobre jogos digitais via Diário Oficial da Paraíba
- Identificar padrões discursivos em comunidades extremistas no contexto gaming em língua portuguesa

---

## Arquitetura

```
Demid-Consumption-Data/
├── app.py                        # Dashboard principal (Streamlit)
├── analise_games_diario.py       # Análise e scoring de documentos oficiais
├── ner_pipeline.py               # Pipeline de NER em português (SpaCy)
├── getdata/
│   ├── reddit_collector.py       # Coleta via PRAW (Reddit API)
│   ├── youtube_collector.py      # Coleta via Google API (YouTube)
│   ├── twitter_collector.py      # Coleta via Tweepy v2 (Twitter/X)
│   └── telegram_collector.py     # Coleta via Telethon (Telegram público)
├── data/                         # Dados coletados (.parquet)
├── logs/                         # Logs de coleta
└── .streamlit/                   # Configuração do tema e assets
```

---

## Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Dashboard | Streamlit |
| NLP / NER | SpaCy `pt_core_news_lg` |
| Coleta Reddit | PRAW |
| Coleta YouTube | google-api-python-client |
| Coleta Twitter/X | Tweepy v2 |
| Coleta Telegram | Telethon |
| Armazenamento | Parquet (PyArrow) |
| Visualizações | Plotly |
| ML | scikit-learn |

---

## Instalação

### Pré-requisitos
- Python 3.11+
- Credenciais de API (ver seção abaixo)

### Setup

```bash
# 1. Clone o repositório
git clone https://github.com/ArthurKruos/Demid-Consumption-Data.git
cd Demid-Consumption-Data

# 2. Instale as dependências
pip install -r requirements.txt

# 3. Baixe o modelo de NLP em português
python -m spacy download pt_core_news_lg

# 4. Execute a aplicação
streamlit run app.py
```

A aplicação abrirá em `http://localhost:8501`.

---

## Pré-requisitos por fonte de dados

| Fonte | Credencial necessária | Custo | Limite | Nível de esforço |
|-------|-----------------------|-------|--------|------------------|
| **YouTube (yt-dlp)** | ❌ Nenhuma | Grátis | Sem cota | ✅ Zero configuração |
| **Reddit** | `client_id` + `client_secret` | Grátis | 60 req/min | ✅ Fácil |
| **Telegram** | `api_id` + `api_hash` | Grátis | Sem cota prática | ✅ Fácil |
| **YouTube API v3** | `api_key` (fallback) | Grátis | 10.000 unidades/dia | ⚠️ Cota baixa |
| **Twitter/X** | `bearer_token` | Grátis (10 tweets/req) | Muito restrito | ⚠️ Limitado |

---

### YouTube — yt-dlp (recomendado, sem API key)
Não requer nenhuma credencial. Já está incluído no `requirements.txt`.
```bash
pip install yt-dlp
```
A API Key do YouTube v3 é aceita como **fallback** opcional.

### Reddit
1. Acesse [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
2. Crie um app do tipo **script**
3. Copie o `client_id` e `client_secret`
> Plano gratuito: 60 req/min — suficiente para pesquisa

### Twitter/X ⚠️
1. Acesse [developer.twitter.com](https://developer.twitter.com/)
2. Crie um projeto e app
3. Copie o **Bearer Token**
> Plano gratuito: máximo **10 tweets por requisição**. Para mais volume, o plano Basic custa U$100/mês.

### Telegram
1. Acesse [my.telegram.org](https://my.telegram.org/)
2. Em **API development tools**, crie um app
3. Copie o `api_id` e `api_hash`
4. Na primeira execução, o Telethon pedirá seu número de telefone para autenticar (sessão salva em `data/sessions/`)
5. O coletor acessa apenas canais e grupos **públicos**

### YouTube Data API v3 (opcional / fallback)
1. Acesse o [Google Cloud Console](https://console.cloud.google.com/)
2. Ative a **YouTube Data API v3**
3. Crie uma chave de API
> Limite: 10.000 unidades/dia. Cada busca custa 100 unidades (= 100 buscas/dia máximo).

---

## Módulos

### Dashboard de Redes Sociais
Busca por palavras-chave em Reddit, YouTube, Twitter/X e Telegram. Retorna posts, vídeos e mensagens com métricas de engajamento. Os resultados são exportáveis em Parquet.

### Painel do Diário Oficial
Carrega os arquivos `data/doe_raw.parquet` (coletados via `getdata/crawler_DU.py`) e exibe:
- Evolução mensal e anual de menções a jogos digitais
- Score de intensidade por documento
- Entidades extraídas via NER (órgãos, pessoas, locais)
- Rastreamento de termos customizados ao longo do tempo

### Crawler do Diário Oficial
Executado separadamente para coletar PDFs do Diário Oficial do Estado da Paraíba:
```bash
python getdata/crawler_DU.py
```
A coleta é incremental: na primeira execução baixa o ano inteiro; nas seguintes, busca apenas novos documentos.

---

## Dados Coletados

Os arquivos `.parquet` em `data/` não são versionados no Git (ver `.gitignore`). Para replicar a base:

1. Execute o crawler do Diário Oficial para gerar `doe_raw.parquet`
2. Use o dashboard para coletar dados das redes sociais

---

## Contexto de Pesquisa

Este projeto integra o grupo de pesquisa do Departamento de Computação da UFPB, com foco em:

- Análise computacional de políticas públicas para jogos digitais no Brasil
- Identificação de comunidades e discursos extremistas em contextos gaming lusófonos
- Extração de conhecimento de fontes heterogêneas (mídias sociais + documentos oficiais)

---

## Contribuidores

- Arthur Silva França
- Júlia JGM B

---

## Licença

Uso acadêmico — Projeto de Iniciação Científica UFPB.
