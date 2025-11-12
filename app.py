import streamlit as st
import pandas as pd
import time
import praw # Para Reddit
import tweepy # Para X/Twitter

# --- 1. Credenciais e Configurações ---

st.set_page_config(
    page_title="Game Community Data Extractor",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎮 Extrator de Dados da Comunidade de Jogos")
st.markdown("Use o painel lateral para configurar sua busca de palavras-chave e inserir suas credenciais de API.")
st.markdown("---")

# --- Sidebar: Configurações de Busca ---

st.sidebar.header("⚙️ Configurações de Busca")

# Campo para as palavras-chave
keywords_input = st.sidebar.text_input(
    "Insira as Palavras-Chave (separadas por vírgula)",
    "Elden Ring, WoW, Final Fantasy"
)

# Seleção das fontes
st.sidebar.subheader("🌐 Fontes de Dados")
use_reddit = st.sidebar.checkbox("Reddit", value=True)
use_x = st.sidebar.checkbox("X (Twitter)", value=True)
use_youtube = st.sidebar.checkbox("YouTube", value=True)
use_twitch = st.sidebar.checkbox("Twitch", value=True)


# --- Sidebar: Credenciais do Reddit ---
st.sidebar.subheader("🔒 Credenciais do Reddit (PRAW)")
CLIENT_ID = st.sidebar.text_input("Reddit Client ID", type="password")
CLIENT_SECRET = st.sidebar.text_input("Reddit Client Secret", type="password")
USER_AGENT = "StreamlitApp_v1"

# --- Sidebar: Credenciais do X/Twitter ---
st.sidebar.subheader("🐦 Credenciais do X/Twitter (Tweepy)")
BEARER_TOKEN = st.sidebar.text_input("X/Twitter Bearer Token", type="password")
# O Tweepy usará este token para fazer as chamadas da API v2


# Botão de busca
search_button = st.sidebar.button("🔍 Iniciar Busca")


# --- 2. Funções de Busca (Reddit - REAL) ---

def get_reddit_instance():
    """Tenta criar uma instância do PRAW com as credenciais."""
    if not CLIENT_ID or not CLIENT_SECRET:
        return None
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )
        return reddit
    except Exception as e:
        st.error(f"Erro ao inicializar o PRAW. Verifique suas credenciais: {e}")
        return None

def fetch_reddit_data(keywords, limit=10):
    """Busca dados no Reddit usando PRAW."""
    reddit = get_reddit_instance()
    
    if reddit is None:
        return pd.DataFrame() # Retorna vazio se faltarem credenciais

    query = " OR ".join(keywords) # Ex: "Elden Ring OR WoW"
    st.info(f"⚙️ Buscando no Reddit por: **{query}**")
    
    data = []
    try:
        # Busca no subreddit de jogos mais popular e nos posts em geral (r/all)
        for submission in reddit.subreddit('JogosBrasil').search(query, limit=limit):
            data.append({
                'Fonte': 'Reddit',
                'Título': submission.title,
                'Subreddit': submission.subreddit.display_name,
                'Conteúdo (Selftext)': submission.selftext[:100] + '...' if submission.selftext else 'Link/Mídia',
                'Upvotes': submission.score,
                'Comentários': submission.num_comments,
                'Link': f"https://www.reddit.com{submission.permalink}",
                'Data': pd.to_datetime(submission.created_utc, unit='s')
            })
    except Exception as e:
        st.error(f"Erro na busca do Reddit: {e}")
        return pd.DataFrame()

    return pd.DataFrame(data)


# --- 3. Funções de Busca (X/Twitter - REAL) ---

def get_x_client():
    """Tenta criar um cliente Tweepy com o Bearer Token."""
    if not BEARER_TOKEN:
        return None
    try:
        client = tweepy.Client(BEARER_TOKEN)
        return client
    except Exception as e:
        st.error(f"Erro ao inicializar o Tweepy. Verifique seu Bearer Token: {e}")
        return None

def fetch_x_data(keywords, limit=10): # <-- Função ATUALIZADA
    """Busca dados no X/Twitter usando Tweepy (API v2)."""
    client = get_x_client()

    if client is None:
        st.error("Por favor, insira o Bearer Token do X/Twitter na sidebar.")
        return pd.DataFrame()

    # Formata a query: Ex: "Elden Ring OR WoW lang:pt -is:retweet"
    query = " OR ".join(keywords) + " -is:retweet" # Exclui retweets para melhor qualidade
    st.info(f"⚙️ Buscando no X/Twitter por: **{query}**")
    
    data = []
    try:
        # Faz a busca recente de tweets
        response = client.search_recent_tweets(
            query=query, 
            tweet_fields=["created_at", "public_metrics"], 
            max_results=limit
        )

        if response.data:
            for tweet in response.data:
                metrics = tweet.public_metrics
                data.append({
                    'Fonte': 'X/Twitter',
                    'Tweet': tweet.text,
                    'Curtidas': metrics['like_count'],
                    'Retweets': metrics['retweet_count'],
                    'Comentários': metrics['reply_count'],
                    'Data': tweet.created_at,
                    # O link completo é mais complexo na API v2, mas isso é um bom placeholder
                    'Link': f"https://twitter.com/i/web/status/{tweet.id}" 
                })
        else:
            st.warning(f"Nenhum tweet encontrado para: {query}")
    
    except Exception as e:
        st.error(f"Erro na busca do X/Twitter. Verifique a query ou o token: {e}")
        return pd.DataFrame()

    return pd.DataFrame(data)


# --- 4. Funções de Busca (YouTube e Twitch - MOCKUP) ---

@st.cache_data
def fetch_youtube_data(keywords, limit=5):
    """MOCKUP: Simula a busca de dados no YouTube."""
    st.info(f"⚙️ Buscando no YouTube por: **{keywords}** (MOCKUP)")
    time.sleep(1)
    data = []
    for i in range(limit):
        data.append({
            'Fonte': 'YouTube',
            'Canal': f"CanalGaming_{i+1}",
            'Vídeo': f"Gameplay de {keywords[0]} - Novo Lançamento",
            'Visualizações': 5000 + i*100,
            'Link': 'URL_YouTube',
            'Data': pd.Timestamp.now() - pd.Timedelta(weeks=i)
        })
    return pd.DataFrame(data)

@st.cache_data
def fetch_twitch_data(keywords, limit=5):
    """MOCKUP: Simula a busca de dados na Twitch."""
    st.info(f"⚙️ Buscando na Twitch por: **{keywords}** (MOCKUP)")
    time.sleep(1)
    data = []
    for i in range(limit):
        data.append({
            'Fonte': 'Twitch',
            'Streamer': f"StreamerPro_{i+1}",
            'Título': f"Live: Jogando {keywords[0]}",
            'Visualizadores': 800 + i*50,
            'Link': 'URL_Twitch',
            'Data': 'Ao Vivo' if i == 0 else 'Offline'
        })
    return pd.DataFrame(data)


# --- 5. Lógica de Busca Principal ---

if search_button:
    # 1. Processar as palavras-chave
    keywords = [k.strip() for k in keywords_input.split(',') if k.strip()]
    
    if not keywords:
        st.error("Por favor, insira pelo menos uma palavra-chave.")
    else:
        st.header(f"Resultados para: **{', '.join(keywords)}**")
        all_results = []
        
        # 2. Executar as buscas
        with st.spinner("Buscando dados nas plataformas..."):
            
            if use_reddit:
                df_reddit = fetch_reddit_data(keywords)
                if not df_reddit.empty:
                    all_results.append(df_reddit)
            
            if use_x:
                df_x = fetch_x_data(keywords)
                if not df_x.empty:
                    all_results.append(df_x)

            if use_youtube:
                df_youtube = fetch_youtube_data(keywords)
                if not df_youtube.empty:
                    all_results.append(df_youtube)

            if use_twitch:
                df_twitch = fetch_twitch_data(keywords)
                if not df_twitch.empty:
                    all_results.append(df_twitch)

        # 3. Exibir e Baixar resultados
        if all_results:
            st.success("Busca concluída!")
            
            df_final = pd.concat(all_results, ignore_index=True)
            
            st.header("✨ Visualização Completa dos Dados")
            st.dataframe(df_final, use_container_width=True)
            
            st.subheader("📊 Resumo por Fonte")
            source_summary = df_final['Fonte'].value_counts().rename_axis('Fonte').to_frame('Total de Resultados')
            st.dataframe(source_summary)

            st.download_button(
                label="📥 Baixar Dados (CSV)",
                data=df_final.to_csv(index=False).encode('utf-8'),
                file_name=f'dados_jogos_{time.strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv',
            )
        else:
            st.warning("Nenhum dado encontrado. Verifique suas credenciais e as palavras-chave.")