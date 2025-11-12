import pandas as pd
import praw
import streamlit as st

def get_reddit_instance(client_id, client_secret, user_agent):
    """Tenta criar uma instância do PRAW com as credenciais."""
    if not client_id or not client_secret:
        return None
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        # Tenta uma chamada simples para verificar se as credenciais funcionam
        # (Apenas para garantir que a instância foi criada)
        return reddit
    except Exception as e:
        # Exibe o erro usando Streamlit no contexto onde a função é chamada
        st.error(f"Erro ao inicializar o PRAW. Verifique suas credenciais: {e}")
        return None

def fetch_reddit_data(client_id, client_secret, user_agent, keywords, limit=100):
    """Busca dados no Reddit usando PRAW, recebendo credenciais como argumentos."""
    
    # 1. Obter a instância do Reddit
    reddit = get_reddit_instance(client_id, client_secret, user_agent)
    
    if reddit is None:
        st.error("Credenciais do Reddit ausentes ou inválidas.")
        return pd.DataFrame()

    query = " OR ".join(keywords) # Ex: "Elden Ring OR WoW"
    st.info(f"⚙️ Buscando no Reddit por: **{query}**")
    
    data = []
    try:
        # Busca no subreddit de jogos brasileiro mais popular
        # Substituí 'JogosBrasil' por 'gamesEcultura' que é frequentemente citado como o maior.
        for submission in reddit.subreddit('gamesEcultura').search(query, limit=limit):
            # Ignora o post se for um objeto None por algum motivo
            if not submission:
                continue
                
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
        # Erro de busca (ex: limite de taxa, subreddit não existe, etc.)
        st.error(f"Erro na busca do Reddit: {e}")
        return pd.DataFrame()

    return pd.DataFrame(data)