import pandas as pd
import tweepy
import streamlit as st

def get_x_client(bearer_token):
    """Tenta criar um cliente Tweepy com o Bearer Token."""
    if not bearer_token:
        return None
    try:
        # A nova API do X/Twitter (v2) usa tweepy.Client
        client = tweepy.Client(bearer_token)
        return client
    except Exception as e:
        # Exibe o erro usando Streamlit no contexto onde a função é chamada
        st.error(f"Erro ao inicializar o Tweepy. Verifique seu Bearer Token: {e}")
        return None

def fetch_x_data(bearer_token, keywords, limit=10):
    """Busca dados no X/Twitter usando Tweepy (API v2), recebendo o token como argumento."""
    client = get_x_client(bearer_token)

    if client is None:
        return pd.DataFrame()

    # Formata a query: Ex: "Elden Ring OR WoW -is:retweet"
    # Exclui retweets para melhor qualidade.
    query = " OR ".join(keywords) + " -is:retweet" 
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
                    # O link completo é mais complexo na API v2
                    'Link': f"https://twitter.com/i/web/status/{tweet.id}" 
                })
        else:
            st.warning(f"Nenhum tweet encontrado para: {query}")
    
    except Exception as e:
        st.error(f"Erro na busca do X/Twitter. Verifique a query ou o token: {e}")
        return pd.DataFrame()

    return pd.DataFrame(data)