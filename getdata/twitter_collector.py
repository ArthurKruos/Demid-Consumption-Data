import os
import time
import logging
import pandas as pd
from datetime import datetime
import tweepy
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

DATA_DIR = "data"
LOG_DIR = "logs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

TWEETS_PATH = os.path.join(DATA_DIR, "twitter_tweets.parquet")

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "twitter_collector.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def is_portuguese(text):
    if not text or len(text) < 20:
        return False
    try:
        return detect(text) == "pt"
    except LangDetectException:
        return False


def append_parquet(path, df_new, id_column):
    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=id_column)
    else:
        df_combined = df_new
    df_combined.to_parquet(path, index=False)


def get_twitter_client(bearer_token):
    if not bearer_token:
        return None
    try:
        return tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    except Exception as e:
        logging.error(f"Erro ao criar cliente Tweepy: {e}")
        return None


def fetch_twitter_data(bearer_token, keywords, limit=10):
    """
    Busca tweets recentes por palavras-chave.
    Limite máximo no plano gratuito: 10 tweets por request.
    Retorna DataFrame compatível com o padrão do app.
    """
    client = get_twitter_client(bearer_token)

    if client is None:
        logging.error("Bearer Token ausente ou inválido.")
        return pd.DataFrame()

    if isinstance(keywords, str):
        keywords = [keywords]

    # Monta query com exclusão de retweets e filtro de idioma
    query_terms = " OR ".join(f'"{k}"' for k in keywords)
    query = f"({query_terms}) -is:retweet lang:pt"

    # Plano gratuito: max 10 por request
    max_results = min(limit, 10)

    all_tweets = []

    try:
        response = client.search_recent_tweets(
            query=query,
            tweet_fields=["created_at", "public_metrics", "author_id", "lang"],
            max_results=max_results
        )

        if not response.data:
            logging.info(f"Nenhum tweet encontrado para: {query}")
            return pd.DataFrame()

        for tweet in response.data:
            metrics = tweet.public_metrics or {}
            all_tweets.append({
                "Fonte": "Twitter/X",
                "Tipo": "Tweet",
                "tweet_id": str(tweet.id),
                "author_id": str(tweet.author_id),
                "texto": tweet.text,
                "like_count": metrics.get("like_count", 0),
                "retweet_count": metrics.get("retweet_count", 0),
                "reply_count": metrics.get("reply_count", 0),
                "lang": tweet.lang,
                "created_at": tweet.created_at,
                "collected_at": datetime.utcnow(),
                "url": f"https://twitter.com/i/web/status/{tweet.id}"
            })

        logging.info(f"Coletados {len(all_tweets)} tweets para: {query}")

    except tweepy.errors.TooManyRequests:
        logging.warning("Rate limit atingido. Aguardando...")
        time.sleep(15 * 60)
    except tweepy.errors.Forbidden as e:
        logging.error(f"Acesso negado (verifique o nível do plano): {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Erro na coleta do Twitter: {e}")
        return pd.DataFrame()

    if not all_tweets:
        return pd.DataFrame()

    df = pd.DataFrame(all_tweets)
    append_parquet(TWEETS_PATH, df, "tweet_id")

    return df
