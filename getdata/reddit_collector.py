import os
import time
import logging
import pandas as pd
from datetime import datetime
import praw
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException


DATA_DIR = "data"
LOG_DIR = "logs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

POSTS_PATH = os.path.join(DATA_DIR, "reddit_posts.parquet")
COMMENTS_PATH = os.path.join(DATA_DIR, "reddit_comments.parquet")

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "reddit_collector.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ==========================================================
# UTILITÁRIOS
# ==========================================================

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


def get_reddit_client(client_id, client_secret, user_agent):
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )


# ==========================================================
# COLETA INTERNA POR SUBREDDIT
# ==========================================================

def collect_subreddit(client, subreddit_name, limit=100):

    subreddit = client.subreddit(subreddit_name)

    posts_data = []
    comments_data = []

    for post in subreddit.new(limit=limit):

        if is_portuguese(post.title + " " + post.selftext):
            posts_data.append({
                "Fonte": "Reddit",
                "Tipo": "Post",
                "post_id": post.id,
                "subreddit": post.subreddit.display_name,
                "author": str(post.author),
                "texto": post.title + " " + post.selftext,
                "score": post.score,
                "num_comments": post.num_comments,
                "created_utc": datetime.fromtimestamp(post.created_utc),
                "collected_at": datetime.utcnow()
            })

        post.comments.replace_more(limit=0)

        for comment in post.comments.list():

            if is_portuguese(comment.body):
                comments_data.append({
                    "Fonte": "Reddit",
                    "Tipo": "Comentario",
                    "comment_id": comment.id,
                    "post_id": post.id,
                    "subreddit": post.subreddit.display_name,
                    "author": str(comment.author),
                    "texto": comment.body,
                    "score": comment.score,
                    "depth": comment.depth,
                    "created_utc": datetime.fromtimestamp(comment.created_utc),
                    "collected_at": datetime.utcnow()
                })

        time.sleep(0.2)

    return posts_data, comments_data


# ==========================================================
# FUNÇÃO PRINCIPAL PARA O APP
# ==========================================================

def fetch_reddit_data(client_id, client_secret, user_agent, keywords=None, limit=100):
    """
    Função chamada pelo app.py.
    Retorna DataFrame consolidado.
    """

    if not client_id or not client_secret:
        logging.error("Credenciais Reddit ausentes.")
        return pd.DataFrame()

    client = get_reddit_client(client_id, client_secret, user_agent)

    all_posts = []
    all_comments = []

    # Se keywords forem passadas, busca em /r/all
    if keywords:
        for keyword in keywords:
            try:
                for submission in client.subreddit("all").search(keyword, limit=limit):

                    submission.comments.replace_more(limit=0)

                    if is_portuguese(submission.title + " " + submission.selftext):
                        all_posts.append({
                            "Fonte": "Reddit",
                            "Tipo": "Post",
                            "keyword": keyword,
                            "subreddit": submission.subreddit.display_name,
                            "texto": submission.title + " " + submission.selftext,
                            "score": submission.score,
                            "created_utc": datetime.fromtimestamp(submission.created_utc),
                            "collected_at": datetime.utcnow()
                        })

                    for comment in submission.comments.list():
                        if is_portuguese(comment.body):
                            all_comments.append({
                                "Fonte": "Reddit",
                                "Tipo": "Comentario",
                                "keyword": keyword,
                                "subreddit": submission.subreddit.display_name,
                                "texto": comment.body,
                                "score": comment.score,
                                "created_utc": datetime.fromtimestamp(comment.created_utc),
                                "collected_at": datetime.utcnow()
                            })

            except Exception as e:
                logging.error(f"Erro ao buscar keyword {keyword}: {e}")

    # Salva incremental
    if all_posts:
        append_parquet(POSTS_PATH, pd.DataFrame(all_posts), "texto")

    if all_comments:
        append_parquet(COMMENTS_PATH, pd.DataFrame(all_comments), "texto")

    df_final = pd.DataFrame(all_posts + all_comments)

    logging.info("Coleta finalizada via fetch_reddit_data.")

    return df_final


# ==========================================================
# COLETA POR SUBREDDIT — mapeamento PIBIC (schema unificado)
# ==========================================================
# Gera o MESMO schema do coletor PIBIC do YouTube, para que o módulo
# analise_youtube.py funcione sem alteração (canal_origem = subreddit).

PIBIC_POSTS_PATH = os.path.join(DATA_DIR, "reddit_pibic_posts.parquet")
PIBIC_COMMENTS_PATH = os.path.join(DATA_DIR, "reddit_pibic_comments.parquet")


def collect_subreddit_pibic(
    client,
    subreddit_name,
    categoria="",
    limit=50,
    ordenacao="hot",
    get_comments=True,
    max_comments=100,
):
    """
    Coleta posts e comentários de um subreddit, capturando autor.
    Retorna (posts, comments) já no schema unificado de análise.
    """
    sub = client.subreddit(subreddit_name)

    # Seleciona a listagem conforme a ordenação
    if ordenacao == "top":
        listagem = sub.top(time_filter="year", limit=limit)
    elif ordenacao == "new":
        listagem = sub.new(limit=limit)
    else:
        listagem = sub.hot(limit=limit)

    posts, comments = [], []

    for post in listagem:
        posts.append({
            "video_id": post.id,                     # post_id (mantém nome p/ reuso)
            "channel": subreddit_name,
            "title": post.title or "",
            "description": (post.selftext or "")[:500],
            "view_count": 0,                          # Reddit não expõe views
            "like_count": int(post.score or 0),       # score = upvotes líquidos
            "comment_count": int(post.num_comments or 0),
            "published_at": datetime.fromtimestamp(post.created_utc),
            "url": f"https://reddit.com{post.permalink}",
            "collected_at": datetime.utcnow(),
            "categoria_pibic": categoria,
            "canal_origem": subreddit_name,
            "collector": "praw",
        })

        if get_comments:
            try:
                post.comments.replace_more(limit=0)
                for c in post.comments.list()[:max_comments]:
                    body = getattr(c, "body", "") or ""
                    if not body or body in ("[deleted]", "[removed]"):
                        continue
                    comments.append({
                        "comment_id": c.id,
                        "video_id": post.id,
                        "parent_id": getattr(c, "parent_id", None),
                        "author": str(c.author),
                        "texto": body,
                        "like_count": int(getattr(c, "score", 0) or 0),
                        "published_at": datetime.fromtimestamp(c.created_utc),
                        "collected_at": datetime.utcnow(),
                        "categoria_pibic": categoria,
                        "canal_origem": subreddit_name,
                    })
            except Exception as e:
                logging.error(f"Erro nos comentários de {post.id}: {e}")

        time.sleep(0.3)

    return posts, comments


def fetch_reddit_pibic(
    client_id,
    client_secret,
    user_agent,
    subreddits,
    limit=50,
    ordenacao="hot",
    get_comments=True,
    max_comments=100,
    progress_callback=None,
):
    """
    Coleta um conjunto de subreddits e persiste em parquet dedicado.

    Parâmetros:
        subreddits : lista de dicts {nome, categoria}
        progress_callback : função(idx, total, nome) para feedback de UI

    Retorna dict {"videos": df_posts, "comments": df_comments}
    (chave "videos" mantida para compatibilidade com o painel de análise).
    """
    if not client_id or not client_secret:
        logging.error("Credenciais Reddit ausentes.")
        return {"videos": pd.DataFrame(), "comments": pd.DataFrame(),
                "erro": "Credenciais Reddit ausentes."}

    try:
        client = get_reddit_client(client_id, client_secret, user_agent)
    except Exception as e:
        return {"videos": pd.DataFrame(), "comments": pd.DataFrame(),
                "erro": f"Falha ao conectar no Reddit: {e}"}

    all_posts, all_comments = [], []
    total = len(subreddits)

    for idx, sr in enumerate(subreddits, start=1):
        nome = sr["nome"].strip().lstrip("r/").lstrip("/")
        if not nome:
            continue
        if progress_callback:
            progress_callback(idx, total, f"r/{nome}")

        try:
            posts, comments = collect_subreddit_pibic(
                client=client,
                subreddit_name=nome,
                categoria=sr.get("categoria", ""),
                limit=limit,
                ordenacao=ordenacao,
                get_comments=get_comments,
                max_comments=max_comments,
            )
            all_posts.extend(posts)
            all_comments.extend(comments)
        except Exception as e:
            logging.error(f"Erro no subreddit r/{nome}: {e}")

    if all_posts:
        append_parquet(PIBIC_POSTS_PATH, pd.DataFrame(all_posts), "video_id")
    if all_comments:
        append_parquet(PIBIC_COMMENTS_PATH, pd.DataFrame(all_comments), "comment_id")

    logging.info(
        f"Reddit PIBIC | posts={len(all_posts)} | comentários={len(all_comments)}"
    )

    return {
        "videos": pd.DataFrame(all_posts),
        "comments": pd.DataFrame(all_comments),
        "erro": None,
    }