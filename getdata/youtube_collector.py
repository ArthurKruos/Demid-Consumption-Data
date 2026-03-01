import os
import time
import logging
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# ==========================================================
# CONFIG
# ==========================================================

DATA_DIR = "data"
LOG_DIR = "logs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

VIDEOS_PATH = os.path.join(DATA_DIR, "youtube_videos.parquet")
COMMENTS_PATH = os.path.join(DATA_DIR, "youtube_comments.parquet")

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "youtube_collector.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==========================================================
# UTIL
# ==========================================================

def is_portuguese(text):
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


def get_youtube_service(api_key):
    return build("youtube", "v3", developerKey=api_key)

# ==========================================================
# COLETA DE COMENTÁRIOS
# ==========================================================

def collect_comments(service, video_id):

    comments = []
    next_page_token = None

    while True:
        request = service.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token,
            textFormat="plainText"
        )

        response = request.execute()

        for item in response.get("items", []):

            top = item["snippet"]["topLevelComment"]["snippet"]

            if is_portuguese(top.get("textDisplay", "")):
                comments.append({
                    "comment_id": item["id"],
                    "video_id": video_id,
                    "parent_id": None,
                    "author": top.get("authorDisplayName"),
                    "text": top.get("textDisplay"),
                    "like_count": top.get("likeCount", 0),
                    "published_at": top.get("publishedAt"),
                    "collected_at": datetime.utcnow()
                })

            # Replies
            if "replies" in item:
                for reply in item["replies"]["comments"]:
                    snippet = reply["snippet"]

                    if is_portuguese(snippet.get("textDisplay", "")):
                        comments.append({
                            "comment_id": reply["id"],
                            "video_id": video_id,
                            "parent_id": item["id"],
                            "author": snippet.get("authorDisplayName"),
                            "text": snippet.get("textDisplay"),
                            "like_count": snippet.get("likeCount", 0),
                            "published_at": snippet.get("publishedAt"),
                            "collected_at": datetime.utcnow()
                        })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(0.2)

    return comments

# ==========================================================
# COLETA OTIMIZADA DE VÍDEOS
# ==========================================================

def collect_videos_by_keyword(api_key, query, max_results=50):

    service = get_youtube_service(api_key)

    # Carregar IDs já coletados
    existing_ids = set()
    if os.path.exists(VIDEOS_PATH):
        df_existing = pd.read_parquet(VIDEOS_PATH)
        existing_ids = set(df_existing["video_id"].astype(str))

    collected_videos = []
    collected_comments = []

    request = service.search().list(
        part="snippet",
        q=query,
        type="video",
        maxResults=max_results,
        relevanceLanguage="pt"
    )

    response = request.execute()

    video_ids = [
        item["id"]["videoId"]
        for item in response.get("items", [])
        if item["id"]["videoId"] not in existing_ids
    ]

    if not video_ids:
        logging.info("Nenhum vídeo novo encontrado.")
        return

    # Buscar dados detalhados em lote (1 chamada)
    video_response = service.videos().list(
        part="snippet,statistics",
        id=",".join(video_ids)
    ).execute()

    for video in video_response["items"]:

        video_id = video["id"]

        collected_videos.append({
            "video_id": video_id,
            "channel_id": video["snippet"]["channelId"],
            "title": video["snippet"]["title"],
            "description": video["snippet"]["description"],
            "published_at": video["snippet"]["publishedAt"],
            "view_count": int(video["statistics"].get("viewCount", 0)),
            "like_count": int(video["statistics"].get("likeCount", 0)),
            "comment_count": int(video["statistics"].get("commentCount", 0)),
            "collected_at": datetime.utcnow()
        })

        if int(video["statistics"].get("commentCount", 0)) > 0:
            collected_comments.extend(
                collect_comments(service, video_id)
            )

    if collected_videos:
        append_parquet(VIDEOS_PATH, pd.DataFrame(collected_videos), "video_id")

    if collected_comments:
        append_parquet(COMMENTS_PATH, pd.DataFrame(collected_comments), "comment_id")

    logging.info(f"Coleta otimizada concluída para: {query}")

# ==========================================================
# FUNÇÃO PRINCIPAL PARA STREAMLIT
# ==========================================================

def fetch_youtube_data(api_key, keywords, max_results=50):

    if not api_key:
        logging.error("API Key não fornecida.")
        return pd.DataFrame()

    if isinstance(keywords, str):
        keywords = [keywords]

    for keyword in keywords:
        try:
            collect_videos_by_keyword(api_key, keyword, max_results)
        except Exception as e:
            logging.error(f"Erro na coleta da keyword {keyword}: {e}")

    dfs = []

    if os.path.exists(VIDEOS_PATH):
        df_videos = pd.read_parquet(VIDEOS_PATH)
        df_videos["Fonte"] = "YouTube"
        df_videos["Tipo"] = "Video"
        df_videos["texto"] = df_videos["title"].fillna("") + " " + df_videos["description"].fillna("")
        dfs.append(df_videos)

    if os.path.exists(COMMENTS_PATH):
        df_comments = pd.read_parquet(COMMENTS_PATH)
        df_comments["Fonte"] = "YouTube"
        df_comments["Tipo"] = "Comentario"
        df_comments.rename(columns={"text": "texto"}, inplace=True)
        dfs.append(df_comments)

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    return pd.DataFrame()