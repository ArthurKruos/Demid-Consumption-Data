"""
YouTube Collector — DEMID
=========================
Estratégia dual:
  1. yt-dlp (PADRÃO) — sem chave de API, sem cota, coleta vídeos + comentários
  2. YouTube Data API v3 (FALLBACK) — usa quando api_key for fornecida e yt-dlp falhar

Por que yt-dlp como padrão?
  - Nenhuma credencial necessária
  - Sem limite de cota (API oficial: apenas 100 buscas/dia no plano gratuito)
  - Coleta comentários nativamente (--get-comments)
  - Mais dados: tags, categorias, legendas automáticas, duração
  - Usado ativamente em pesquisa acadêmica
"""

import os
import time
import logging
import pandas as pd
from datetime import datetime
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

# yt-dlp — instalação: pip install yt-dlp
try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

# Google API — fallback opcional
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_API_AVAILABLE = True
except ImportError:
    GOOGLE_API_AVAILABLE = False

# ==============================================================
# CONFIGURAÇÃO
# ==============================================================

DATA_DIR = "data"
LOG_DIR = "logs"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

VIDEOS_PATH = os.path.join(DATA_DIR, "youtube_videos.parquet")
COMMENTS_PATH = os.path.join(DATA_DIR, "youtube_comments.parquet")

logger = logging.getLogger("youtube_collector")
if not logger.handlers:
    handler = logging.FileHandler(os.path.join(LOG_DIR, "youtube_collector.log"))
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ==============================================================
# UTILITÁRIOS
# ==============================================================

def is_portuguese(text: str) -> bool:
    """Detecta português — tolerante a textos mistos PT/EN comuns no gaming BR."""
    if not text or len(text) < 10:
        return True  # textos curtos passam (evita falsos negativos em gaming)
    try:
        lang = detect(text)
        return lang in ("pt", "pt-br")
    except LangDetectException:
        return True  # em caso de dúvida, mantém


def append_parquet(path: str, df_new: pd.DataFrame, id_column: str) -> None:
    """Salva incrementalmente, deduplicando por id_column."""
    if df_new.empty:
        return
    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=[id_column])
    else:
        df_combined = df_new
    df_combined.to_parquet(path, index=False)


def _load_existing_video_ids() -> set:
    if os.path.exists(VIDEOS_PATH):
        df = pd.read_parquet(VIDEOS_PATH, columns=["video_id"])
        return set(df["video_id"].astype(str))
    return set()


# ==============================================================
# COLETOR PRINCIPAL — yt-dlp (sem API key, sem cota)
# ==============================================================

def _ytdlp_opts(get_comments: bool = False, max_comments: int = 200) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "extract_flat": False,
    }
    if get_comments:
        opts["getcomments"] = True
        opts["extractor_args"] = {
            "youtube": {"max_comments": [str(max_comments)]}
        }
    return opts


def _parse_video_entry(entry: dict) -> dict | None:
    """Converte um entry do yt-dlp para o schema do projeto."""
    if not entry or entry.get("_type") == "playlist":
        return None

    video_id = entry.get("id", "")
    if not video_id:
        return None

    title = entry.get("title", "") or ""
    description = entry.get("description", "") or ""

    return {
        "video_id": video_id,
        "channel_id": entry.get("channel_id") or entry.get("uploader_id", ""),
        "channel": entry.get("channel") or entry.get("uploader", ""),
        "title": title,
        "description": description[:500],  # trunca para não explodir parquet
        "tags": ", ".join(entry.get("tags") or []),
        "duration_sec": entry.get("duration", 0) or 0,
        "published_at": entry.get("upload_date", ""),  # formato YYYYMMDD
        "view_count": int(entry.get("view_count") or 0),
        "like_count": int(entry.get("like_count") or 0),
        "comment_count": int(entry.get("comment_count") or 0),
        "url": f"https://youtube.com/watch?v={video_id}",
        "collected_at": datetime.utcnow(),
        "collector": "yt-dlp",
    }


def _parse_comments_from_entry(entry: dict) -> list[dict]:
    """Extrai comentários de um entry yt-dlp."""
    comments_raw = entry.get("comments") or []
    parsed = []
    for c in comments_raw:
        text = c.get("text", "") or ""
        if not text:
            continue
        parsed.append({
            "comment_id": c.get("id", f"ytdlp_{entry.get('id')}_{len(parsed)}"),
            "video_id": entry.get("id", ""),
            "parent_id": c.get("parent") if c.get("parent") != "root" else None,
            "author": c.get("author", ""),
            "texto": text,
            "like_count": int(c.get("like_count") or 0),
            "published_at": c.get("timestamp", ""),
            "collected_at": datetime.utcnow(),
        })
    return parsed


def collect_ytdlp(
    query: str,
    max_results: int = 20,
    get_comments: bool = True,
    max_comments: int = 100,
) -> tuple[list[dict], list[dict]]:
    """
    Busca vídeos no YouTube via yt-dlp.
    Retorna (videos, comments).
    """
    if not YTDLP_AVAILABLE:
        logger.error("yt-dlp não instalado. Execute: pip install yt-dlp")
        return [], []

    existing_ids = _load_existing_video_ids()
    videos_out = []
    comments_out = []

    search_url = f"ytsearch{max_results}:{query}"
    opts = _ytdlp_opts(get_comments=get_comments, max_comments=max_comments)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            result = ydl.extract_info(search_url, download=False)

        entries = result.get("entries") or []

        for entry in entries:
            if not entry:
                continue

            video = _parse_video_entry(entry)
            if not video:
                continue

            if video["video_id"] in existing_ids:
                logger.info(f"Vídeo já coletado, pulando: {video['video_id']}")
                continue

            # Filtra por idioma (título + descrição)
            texto_check = video["title"] + " " + video["description"]
            if not is_portuguese(texto_check):
                continue

            videos_out.append(video)
            existing_ids.add(video["video_id"])

            if get_comments:
                comments_out.extend(_parse_comments_from_entry(entry))

        logger.info(
            f"yt-dlp | query='{query}' | vídeos={len(videos_out)} | comentários={len(comments_out)}"
        )

    except Exception as e:
        logger.error(f"yt-dlp falhou para '{query}': {e}")

    return videos_out, comments_out


# ==============================================================
# COLETA POR CANAL — para o mapeamento PIBIC (sem API key)
# ==============================================================

PIBIC_VIDEOS_PATH = os.path.join(DATA_DIR, "pibic_videos.parquet")
PIBIC_COMMENTS_PATH = os.path.join(DATA_DIR, "pibic_comments.parquet")


def _normalize_channel_url(url: str) -> str:
    """Garante que a URL aponte para a aba /videos do canal."""
    url = url.strip().rstrip("/")
    # Remove sufixos de aba já existentes
    for suf in ("/videos", "/streams", "/shorts", "/featured", "/about"):
        if url.endswith(suf):
            url = url[: -len(suf)]
            break
    return url + "/videos"


def _extract_single_video(video_id: str, get_comments: bool, max_comments: int) -> tuple[dict | None, list[dict]]:
    """Extrai um vídeo individual (metadados + comentários) via yt-dlp."""
    if not YTDLP_AVAILABLE:
        return None, []

    opts = _ytdlp_opts(get_comments=get_comments, max_comments=max_comments)
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            entry = ydl.extract_info(url, download=False)
        if not entry:
            return None, []
        video = _parse_video_entry(entry)
        comments = _parse_comments_from_entry(entry) if get_comments else []
        return video, comments
    except Exception as e:
        logger.error(f"Erro ao extrair vídeo {video_id}: {e}")
        return None, []


def collect_channel(
    channel_url: str,
    categoria: str = "",
    max_videos: int = 30,
    get_comments: bool = True,
    max_comments: int = 100,
    filter_pt: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Coleta vídeos e comentários de um CANAL específico via yt-dlp.

    Estratégia em 2 passos (mais robusta para canais grandes):
      1. Extração flat da aba /videos → lista de IDs (rápido)
      2. Extração completa de cada vídeo (metadados + comentários)

    Parâmetros:
        channel_url  : URL ou handle do canal (@nome, /user/nome, /channel/ID)
        categoria    : rótulo de origem (mantido como metadado de pesquisa)
        max_videos   : máximo de vídeos a coletar do canal
        get_comments : se True, coleta comentários de cada vídeo
        max_comments : limite de comentários por vídeo
        filter_pt    : se True, descarta vídeos não-portugueses
                       (default False — canais curados são analisados na íntegra)

    Retorna (videos, comments).
    """
    if not YTDLP_AVAILABLE:
        logger.error("yt-dlp não instalado. Execute: pip install yt-dlp")
        return [], []

    videos_out: list[dict] = []
    comments_out: list[dict] = []

    canal_videos_url = _normalize_channel_url(channel_url)

    # ---- Passo 1: lista flat de vídeos do canal ----
    flat_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "extract_flat": "in_playlist",
        "playlistend": max_videos,
    }

    video_ids: list[str] = []
    channel_name = channel_url
    try:
        with yt_dlp.YoutubeDL(flat_opts) as ydl:
            info = ydl.extract_info(canal_videos_url, download=False)
        channel_name = info.get("channel") or info.get("title") or channel_url
        for entry in (info.get("entries") or [])[:max_videos]:
            if entry and entry.get("id"):
                video_ids.append(entry["id"])
    except Exception as e:
        logger.error(f"Erro ao listar canal {channel_url}: {e}")
        return [], []

    if not video_ids:
        logger.warning(f"Nenhum vídeo encontrado no canal {channel_url}")
        return [], []

    # ---- Passo 2: extração completa de cada vídeo ----
    for vid in video_ids:
        video, comments = _extract_single_video(vid, get_comments, max_comments)
        if not video:
            continue

        if filter_pt:
            texto_check = video["title"] + " " + video["description"]
            if not is_portuguese(texto_check):
                continue

        # Anexa metadados de pesquisa
        video["categoria_pibic"] = categoria
        video["canal_origem"] = channel_url
        videos_out.append(video)

        for c in comments:
            c["categoria_pibic"] = categoria
            c["canal_origem"] = channel_url
        comments_out.extend(comments)

        time.sleep(0.5)  # pausa anti-bloqueio entre vídeos

    logger.info(
        f"Canal '{channel_name}' | vídeos={len(videos_out)} | comentários={len(comments_out)}"
    )
    return videos_out, comments_out


def fetch_pibic_data(
    canais: list[dict],
    max_videos: int = 30,
    get_comments: bool = True,
    max_comments: int = 100,
    videos_avulsos: list[dict] | None = None,
    progress_callback=None,
) -> dict:
    """
    Coleta o conjunto curado de canais do PIBIC e persiste em parquet dedicado.

    Parâmetros:
        canais          : lista de dicts {url, categoria, obs}
        max_videos      : vídeos por canal
        get_comments    : coletar comentários
        max_comments    : limite por vídeo
        videos_avulsos  : lista opcional de {video_id, categoria, obs}
        progress_callback : função(idx, total, nome) para feedback de UI

    Retorna dict com DataFrames: {"videos": df, "comments": df}
    """
    all_videos: list[dict] = []
    all_comments: list[dict] = []

    total = len(canais) + (len(videos_avulsos) if videos_avulsos else 0)
    idx = 0

    for canal in canais:
        idx += 1
        if progress_callback:
            progress_callback(idx, total, canal["url"])

        videos, comments = collect_channel(
            channel_url=canal["url"],
            categoria=canal.get("categoria", ""),
            max_videos=max_videos,
            get_comments=get_comments,
            max_comments=max_comments,
            filter_pt=False,
        )
        all_videos.extend(videos)
        all_comments.extend(comments)

    # Vídeos avulsos
    if videos_avulsos:
        for v in videos_avulsos:
            idx += 1
            if progress_callback:
                progress_callback(idx, total, f"vídeo {v['video_id']}")

            video, comments = _extract_single_video(
                v["video_id"], get_comments, max_comments
            )
            if video:
                video["categoria_pibic"] = v.get("categoria", "")
                video["canal_origem"] = "video_avulso"
                all_videos.append(video)
                for c in comments:
                    c["categoria_pibic"] = v.get("categoria", "")
                    c["canal_origem"] = "video_avulso"
                all_comments.extend(comments)

    # Persiste incrementalmente
    if all_videos:
        append_parquet(PIBIC_VIDEOS_PATH, pd.DataFrame(all_videos), "video_id")
    if all_comments:
        append_parquet(PIBIC_COMMENTS_PATH, pd.DataFrame(all_comments), "comment_id")

    df_videos = pd.DataFrame(all_videos)
    df_comments = pd.DataFrame(all_comments)

    logger.info(
        f"PIBIC | total vídeos={len(df_videos)} | total comentários={len(df_comments)}"
    )

    return {"videos": df_videos, "comments": df_comments}


def exportar_maxqda(df_videos: pd.DataFrame, df_comments: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida vídeos e comentários num único DataFrame "documento" pronto
    para importação no MAXQDA (uma linha = um documento textual).

    O MAXQDA importa CSV/Excel estruturado: cada linha vira um documento com
    variáveis (canal, categoria, tipo, autor, data, engajamento).
    """
    docs = []

    if not df_videos.empty:
        for _, v in df_videos.iterrows():
            docs.append({
                "documento_id": f"video_{v.get('video_id', '')}",
                "tipo": "Vídeo (título+descrição)",
                "categoria_pibic": v.get("categoria_pibic", ""),
                "canal": v.get("channel", ""),
                "canal_origem": v.get("canal_origem", ""),
                "autor": v.get("channel", ""),
                "titulo": v.get("title", ""),
                "texto": (str(v.get("title", "")) + "\n\n" + str(v.get("description", ""))).strip(),
                "data": v.get("published_at", ""),
                "views": v.get("view_count", 0),
                "likes": v.get("like_count", 0),
                "comentarios": v.get("comment_count", 0),
                "url": v.get("url", ""),
            })

    if not df_comments.empty:
        for _, c in df_comments.iterrows():
            docs.append({
                "documento_id": f"comentario_{c.get('comment_id', '')}",
                "tipo": "Comentário",
                "categoria_pibic": c.get("categoria_pibic", ""),
                "canal": "",
                "canal_origem": c.get("canal_origem", ""),
                "autor": c.get("author", ""),
                "titulo": "",
                "texto": str(c.get("texto", "")),
                "data": c.get("published_at", ""),
                "views": 0,
                "likes": c.get("like_count", 0),
                "comentarios": 0,
                "url": f"https://youtube.com/watch?v={c.get('video_id', '')}",
            })

    return pd.DataFrame(docs)


# ==============================================================
# FALLBACK — YouTube Data API v3 (requer api_key)
# ==============================================================

def collect_official_api(
    api_key: str,
    query: str,
    max_results: int = 20,
) -> tuple[list[dict], list[dict]]:
    """
    Fallback: coleta via YouTube Data API v3.
    Custo: 100 unidades por busca + 1 por vídeo. Cota: 10.000/dia.
    Não coleta comentários (caro em cota).
    """
    if not GOOGLE_API_AVAILABLE:
        logger.error("google-api-python-client não instalado.")
        return [], []

    existing_ids = _load_existing_video_ids()
    videos_out = []

    try:
        service = build("youtube", "v3", developerKey=api_key)

        search_resp = service.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=min(max_results, 50),
            relevanceLanguage="pt",
        ).execute()

        new_ids = [
            item["id"]["videoId"]
            for item in search_resp.get("items", [])
            if item["id"]["videoId"] not in existing_ids
        ]

        if not new_ids:
            return [], []

        video_resp = service.videos().list(
            part="snippet,statistics",
            id=",".join(new_ids),
        ).execute()

        for v in video_resp.get("items", []):
            stats = v.get("statistics", {})
            snip = v.get("snippet", {})
            title = snip.get("title", "")
            desc = snip.get("description", "")

            if not is_portuguese(title + " " + desc):
                continue

            videos_out.append({
                "video_id": v["id"],
                "channel_id": snip.get("channelId", ""),
                "channel": snip.get("channelTitle", ""),
                "title": title,
                "description": desc[:500],
                "tags": "",
                "duration_sec": 0,
                "published_at": snip.get("publishedAt", ""),
                "view_count": int(stats.get("viewCount") or 0),
                "like_count": int(stats.get("likeCount") or 0),
                "comment_count": int(stats.get("commentCount") or 0),
                "url": f"https://youtube.com/watch?v={v['id']}",
                "collected_at": datetime.utcnow(),
                "collector": "official-api",
            })

        logger.info(f"API oficial | query='{query}' | vídeos={len(videos_out)}")

    except HttpError as e:
        if e.resp.status == 403:
            logger.error(f"Cota da API YouTube esgotada ou acesso negado: {e}")
        else:
            logger.error(f"HttpError na API YouTube: {e}")

    except Exception as e:
        logger.error(f"Erro na API oficial YouTube: {e}")

    return videos_out, []


# ==============================================================
# FUNÇÃO PRINCIPAL — chamada pelo app.py
# ==============================================================

def fetch_youtube_data(
    keywords: list[str] | str,
    max_results: int = 20,
    get_comments: bool = True,
    max_comments: int = 100,
    api_key: str = "",
) -> pd.DataFrame:
    """
    Coleta dados do YouTube para uma lista de palavras-chave.

    Estratégia:
        1. Tenta yt-dlp (sem API key, sem cota)
        2. Se yt-dlp não disponível e api_key fornecida, usa API oficial

    Parâmetros:
        keywords     : lista de termos de busca
        max_results  : máximo de vídeos por keyword
        get_comments : se True, coleta comentários via yt-dlp
        max_comments : limite de comentários por vídeo
        api_key      : chave da YouTube Data API v3 (opcional, só para fallback)

    Retorna DataFrame consolidado com colunas "Fonte", "Tipo", "texto".
    """
    if isinstance(keywords, str):
        keywords = [keywords]

    all_videos: list[dict] = []
    all_comments: list[dict] = []

    for keyword in keywords:
        try:
            if YTDLP_AVAILABLE:
                videos, comments = collect_ytdlp(
                    query=keyword,
                    max_results=max_results,
                    get_comments=get_comments,
                    max_comments=max_comments,
                )
            elif api_key and GOOGLE_API_AVAILABLE:
                logger.warning("yt-dlp não disponível, usando API oficial.")
                videos, comments = collect_official_api(
                    api_key=api_key,
                    query=keyword,
                    max_results=max_results,
                )
            else:
                logger.error("Nenhum coletor YouTube disponível.")
                continue

            all_videos.extend(videos)
            all_comments.extend(comments)

        except Exception as e:
            logger.error(f"Erro na coleta YouTube para '{keyword}': {e}")

        time.sleep(1.5)  # pausa entre keywords

    # Persiste incrementalmente
    if all_videos:
        append_parquet(VIDEOS_PATH, pd.DataFrame(all_videos), "video_id")

    if all_comments:
        append_parquet(COMMENTS_PATH, pd.DataFrame(all_comments), "comment_id")

    # Monta DataFrame no schema do app
    dfs = []

    if all_videos:
        df_v = pd.DataFrame(all_videos)
        df_v["Fonte"] = "YouTube"
        df_v["Tipo"] = "Video"
        df_v["texto"] = df_v["title"].fillna("") + " " + df_v["description"].fillna("")
        dfs.append(df_v)

    if all_comments:
        df_c = pd.DataFrame(all_comments)
        df_c["Fonte"] = "YouTube"
        df_c["Tipo"] = "Comentario"
        df_c.rename(columns={"texto": "texto"}, inplace=True)
        dfs.append(df_c)

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    return pd.DataFrame()
