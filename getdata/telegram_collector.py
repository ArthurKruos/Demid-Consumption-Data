import os
import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

try:
    from telethon.sync import TelegramClient
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    from telethon import functions, errors
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

DATA_DIR = "data"
LOG_DIR = "logs"
SESSION_DIR = os.path.join(DATA_DIR, "sessions")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)

MESSAGES_PATH = os.path.join(DATA_DIR, "telegram_messages.parquet")

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "telegram_collector.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Canais e grupos públicos relevantes para pesquisa de gaming PT-BR
# Apenas canais públicos — sem necessidade de convite ou adesão
DEFAULT_GAMING_CHANNELS = [
    "gamesbrasileiros",
    "jogosindiesbr",
    "streamersbrasil",
]


def is_portuguese(text):
    if not text or len(text) < 15:
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


def _get_existing_ids():
    if os.path.exists(MESSAGES_PATH):
        df = pd.read_parquet(MESSAGES_PATH, columns=["message_id"])
        return set(df["message_id"].astype(str))
    return set()


def fetch_telegram_data(api_id, api_hash, channels, keywords=None, limit=200):
    """
    Coleta mensagens de canais/grupos PÚBLICOS do Telegram.

    Parâmetros:
        api_id     : int  — obtido em my.telegram.org
        api_hash   : str  — obtido em my.telegram.org
        channels   : list[str] — usernames dos canais (sem @)
        keywords   : list[str] | None — filtra mensagens por termos
        limit      : int  — máximo de mensagens por canal

    Retorna DataFrame com as mensagens coletadas.
    """
    if not TELETHON_AVAILABLE:
        logging.error("Telethon não instalado. Execute: pip install telethon")
        return pd.DataFrame(), "Telethon não instalado. Execute: pip install telethon"

    if not api_id or not api_hash:
        return pd.DataFrame(), "api_id e api_hash são obrigatórios."

    if not channels:
        channels = DEFAULT_GAMING_CHANNELS

    existing_ids = _get_existing_ids()
    session_path = os.path.join(SESSION_DIR, "demid_session")
    all_messages = []

    keywords_lower = [k.lower() for k in keywords] if keywords else []

    try:
        with TelegramClient(session_path, int(api_id), api_hash) as client:
            for channel_username in channels:
                try:
                    entity = client.get_entity(channel_username)
                    channel_title = getattr(entity, "title", channel_username)

                    messages = client.get_messages(entity, limit=limit)

                    for msg in messages:
                        if not msg.text:
                            continue

                        msg_id = f"{channel_username}_{msg.id}"
                        if msg_id in existing_ids:
                            continue

                        if not is_portuguese(msg.text):
                            continue

                        # Filtra por keyword se fornecida
                        if keywords_lower:
                            text_lower = msg.text.lower()
                            if not any(kw in text_lower for kw in keywords_lower):
                                continue

                        has_media = isinstance(
                            msg.media, (MessageMediaPhoto, MessageMediaDocument)
                        )

                        all_messages.append({
                            "Fonte": "Telegram",
                            "Tipo": "Mensagem",
                            "message_id": msg_id,
                            "channel": channel_username,
                            "channel_title": channel_title,
                            "sender_id": str(msg.sender_id) if msg.sender_id else None,
                            "texto": msg.text,
                            "views": getattr(msg, "views", 0) or 0,
                            "forwards": getattr(msg, "forwards", 0) or 0,
                            "replies": getattr(msg.replies, "replies", 0) if msg.replies else 0,
                            "has_media": has_media,
                            "created_at": msg.date.replace(tzinfo=timezone.utc) if msg.date else None,
                            "collected_at": datetime.utcnow(),
                        })

                    logging.info(
                        f"Canal @{channel_username}: {len([m for m in all_messages if m['channel'] == channel_username])} mensagens coletadas."
                    )

                except errors.ChannelInvalidError:
                    logging.warning(f"Canal inválido ou privado: @{channel_username}")
                except errors.UsernameNotOccupiedError:
                    logging.warning(f"Canal não encontrado: @{channel_username}")
                except Exception as e:
                    logging.error(f"Erro no canal @{channel_username}: {e}")

    except Exception as e:
        logging.error(f"Erro ao conectar no Telegram: {e}")
        return pd.DataFrame(), f"Erro de conexão: {e}"

    if not all_messages:
        return pd.DataFrame(), "Nenhuma mensagem coletada."

    df = pd.DataFrame(all_messages)
    append_parquet(MESSAGES_PATH, df, "message_id")
    logging.info(f"Total coletado: {len(df)} mensagens do Telegram.")

    return df, None
