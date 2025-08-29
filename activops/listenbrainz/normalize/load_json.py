import json
import os

from activops.db.db_connection import get_db_connection
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger
from activops.utils.config import PODCAST_JSON_PATH, VIDEO_JSON_PATH

@with_child_logger
def load_chronique_config(logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    try:
        json_path = PODCAST_JSON_PATH
        with open(json_path, "r", encoding="utf-8") as file:
            raw_config = json.load(file)
        config = {k.lower(): {"_original_title": k, **v} for k, v in raw_config.items()}
        logger.info(
            "Configuration des chroniques podcasts chargée (%d entrées)", len(config)
        )
        return config
    except Exception as exc:
        logger.error("Erreur chargement configuration chroniques : %s", exc)
        return {}

@with_child_logger
def build_video_artist_index(config_raw, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    index = {}
    for theme, rule in config_raw.items():
        service = rule.get("service")
        for artist_name in rule.get("artist", []):
            key = artist_name.strip().lower()
            index[key] = {
                "service": service,
                "theme": theme,
                "scrobble_type": "video",
                "_original_artist": artist_name,
            }
    logger.info("Index artistes vidéos construit (%d entrées)", len(index))
    return index

@with_child_logger
def load_video_config(logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    try:
        json_path = VIDEO_JSON_PATH
        with open(json_path, "r", encoding="utf-8") as file:
            raw_config = json.load(file)
        config = {
            k.lower(): {"_original_artist": k, **v} for k, v in raw_config.items()
        }
        logger.info("Configuration des vidéos chargée (%d entrées)", len(config))
        return config
    except Exception as exc:
        logger.error("Erreur chargement config vidéos : %s", exc)
        return {}
