from activops.db.db_connection import get_db_connection
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

@with_child_logger
def enrich_video_scrobble(scrobble: dict, index: dict, logger: LoggerProtocol | None = None) -> dict:
    logger = ensure_logger(logger, __name__)
    try:
        if scrobble.get("_normalized"):
            return scrobble

        artist_key = scrobble.get("artist", "").strip().lower()
        if artist_key not in index:
            return scrobble

        rule = index[artist_key]
        scrobble["artist"] = rule["_original_artist"]
        scrobble["service"] = rule["service"]
        scrobble["theme"] = rule["theme"]
        scrobble["scrobble_type"] = rule["scrobble_type"]
        scrobble["_normalized"] = "Video"

    except Exception as e:
        logger.warning("Erreur enrichissement scrobble vidéo : %s", e)

    return scrobble
