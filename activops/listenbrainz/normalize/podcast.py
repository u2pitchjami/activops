from activops.db.db_connection import get_db_connection
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

@with_child_logger
def enrich_podcast_scrobble(
    scrobble: dict, config: dict, not_found_logfile=None, logger: LoggerProtocol | None = None
) -> dict:
    logger = ensure_logger(logger, __name__)
    try:
        if scrobble.get("_normalized"):
            title_key = scrobble.get("artist", "").strip().lower()
            if title_key in config:
                rule = config[title_key]
                scrobble["theme"] = rule.get("theme", scrobble.get("theme"))
            return scrobble

        title_key = scrobble.get("title", "").strip().lower()
        if title_key in config:
            #     msg = f"Scrobble non enrichi (inconnu dans config) : '{scrobble.get('title', '')}'"
            #     logger.info(msg)
            #     if not_found_logfile:
            #         with open(not_found_logfile, "a", encoding="utf-8") as f:
            #             f.write(msg + "\n")
            #     return scrobble

            rule = config[title_key]
            original_title = rule.get("_original_title", scrobble.get("title"))
            # print(f"🚧 Enrichissement scrobble podcast : {original_title} - {scrobble.get('artist', '')}")
            if rule.get("switch_title_artist"):
                scrobble["title"], scrobble["artist"] = (
                    scrobble.get("artist", ""),
                    original_title,
                )
            else:
                scrobble["title"] = original_title

            if rule.get("force_album"):
                scrobble["album"] = rule.get("album")
            elif rule.get("set_album_if_missing") and not scrobble.get("album"):
                scrobble["album"] = rule.get("album")

            scrobble["service"] = rule.get("service", scrobble.get("service"))
            scrobble["theme"] = rule.get("theme", scrobble.get("theme"))
            scrobble["scrobble_type"] = "podcast"
            scrobble["_normalized"] = "Podcast"

    except Exception as e:
        logger.warning("Erreur enrichissement scrobble podcast : %s", e)

    return scrobble


@with_child_logger
def normalize_france_inter_live(scrobble: dict, logger: LoggerProtocol | None = None) -> dict:
    logger = ensure_logger(logger, __name__)
    try:
        title = scrobble.get("title", "")
        artist = scrobble.get("artist", "")
        # print(f"🚧 Normalisation France Inter live : {title} - {artist}")
        if title in ["Le 7/10", "Le 6/9", "Le 5/7"] and " • " in artist:
            chronique_name, sep, real_title = artist.partition(" • ")
            scrobble["album"] = title
            scrobble["artist"] = chronique_name.strip()
            scrobble["title"] = real_title.strip()
            scrobble["service"] = "France Inter"
            scrobble["scrobble_type"] = "live_radio"
            scrobble["_normalized"] = "Live Radio"

    except Exception as e:
        logger.warning("Erreur normalisation live France Inter : %s", e)

    return scrobble
