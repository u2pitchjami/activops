from __future__ import annotations

import json

from activops.listenbrainz.models import PodcastConfig, VideoArtistIndex, VideoArtistIndexEntry, VideoConfig
from activops.utils.config import PODCAST_JSON_PATH, VIDEO_JSON_PATH
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def load_chronique_config(logger: LoggerProtocol | None = None) -> PodcastConfig:
    logger = ensure_logger(logger, __name__)
    try:
        with open(PODCAST_JSON_PATH, encoding="utf-8") as file:
            raw_config = json.load(file)
        # clés en lower() + stocker l’original
        config: PodcastConfig = {k.lower(): {"_original_title": k, **v} for k, v in raw_config.items()}
        logger.info("Configuration des chroniques podcasts chargée (%d entrées)", len(config))
        return config
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Erreur chargement configuration chroniques : %s", exc)
        return {}


@with_child_logger
def build_video_artist_index(config_raw: VideoConfig, logger: LoggerProtocol | None = None) -> VideoArtistIndex:
    logger = ensure_logger(logger, __name__)
    index: VideoArtistIndex = {}
    for theme, rule in config_raw.items():
        service = rule.get("service")
        for artist_name in rule.get("artist", []) or []:
            key = artist_name.strip().lower()
            index[key] = VideoArtistIndexEntry(
                service=service,
                theme=theme,
                scrobble_type="video",
                _original_artist=artist_name,
            )
    logger.info("Index artistes vidéos construit (%d entrées)", len(index))
    return index


@with_child_logger
def load_video_config(logger: LoggerProtocol | None = None) -> VideoConfig:
    logger = ensure_logger(logger, __name__)
    try:
        with open(VIDEO_JSON_PATH, encoding="utf-8") as file:
            raw_config = json.load(file)
        config: VideoConfig = {k.lower(): {"_original_artist": k, **v} for k, v in raw_config.items()}
        logger.info("Configuration des vidéos chargée (%d entrées)", len(config))
        return config
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Erreur chargement config vidéos : %s", exc)
        return {}
