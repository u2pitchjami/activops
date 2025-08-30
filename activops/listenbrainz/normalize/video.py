from __future__ import annotations

from activops.listenbrainz.models import ScrobbleRow, VideoArtistIndex
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def enrich_video_scrobble(
    scrobble: ScrobbleRow,
    index: VideoArtistIndex,
    logger: LoggerProtocol | None = None,
) -> ScrobbleRow:
    """
    Enrichit un scrobble 'video' à partir d'un index artiste → (service, thème, type).
    """
    logger = ensure_logger(logger, __name__)
    try:
        # déjà normalisé ? on ne refait rien
        if scrobble.get("_normalized"):
            return scrobble

        artist_key = (scrobble.get("artist") or "").strip().lower()
        rule = index.get(artist_key)
        if not rule:
            return scrobble

        # Restaure l'artiste d'origine, et applique service / thème / type
        original_artist = rule.get("_original_artist") or scrobble.get("artist")
        if original_artist:
            scrobble["artist"] = original_artist

        if rule.get("service") is not None:
            scrobble["service"] = rule.get("service")

        if rule.get("theme") is not None:
            scrobble["theme"] = rule.get("theme")

        scrobble["scrobble_type"] = rule.get("scrobble_type", "video")
        scrobble["_normalized"] = "Video"

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Erreur enrichissement scrobble vidéo : %s", exc)

    return scrobble
