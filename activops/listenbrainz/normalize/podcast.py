from __future__ import annotations

from activops.listenbrainz.models import PodcastConfig, ScrobbleRow
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def enrich_podcast_scrobble(
    scrobble: ScrobbleRow,
    config: PodcastConfig,
    not_found_logfile: str | None = None,
    logger: LoggerProtocol | None = None,
) -> ScrobbleRow:
    logger = ensure_logger(logger, __name__)
    try:
        if scrobble.get("_normalized"):
            title_key = (scrobble.get("artist") or "").strip().lower()
            rule = config.get(title_key)
            if rule:
                scrobble["theme"] = rule.get("theme", scrobble.get("theme"))
            return scrobble

        title_key = (scrobble.get("title") or "").strip().lower()
        rule = config.get(title_key)
        if rule:
            original_title = rule.get("_original_title", scrobble.get("title"))
            if rule.get("switch_title_artist"):
                scrobble["title"], scrobble["artist"] = (
                    scrobble.get("artist") or "",
                    original_title or scrobble.get("title"),
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

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Erreur enrichissement scrobble podcast : %s", exc)

    return scrobble


@with_child_logger
def normalize_france_inter_live(scrobble: ScrobbleRow, logger: LoggerProtocol | None = None) -> ScrobbleRow:
    logger = ensure_logger(logger, __name__)
    try:
        title = scrobble.get("title") or ""
        artist = scrobble.get("artist") or ""
        if title in {"Le 7/10", "Le 6/9", "Le 5/7"} and " • " in artist:
            chronique_name, _sep, real_title = artist.partition(" • ")
            scrobble["album"] = title
            scrobble["artist"] = chronique_name.strip()
            scrobble["title"] = real_title.strip()
            scrobble["service"] = "France Inter"
            scrobble["scrobble_type"] = "live_radio"
            scrobble["_normalized"] = "Live Radio"
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Erreur normalisation live France Inter : %s", exc)
    return scrobble
