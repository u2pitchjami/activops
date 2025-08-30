from __future__ import annotations

from activops.db.db_connection import get_db_connection
from activops.listenbrainz.models import ScrobbleRow
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def get_scrobbles_from_db(all: bool | None = None, logger: LoggerProtocol | None = None) -> list[ScrobbleRow]:
    logger = ensure_logger(logger, __name__)
    results: list[ScrobbleRow] = []
    try:
        conn = get_db_connection(logger=logger)
        if conn is None:
            logger.error("Connexion DB indisponible")
            return results

        with conn.cursor(dictionary=True) as cursor:
            if not all:
                cursor.execute(
                    """
                    SELECT *
                    FROM listenbrainz_tracks
                    WHERE last_updated > NOW() - INTERVAL 1 DAY
                      AND theme IS NULL
                      AND scrobble_type != 'music'
                    """
                )
            else:
                cursor.execute(
                    """
                    SELECT *
                    FROM listenbrainz_tracks
                    WHERE theme IS NULL
                      AND scrobble_type != 'music'
                    """
                )
            rows = cursor.fetchall() or []
            results = rows
            logger.info("Scrobbles récupérés depuis la base : %d", len(results))
        conn.close()
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Erreur lors de la lecture de la base : %s", exc)
    return results


@with_child_logger
def inject_normalized_scrobble(scrobble: ScrobbleRow, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    try:
        if scrobble.get("_normalized"):
            conn = get_db_connection(logger=logger)
            if conn is None:
                logger.error("Connexion DB indisponible")
                return
            sql = """
                UPDATE listenbrainz_tracks
                SET title = %s, artist = %s, album = %s, service = %s,
                    theme = %s, scrobble_type = %s
                WHERE track_id = %s
            """
            values = (
                scrobble.get("title"),
                scrobble.get("artist"),
                scrobble.get("album"),
                scrobble.get("service"),
                scrobble.get("theme"),
                scrobble.get("scrobble_type"),
                scrobble.get("track_id"),
            )
            with conn.cursor() as cursor:
                cursor.execute(sql, values)
                conn.commit()
            logger.info(
                "👌 Scrobble ID %s - [%s] - %s - %s mis à jour.",
                scrobble.get("track_id"),
                scrobble.get("_normalized"),
                scrobble.get("artist"),
                scrobble.get("title"),
            )
            conn.close()
        else:
            logger.warning(
                "🚨 Scrobble ID %s - %s - %s non normalisé, pas d'injection.",
                scrobble.get("track_id"),
                scrobble.get("artist"),
                scrobble.get("title"),
            )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Erreur injection en base pour ID %s : %s", scrobble.get("track_id"), exc)
