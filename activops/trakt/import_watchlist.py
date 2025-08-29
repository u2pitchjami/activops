import json
import os
from pathlib import Path
from activops.db.db_connection import get_db_connection
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger
from activops.trakt.import_to_db import parse_trakt_date
from activops.utils.config import JSON_DIR

@with_child_logger
def import_watchlist(debug=False, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    cursor = conn.cursor()

    inserted = updated = 0

    for wl_file in [
        JSON_DIR / "watchlist_movies.json",
        JSON_DIR / "watchlist_shows.json",
    ]:
        if not wl_file.exists():
            continue

        data = json.loads(wl_file.read_text())
        for entry in data:
            date_add = parse_trakt_date(entry.get("listed_at"))

            media = (
                entry.get("movie") if entry["type"] == "movie" else entry.get("show")
            )

            cursor.execute(
                """
                INSERT INTO trakt_watchlist
                (type, title, prod_date, imdb_id, tmdb_id, date_add, watched, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                    date_add=VALUES(date_add),
                    last_updated=NOW();
            """,
                (
                    entry["type"],
                    media["title"],
                    media.get("year"),
                    media["ids"].get("imdb") or "NO_IMDB",
                    media["ids"].get("tmdb") or "NO_TMDB",
                    date_add,
                    "no",
                ),
            )

            if cursor.rowcount == 1:
                inserted += 1
            elif cursor.rowcount == 2:
                updated += 1

            if debug:
                logger.debug(
                    f"📌 Watchlist {media['title']} ({entry['type']})\
                        → {'Ajouté' if cursor.rowcount == 1 else 'Mis à jour'}"
                )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("✅ Import JSON → Watchlist terminé")
    logger.info(f"📌 Watchlist : {inserted} ajoutés / {updated} mis à jour")

@with_child_logger
def sync_watchlist_with_watched(debug=False, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE trakt_watchlist wl
        JOIN trakt_watched_test w
            ON (wl.imdb_id = w.imdb_id OR wl.tmdb_id = w.tmdb_id)
        SET wl.watched = 'yes',
            wl.last_updated = NOW()
        WHERE wl.watched = 'no';
    """
    )

    updated = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(
        f"🔄 Synchronisation Watchlist → Watched terminée : {updated} éléments mis à jour"
    )
