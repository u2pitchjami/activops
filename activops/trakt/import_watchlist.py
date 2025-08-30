from __future__ import annotations

import json
from pathlib import Path

from activops.db.db_connection import get_db_connection
from activops.trakt.import_to_db import parse_trakt_date
from activops.trakt.models import WatchlistItem
from activops.utils.config import JSON_DIR
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def import_watchlist(debug: bool = False, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("Connexion DB indisponible")
        return
    cursor = conn.cursor()

    inserted = updated = 0
    files: list[Path] = [
        JSON_DIR / "watchlist_movies.json",
        JSON_DIR / "watchlist_shows.json",
    ]

    try:
        for wl_file in files:
            if not wl_file.exists():
                continue

            items: list[WatchlistItem] = json.loads(wl_file.read_text(encoding="utf-8"))
            for entry in items:
                media = entry.get("movie") if entry.get("type") == "movie" else entry.get("show")
                if not media:
                    continue

                date_add = parse_trakt_date(entry.get("listed_at"))
                imdb_id = (media.get("ids") or {}).get("imdb") or "NO_IMDB"
                tmdb_id = (media.get("ids") or {}).get("tmdb") or "NO_TMDB"

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
                        entry.get("type"),
                        media.get("title"),
                        media.get("year"),
                        imdb_id,
                        tmdb_id,
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
                        "📌 Watchlist %s (%s) → %s",
                        media.get("title"),
                        entry.get("type"),
                        "Ajouté" if cursor.rowcount == 1 else "Mis à jour",
                    )

        conn.commit()
        logger.info("✅ Import JSON → Watchlist terminé")
        logger.info("📌 Watchlist : %s ajoutés / %s mis à jour", inserted, updated)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Erreur import_watchlist: %s", exc)
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Rollback a échoué")
    finally:
        cursor.close()
        conn.close()


@with_child_logger
def sync_watchlist_with_watched(debug: bool = False, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("Connexion DB indisponible")
        return
    cursor = conn.cursor()
    try:
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
        logger.info("🔄 Synchronisation Watchlist → Watched : %s éléments mis à jour", updated)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Erreur sync_watchlist_with_watched: %s", exc)
        try:
            conn.rollback()
        except Exception:
            logger.exception("Rollback a échoué")
    finally:
        cursor.close()
        conn.close()
