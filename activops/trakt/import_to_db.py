from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from activops.db.db_connection import get_db_connection
from activops.db.types import CursorProtocol
from activops.utils.config import JSON_DIR
from activops.utils.logger import LoggerProtocol, ensure_logger


def parse_trakt_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    # formats fréquents : "YYYY-mm-ddTHH:MM:SS.000Z" ou "YYYY-mm-ddTHH:MM:SSZ"
    try:
        if date_str.endswith("Z"):
            # normaliser en +00:00 pour fromisoformat
            ds = date_str.replace("Z", "+00:00").replace(".000+00:00", "+00:00")
            dt = datetime.fromisoformat(ds)
            # DB naive en UTC
            return dt.astimezone(UTC).replace(tzinfo=None)
        # ISO sans Z
        return datetime.fromisoformat(date_str)
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.000Z")
        except ValueError:
            return None


def insert_entry(cursor: CursorProtocol, entry: dict[str, Any], entry_type: str) -> int:
    """
    Insère ou met à jour une entrée dans trakt_watched_test.
    """
    watched_date = parse_trakt_date(entry.get("watched_at"))

    if entry_type == "movie":
        movie = entry["movie"]
        cursor.execute(
            """
            INSERT INTO trakt_watched_test
              (type, title, prod_date, episode_title, num_season, num_episode,
               imdb_id, tmdb_id, watched_date, rating, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
              rating=VALUES(rating),
              watched_date=VALUES(watched_date),
              last_updated=NOW();
            """,
            (
                "movie",
                movie.get("title"),
                movie.get("year"),
                None,
                None,
                None,
                (movie.get("ids") or {}).get("imdb"),
                (movie.get("ids") or {}).get("tmdb"),
                watched_date,
                entry.get("rating"),
            ),
        )

    elif entry_type == "show":
        show = entry["show"]
        ep = entry["episode"]
        cursor.execute(
            """
            INSERT INTO trakt_watched_test
              (type, title, prod_date, episode_title, num_season, num_episode,
               imdb_id, tmdb_id, watched_date, rating, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON DUPLICATE KEY UPDATE
              rating=VALUES(rating),
              watched_date=VALUES(watched_date),
              last_updated=NOW();
            """,
            (
                "show",
                show.get("title"),
                show.get("year"),
                ep.get("title"),
                ep.get("season"),
                ep.get("number"),
                (show.get("ids") or {}).get("imdb"),
                (show.get("ids") or {}).get("tmdb"),
                watched_date,
                entry.get("rating"),
            ),
        )

    return cursor.rowcount


def load_and_merge(history_path: Path, ratings_path: Path, entry_type: str) -> list[dict[str, Any]]:
    """
    Fusionne un JSON d'historique et un JSON de notes pour produire une liste d'entrées enrichies.

    entry_type: "movie" | "show"
    """
    history: list[dict[str, Any]] = []
    ratings: list[dict[str, Any]] = []

    if history_path.exists():
        history = json.loads(history_path.read_text(encoding="utf-8"))
    if ratings_path.exists():
        ratings = json.loads(ratings_path.read_text(encoding="utf-8"))

    if entry_type == "movie":
        ratings_map = {
            (r["movie"]["ids"].get("tmdb") or r["movie"]["ids"].get("imdb")): r.get("rating") for r in ratings
        }
        for entry in history:
            key = entry["movie"]["ids"].get("tmdb") or entry["movie"]["ids"].get("imdb")
            entry["rating"] = ratings_map.get(key)
    else:  # show
        ratings_map = {
            (
                r["show"]["ids"].get("tmdb"),
                r["episode"]["season"],
                r["episode"]["number"],
            ): r.get("rating")
            for r in ratings
        }
        for entry in history:
            key = (
                entry["show"]["ids"].get("tmdb"),
                entry["episode"]["season"],
                entry["episode"]["number"],
            )
            entry["rating"] = ratings_map.get(key)

    return history


def import_all(mode: str = "normal", debug: bool = False, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("Connexion DB indisponible")
        return
    cursor = conn.cursor()

    inserted_movies = updated_movies = 0
    inserted_shows = updated_shows = 0

    # Movies
    movies = load_and_merge(JSON_DIR / "history_movies.json", JSON_DIR / "ratings_movies.json", "movie")

    if mode == "complet":
        ratings_movies = (
            json.loads((JSON_DIR / "ratings_movies.json").read_text(encoding="utf-8"))
            if (JSON_DIR / "ratings_movies.json").exists()
            else []
        )
        ratings_map = {
            (r["movie"]["ids"].get("tmdb") or r["movie"]["ids"].get("imdb")): r.get("rating") for r in ratings_movies
        }
        if (JSON_DIR / "watched_movies.json").exists():
            watched_movies = json.loads((JSON_DIR / "watched_movies.json").read_text(encoding="utf-8"))
            for wm in watched_movies:
                tmdb_id = wm["movie"]["ids"].get("tmdb") or wm["movie"]["ids"].get("imdb")
                entry = {
                    "watched_at": wm.get("last_watched_at"),
                    "rating": ratings_map.get(tmdb_id),
                    "movie": wm["movie"],
                }
                movies.append(entry)

    for entry in movies:
        rc = insert_entry(cursor, entry, "movie")
        if rc == 1:
            inserted_movies += 1
        elif rc == 2:
            updated_movies += 1
        if debug:
            logger.debug("🎬 %s → %s", entry["movie"]["title"], "Ajouté" if rc == 1 else "Mis à jour")

    # Shows
    shows = load_and_merge(JSON_DIR / "history_shows.json", JSON_DIR / "ratings_episodes.json", "show")
    for entry in shows:
        rc = insert_entry(cursor, entry, "show")
        if rc == 1:
            inserted_shows += 1
        elif rc == 2:
            updated_shows += 1
        if debug:
            ep = entry["episode"]
            logger.debug(
                "📺 %s S%sE%s → %s",
                entry["show"]["title"],
                ep["season"],
                ep["number"],
                "Ajouté" if rc == 1 else "Mis à jour",
            )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("✅ Import JSON → MariaDB terminé")
    logger.info("🎬 Movies : %s ajoutés / %s mis à jour", inserted_movies, updated_movies)
    logger.info("📺 Shows  : %s ajoutés / %s mis à jour", inserted_shows, updated_shows)
    logger.info(
        "📊 Total  : %s ajoutés / %s mis à jour", inserted_movies + inserted_shows, updated_movies + updated_shows
    )
