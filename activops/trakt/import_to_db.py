import json
import os
from datetime import datetime
from pathlib import Path

from activops.db.db_connection import get_db_connection
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger
from activops.utils.config import JSON_DIR

def insert_watched(cursor, entry, type_):
    if type_ == "movie":
        item = entry["movie"]
        title = item.get("title", "")
        prod_date = item.get("year")
        episode_title = None
        num_season = None
        num_episode = None
        imdb_id = item["ids"].get("imdb")
        tmdb_id = item["ids"].get("tmdb")
    else:
        show = entry["show"]
        episode = entry["episode"]
        title = show.get("title", "")
        prod_date = show.get("year")
        episode_title = episode.get("title", "")
        num_season = episode.get("season")
        num_episode = episode.get("number")
        imdb_id = show["ids"].get("imdb")
        tmdb_id = show["ids"].get("tmdb")

    watched_date = parse_trakt_date(entry.get("watched_at"))
    rating = entry.get("rating")
    last_updated = datetime.now()

    cursor.execute(
        """
        INSERT INTO trakt_watched
        (type, title, prod_date, episode_title, num_season, num_episode,
         imdb_id, tmdb_id, watched_date, rating, last_updated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            rating=VALUES(rating),
            watched_date=VALUES(watched_date),
            last_updated=VALUES(last_updated);
    """,
        (
            type_,
            title,
            prod_date,
            episode_title,
            num_season,
            num_episode,
            imdb_id,
            tmdb_id,
            watched_date,
            rating,
            last_updated,
        ),
    )


def load_and_merge(history_path, ratings_path, type_):
    """
    Fusionne un JSON d'historique et un JSON de notes pour produire une liste d'entrées enrichies.

    :param history_path: Path du fichier JSON d'historique
    :param ratings_path: Path du fichier JSON des notes
    :param type_: "movie" ou "show"
    :return: liste de dicts enrichis
    """
    history = []
    ratings = []

    if history_path.exists():
        history = json.loads(history_path.read_text())
    if ratings_path.exists():
        ratings = json.loads(ratings_path.read_text())

    ratings_map = {}
    if type_ == "movie":
        ratings_map = {
            r["movie"]["ids"].get("tmdb")
            or r["movie"]["ids"].get("imdb"): r.get("rating")
            for r in ratings
        }
    elif type_ == "show":
        ratings_map = {
            (
                r["show"]["ids"].get("tmdb"),
                r["episode"]["season"],
                r["episode"]["number"],
            ): r.get("rating")
            for r in ratings
        }

    for entry in history:
        if type_ == "movie":
            key = entry["movie"]["ids"].get("tmdb") or entry["movie"]["ids"].get("imdb")
            entry["rating"] = ratings_map.get(key)
        elif type_ == "show":
            key = (
                entry["show"]["ids"].get("tmdb"),
                entry["episode"]["season"],
                entry["episode"]["number"],
            )
            entry["rating"] = ratings_map.get(key)
    return history


def parse_trakt_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.000Z")
    except ValueError:
        return None


def insert_entry(cursor, entry, entry_type):
    """
    Insère ou met à jour une entrée dans la table trakt_watched_test.
    """
    if entry_type == "movie":
        watched_date = parse_trakt_date(entry.get("watched_at"))
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
                entry["movie"]["title"],
                entry["movie"]["year"],
                None,
                None,
                None,
                entry["movie"]["ids"].get("imdb"),
                entry["movie"]["ids"].get("tmdb"),
                watched_date,
                entry.get("rating"),
            ),
        )

    elif entry_type == "show":
        watched_date = parse_trakt_date(entry.get("watched_at"))
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
                entry["show"]["title"],
                entry["show"]["year"],
                entry["episode"]["title"],
                entry["episode"]["season"],
                entry["episode"]["number"],
                entry["show"]["ids"].get("imdb"),
                entry["show"]["ids"].get("tmdb"),
                watched_date,
                entry.get("rating"),
            ),
        )

    return cursor.rowcount  # 1 = insert, 2 = update


def import_all(mode="normal", debug=False, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    cursor = conn.cursor()

    inserted_movies = updated_movies = 0
    inserted_shows = updated_shows = 0

    # --- Movies
    movies = load_and_merge(
        JSON_DIR / "history_movies.json", JSON_DIR / "ratings_movies.json", "movie"
    )

    if mode == "complet":
        ratings_movies = (
            json.loads((JSON_DIR / "ratings_movies.json").read_text())
            if (JSON_DIR / "ratings_movies.json").exists()
            else []
        )
        ratings_map = {
            r["movie"]["ids"].get("tmdb")
            or r["movie"]["ids"].get("imdb"): r.get("rating")
            for r in ratings_movies
        }
        watched_movies = json.loads((JSON_DIR / "watched_movies.json").read_text())
        for wm in watched_movies:
            tmdb_id = wm["movie"]["ids"].get("tmdb") or wm["movie"]["ids"].get("imdb")
            entry = {
                "watched_at": wm.get("last_watched_at"),
                "rating": ratings_map.get(tmdb_id),
                "movie": wm["movie"],
            }
            movies.append(entry)

    for entry in movies:
        rowcount = insert_entry(cursor, entry, "movie")
        if rowcount == 1:
            inserted_movies += 1
        elif rowcount == 2:
            updated_movies += 1
        if debug:
            logger.debug(
                f"🎬 {entry['movie']['title']} → {'Ajouté' if rowcount == 1 else 'Mis à jour'}"
            )

    # --- Shows
    shows = load_and_merge(
        JSON_DIR / "history_shows.json", JSON_DIR / "ratings_episodes.json", "show"
    )

    for entry in shows:
        rowcount = insert_entry(cursor, entry, "show")
        if rowcount == 1:
            inserted_shows += 1
        elif rowcount == 2:
            updated_shows += 1
        if debug:
            print(
                f"📺 {entry['show']['title']} S{entry['episode']['season']}E{entry['episode']['number']}\
                    → {'Ajouté' if rowcount == 1 else 'Mis à jour'}"
            )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("✅ Import JSON → MariaDB terminé")
    logger.info(f"🎬 Movies : {inserted_movies} ajoutés / {updated_movies} mis à jour")
    logger.info(f"📺 Shows : {inserted_shows} ajoutés / {updated_shows} mis à jour")
    logger.info(
        f"📊 Total : {inserted_movies + inserted_shows} ajoutés / {updated_movies + updated_shows} mis à jour"
    )
