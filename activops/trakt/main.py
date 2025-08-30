from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import tarfile

from activops.trakt.import_to_db import import_all
from activops.trakt.import_watchlist import import_watchlist, sync_watchlist_with_watched
from activops.trakt.trakt_client import TraktClient
from activops.utils.config import BACKUP_DIR, JSON_DIR
from activops.utils.logger import get_logger
from activops.utils.safe_runner import safe_main

logger = get_logger("Trakt Import")


def archive_backup(backup_dir: Path) -> None:
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_file = backup_dir / f"trakt_backup_{datetime.now().date()}.tar.gz"
    with tarfile.open(backup_file, "w:gz") as tar:
        for f in JSON_DIR.glob("*.json"):
            tar.add(f, arcname=f.name)
    logger.info("📦 Sauvegarde compressée : %s", backup_file)


@safe_main
def main(mode: str = "normal", archive: bool = False, debug: bool = False) -> None:
    client = TraktClient()

    if mode == "complet":
        logger.info("🔄 Mode complet : récupération de tout l'historique…")
        endpoints = {
            "/users/me/history/movies": "history_movies.json",
            "/users/me/watched/movies": "watched_movies.json",
            "/users/me/history/shows": "history_shows.json",
            "/users/me/watched/shows": "watched_shows.json",
            "/users/me/ratings/movies": "ratings_movies.json",
            "/users/me/ratings/episodes": "ratings_episodes.json",
            "/users/me/watchlist/movies": "watchlist_movies.json",
            "/users/me/watchlist/shows": "watchlist_shows.json",
        }
    else:
        logger.info("⏩ Mode normal : récupération des derniers éléments…")
        endpoints = {
            "/users/me/history/movies": "history_movies.json",
            "/users/me/history/shows": "history_shows.json",
            "/users/me/watchlist/movies": "watchlist_movies.json",
            "/users/me/watchlist/shows": "watchlist_shows.json",
        }

    for endpoint, filename in endpoints.items():
        client.backup_endpoint(endpoint, filename)

    import_all(mode=mode, debug=debug, logger=logger)
    import_watchlist(debug=debug, logger=logger)
    sync_watchlist_with_watched(debug=debug, logger=logger)

    if mode == "complet" and archive:
        archive_backup(Path(BACKUP_DIR))

    logger.info("✅ Import terminé en mode %s", mode)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Importer vos données Trakt dans MariaDB")
    parser.add_argument("mode", choices=["normal", "complet"], default="normal", nargs="?")
    parser.add_argument("--archive", action="store_true", help="Compresser les JSON (mode complet)")
    parser.add_argument("--debug", action="store_true", help="Logs détaillés")
    args = parser.parse_args()
    main(mode=args.mode, archive=args.archive, debug=args.debug)
