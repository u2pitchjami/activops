import argparse
import os
import tarfile
from datetime import datetime
from pathlib import Path

from activops.trakt.import_to_db import import_all
from activops.trakt.import_watchlist import import_watchlist, sync_watchlist_with_watched

from trakt_client import TraktClient
from activops.utils.safe_runner import safe_main

from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger, get_logger
from activops.utils.config import JSON_DIR, BACKUP_DIR

logger = get_logger("Trakt Import")

def archive_backup(backup_dir: Path):
    backup_file = backup_dir / f"trakt_backup_{datetime.now().date()}.tar.gz"
    with tarfile.open(backup_file, "w:gz") as tar:
        for f in JSON_DIR.glob("*.json"):
            tar.add(f, arcname=f.name)
    logger.info(f"📦 Sauvegarde compressée : {backup_file}")


@safe_main
def main(mode="normal", archive=False, debug=False):
    client = TraktClient()

    if mode == "complet":
        logger.info("🔄 Mode complet : récupération de tout l'historique...")
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
        logger.info("⏩ Mode normal : récupération des derniers éléments...")
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

        backup_dir = Path(BACKUP_DIR)
        archive_backup(backup_dir)

    logger.info(f"✅ Import terminé en mode {args.mode}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Importer vos données Trakt dans MariaDB"
    )
    parser.add_argument(
        "mode",
        choices=["normal", "complet"],
        default="normal",
        nargs="?",
        help="Mode d'import : 'normal' pour les derniers éléments, 'complet' pour tout l'historique",
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Compresser les fichiers JSON récupérés (uniquement en mode complet)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Activer les logs détaillés pour voir les insertions/mises à jour",
    )
    args = parser.parse_args()

    main(mode=args.mode, archive=args.archive, debug=args.debug)
