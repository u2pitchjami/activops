import argparse
import glob
import json
import os

from activops.db.db_connection import get_db_connection
import requests
from activops.utils.config import LISTENBRAINZ_USER
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger, get_logger

# Initialisation
script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("Listenbrainz_import")

@with_child_logger
def get_listens_from_api(logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    try:
        url = (
            f"https://api.listenbrainz.org/1/user/{LISTENBRAINZ_USER}/listens?count=50"
        )
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("payload", {}).get("listens", [])
    except requests.RequestException as e:
        logger.error(f"Erreur API ListenBrainz: {e}")
        return []

@with_child_logger
def get_listens_from_json(folder, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    listens = []
    pattern = os.path.join(folder, "**", "*.json*")  # ** = récursif
    for file in glob.iglob(pattern, recursive=True):
        try:
            with open(file, encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if "track_metadata" in data:
                            listens.append(data)
                        elif "payload" in data:
                            listens.extend(data.get("payload", {}).get("listens", []))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Ligne mal formée dans {file}: {e}")
        except Exception as e:
            logger.warning(f"Erreur lecture {file} : {e}")
    return listens


def determine_scrobble_type(artist_mbid, client, service, album):
    if artist_mbid:
        return "music"
    if client == "Web Scrobbler" and service == "YouTube":
        return "video"
    if (client == "Web Scrobbler" and service == "Radio France") or (
        client == "Pano Scrobbler" and not artist_mbid and album
    ):
        return "podcast"
    return "unknown"

@with_child_logger
def insert_listens(listens, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    try:
        conn = get_db_connection(logger=logger)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO listenbrainz_tracks
            (recording_msid, artist, artist_mbid, title, album, album_mbid,\
                track_mbid, service, client, played_at, scrobble_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s), %s)
            ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP
        """

        count = 0
        for entry in listens:
            meta = entry.get("track_metadata", {})
            msid = meta.get("recording_msid") or meta.get("additional_info", {}).get(
                "recording_msid"
            )
            # print(f"  recording_msid: {msid}")
            mbids = meta.get("mbid_mapping") or {}
            info = meta.get("additional_info", {})

            values = (
                msid,
                meta.get("artist_name"),
                mbids.get("artist_mbids", [None])[0],
                meta.get("track_name"),
                meta.get("release_name"),
                mbids.get("release_mbid"),
                mbids.get("recording_mbid"),
                info.get("music_service_name"),
                info.get("submission_client"),
                entry.get("listened_at"),
                determine_scrobble_type(
                    mbids.get("artist_mbids", [None])[0],
                    info.get("submission_client"),
                    info.get("music_service_name"),
                    meta.get("release_name"),
                ),
            )
            try:
                cursor.execute(insert_query, values)
                count += 1
            except Exception as err:
                logger.warning(f"Échec insertion ligne: {err}")

        conn.commit()
        logger.info(f"{count} scrobbles insérés ou mis à jour.")
        cursor.close()
        conn.close()

    except Exception as err:
        logger.error(f"Erreur connexion DB: {err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["api", "json"], required=True)
    parser.add_argument("--folder", help="Répertoire des JSON si source = json")
    args = parser.parse_args()

    if args.source == "api":
        logger.info("Source: API ListenBrainz")
        listens = get_listens_from_api(logger=logger)
    elif args.source == "json":
        if not args.folder:
            logger.error("Le paramètre --folder est requis avec --source json")
            exit(1)
        logger.info(f"Source: Fichiers JSON dans {args.folder}")
        listens = get_listens_from_json(args.folder, logger=logger)

    if listens:
        insert_listens(listens, logger=logger)
    else:
        logger.warning("Aucune écoute à importer.")
