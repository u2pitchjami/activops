import csv
import os
from activops.utils.logger import get_logger
from activops.db.db_connection import get_db_connection

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("ListenBrainz Update from CSV")

@safe_main
def update_from_csv(csv_file):
    conn = get_db_connection()
    cursor = conn.cursor()

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cursor.execute(
                    """
                    UPDATE listenbrainz_tracks
                    SET title = %s, artist = %s, artist_mbid = %s, album = %s, album_mbid = %s, \
                        track_mbid = %s, service = %s, client = %s, scrobble_type = %s, theme = %s
                    WHERE id = %s
                """,
                    (
                        row["title"],
                        row["artist"],
                        row["artist_mbid"],
                        row["album"],
                        row["album_mbid"],
                        row["track_mbid"],
                        row["service"],
                        row["client"],
                        row["scrobble_type"],
                        row["theme"],
                        row["id"],
                    ),
                )
            except Exception as e:
                logger.error(f"Erreur ligne ID {row.get('id')} : {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Mise à jour terminée depuis %s", csv_file)

if __name__ == "__main__":
    update_from_csv("correctifs_scrobbles.csv")
