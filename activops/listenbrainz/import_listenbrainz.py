from __future__ import annotations

import argparse
import glob
import json
import os
from pathlib import Path
from typing import cast

import requests

from activops.db.db_connection import get_db_connection
from activops.listenbrainz.models import (
    AdditionalInfo,
    JsonObj,
    ListenEntry,
    Mbids,
    PayloadEnvelope,
    ScrobbleRow,
    TrackMetadata,
)
from activops.utils.config import LISTENBRAINZ_USER
from activops.utils.logger import LoggerProtocol, ensure_logger, get_logger, with_child_logger

# ---------------------------------------------------------------------------
# Logger global
# ---------------------------------------------------------------------------

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("Listenbrainz_import")


# ---------------------------------------------------------------------------
# Utils de normalisation / extraction
# ---------------------------------------------------------------------------


def _as_listens(obj: JsonObj) -> list[ListenEntry]:
    """Normalise une ligne JSON: soit un ListenEntry unique, soit un payload batch."""
    if "track_metadata" in obj:
        return [cast(ListenEntry, obj)]
    if "payload" in obj:
        payload = cast(PayloadEnvelope, obj).get("payload", {}) or {}
        return payload.get("listens", []) or []
    return []


def determine_scrobble_type(artist_mbid: str | None, client: str | None, service: str | None, album: str | None) -> str:
    if artist_mbid:
        return "music"
    if client == "Web Scrobbler" and service == "YouTube":
        return "video"
    if (client == "Web Scrobbler" and service == "Radio France") or (
        client == "Pano Scrobbler" and not artist_mbid and album
    ):
        return "podcast"
    return "unknown"


def _entry_to_scrobble_row(entry: ListenEntry) -> ScrobbleRow:
    meta: TrackMetadata = entry.get("track_metadata", {}) or {}
    info: AdditionalInfo = meta.get("additional_info", {}) or {}
    mbids: Mbids = (meta.get("mbid_mapping") or {}) or {}

    # msid peut se trouver au niveau meta OU dans additional_info
    msid: str | None = meta.get("recording_msid") or info.get("recording_msid")

    artist_mbids = mbids.get("artist_mbids", []) or []
    artist_mbid = artist_mbids[0] if artist_mbids else None

    return ScrobbleRow(
        recording_msid=msid,
        artist=meta.get("artist_name"),
        artist_mbid=artist_mbid,
        title=meta.get("track_name"),
        album=meta.get("release_name"),
        album_mbid=mbids.get("release_mbid"),
        track_mbid=mbids.get("recording_mbid"),
        service=info.get("music_service_name"),
        client=info.get("submission_client"),
        listened_at=entry.get("listened_at"),
        scrobble_type=determine_scrobble_type(
            artist_mbid=artist_mbid,
            client=info.get("submission_client"),
            service=info.get("music_service_name"),
            album=meta.get("release_name"),
        ),
    )


# ---------------------------------------------------------------------------
# Sources: API & JSONL
# ---------------------------------------------------------------------------


@with_child_logger
def get_listens_from_api(logger: LoggerProtocol | None = None) -> list[ListenEntry]:
    logger = ensure_logger(logger, __name__)
    try:
        url = f"https://api.listenbrainz.org/1/user/{LISTENBRAINZ_USER}/listens?count=50"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data: JsonObj = response.json()
        payload: JsonObj = data.get("payload", {}) or {}
        listens: list[ListenEntry] = cast(list[ListenEntry], payload.get("listens", []) or [])
        return listens
    except requests.RequestException as err:
        logger.error("Erreur API ListenBrainz: %s", err)
        return []


@with_child_logger
def get_listens_from_json(folder: Path | str, logger: LoggerProtocol | None = None) -> list[ListenEntry]:
    logger = ensure_logger(logger, __name__)
    listens: list[ListenEntry] = []
    pattern = os.path.join(str(folder), "**", "*.json*")  # récursif

    for file in glob.iglob(pattern, recursive=True):
        try:
            with open(file, encoding="utf-8") as handle:
                for lineno, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = cast(JsonObj, json.loads(line))
                    except json.JSONDecodeError as exc:
                        logger.warning("Ligne mal formée dans %s:%s → %s", file, lineno, exc)
                        continue
                    listens.extend(_as_listens(obj))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Erreur lecture %s : %s", file, exc)

    return listens


# ---------------------------------------------------------------------------
# Insertion DB
# ---------------------------------------------------------------------------


@with_child_logger
def insert_listens(listens: list[ListenEntry], logger: LoggerProtocol | None = None) -> None:
    """
    Transforme les ListenEntry en ScrobbleRow, puis INSERT via executemany.
    """
    logger = ensure_logger(logger, __name__)
    # 🔁 convertit chaque entrée API -> ligne DB
    rows: list[ScrobbleRow] = [_entry_to_scrobble_row(e) for e in listens]

    insert_query = """
        INSERT INTO listenbrainz_tracks
          (recording_msid, artist, artist_mbid, title, album, album_mbid,
           track_mbid, service, client, played_at, scrobble_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s), %s)
        ON DUPLICATE KEY UPDATE last_updated = CURRENT_TIMESTAMP
    """

    try:
        conn = get_db_connection(logger=logger)
        if conn is None:
            logger.error("Connexion DB indisponible")
            return

        with (
            conn.cursor() as cur
        ):  # si mypy râle, on peut typer:  cur: CursorProtocol = conn.cursor()  # type: ignore[assignment]
            params = [
                (
                    r.get("recording_msid"),
                    r.get("artist"),
                    r.get("artist_mbid"),
                    r.get("title"),
                    r.get("album"),
                    r.get("album_mbid"),
                    r.get("track_mbid"),
                    r.get("service"),
                    r.get("client"),
                    int(val) if (val := r.get("listened_at")) is not None else None,
                    r.get("scrobble_type"),
                )
                for r in rows
            ]

            if params:
                cur.executemany(insert_query, params)
                conn.commit()
                logger.info("%s scrobbles insérés ou mis à jour.", len(params))
            else:
                logger.info("Aucune donnée à insérer.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Erreur connexion/insert DB: %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["api", "json"], required=True)
    parser.add_argument("--folder", help="Répertoire des JSON si source = json")
    args = parser.parse_args()

    if args.source == "api":
        logger.info("Source: API ListenBrainz")
        listens = get_listens_from_api(logger=logger)
    else:  # json
        if not args.folder:
            logger.error("Le paramètre --folder est requis avec --source json")
            return 1
        logger.info("Source: Fichiers JSON dans %s", args.folder)
        listens = get_listens_from_json(Path(args.folder), logger=logger)

    if listens:
        insert_listens(listens, logger=logger)
        return 0

    logger.warning("Aucune écoute à importer.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
