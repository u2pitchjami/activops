from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import sys

from activops.listenbrainz.models import PodcastConfig, ScrobbleRow, VideoArtistIndex, VideoConfig
from activops.listenbrainz.normalize.db import get_scrobbles_from_db, inject_normalized_scrobble
from activops.listenbrainz.normalize.load_json import (
    build_video_artist_index,
    load_chronique_config,
    load_video_config,
)
from activops.listenbrainz.normalize.podcast import enrich_podcast_scrobble, normalize_france_inter_live
from activops.listenbrainz.normalize.video import enrich_video_scrobble  # à typer pareil
from activops.utils.logger import get_logger

logger = get_logger("Listenbrainz:normalize_scrobbles")
script_dir = os.path.dirname(os.path.abspath(__file__))


def convert_datetime(obj: str | datetime) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalisation des scrobbles ListenBrainz")
    parser.add_argument("--dry-run", action="store_true", help="Mode test : n'injecte rien dans la base")
    parser.add_argument("--all", action="store_true", help="prend 24h vs tout")
    parser.add_argument("--logfile", type=str, default="not_found_scrobbles.log")
    args = parser.parse_args()

    raw_video_config: VideoConfig = load_video_config(logger=logger)
    video_artist_index: VideoArtistIndex = build_video_artist_index(raw_video_config, logger=logger)
    config: PodcastConfig = load_chronique_config(logger=logger)

    if not config or not raw_video_config:
        logger.error("Aucune configuration chargée. Abandon.")
        sys.exit(1)

    scrobbles: list[ScrobbleRow] = get_scrobbles_from_db(all=args.all, logger=logger)
    for s in scrobbles:
        s = normalize_france_inter_live(s, logger=logger)
        s = enrich_podcast_scrobble(s, config, not_found_logfile=args.logfile, logger=logger)
        s = enrich_video_scrobble(s, video_artist_index, logger=logger)  # <- à typer côté video.py

        if args.dry_run:
            print(json.dumps(s, indent=2, ensure_ascii=False, default=convert_datetime))
        else:
            inject_normalized_scrobble(s, logger=logger)
