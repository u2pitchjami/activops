from __future__ import annotations

from typing import cast

from garminconnect import Garmin

from activops.garmin.models import GarminClientProtocol
from activops.utils.config import EMAIL, PASSWORD
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def get_garmin_client(logger: LoggerProtocol | None = None) -> GarminClientProtocol | None:
    logger = ensure_logger(logger, __name__)
    try:
        client = Garmin(EMAIL, PASSWORD)
        client.login()
        return cast(GarminClientProtocol, client)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Erreur connexion Garmin: %s", e)
        return None
