import os

from garminconnect import Garmin
from activops.utils.config import EMAIL, PASSWORD
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

@with_child_logger
def get_garmin_client(logger: LoggerProtocol | None = None):

    logger = ensure_logger(logger, __name__)
    try:
        client = Garmin(EMAIL, PASSWORD)
        client.login()
        return client
    except Exception as e:
        logger.error(f"Erreur connexion Garmin: {e}")
        return None
