import mysql.connector
from activops.utils.config import DB_CONFIG

from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

@with_child_logger
def get_db_connection(logger: LoggerProtocol | None = None):
    """
    Établit une connexion à MySQL en utilisant les variables d'environnement.
    """
    logger = ensure_logger(logger, __name__)
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        if logger:
            logger.error(f"❌ ERREUR de connexion à MySQL : {err}")
        return None

def get_dict_cursor(conn) -> "mysql.connector.cursor.MySQLCursorDict":
    return conn.cursor(dictionary=True)
