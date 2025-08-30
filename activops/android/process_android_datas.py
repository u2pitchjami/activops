from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any

from activops.db.db_connection import get_db_connection, get_dict_cursor
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

script_dir = os.path.dirname(os.path.abspath(__file__))


@with_child_logger
def process_android_datas(logger: LoggerProtocol | None = None) -> None:
    """
    Agrège android_tmp → insère dans android_usage → purge android_tmp.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("❌ Connexion MySQL indisponible")
        return

    try:
        # ---- Phase 1: lecture + insertions ---------------------------------
        with get_dict_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT DISTINCT execution_timestamp AS execution_timestamp
                FROM android_tmp
                ORDER BY execution_timestamp ASC;
                """
            )
            timestamps: list[dict[str, Any]] = cursor.fetchall() or []
            prev_execution_timestamp: datetime | None = None

            for row in timestamps:
                execution_timestamp = row["execution_timestamp"]  # typé datetime par le driver
                if not isinstance(execution_timestamp, datetime):
                    # fallback (selon driver/paramétrage)
                    try:
                        execution_timestamp = datetime.fromisoformat(str(execution_timestamp))
                    except Exception:  # pylint: disable=broad-except
                        logger.warning("Timestamp inattendu: %r", row["execution_timestamp"])
                        continue

                if (
                    prev_execution_timestamp is not None
                    and execution_timestamp.date() == prev_execution_timestamp.date()
                ):
                    period_start = prev_execution_timestamp
                else:
                    period_start = execution_timestamp - timedelta(minutes=10)
                    logger.info("🌙 Nouveau jour, period_start=%s", period_start)

                period_end = execution_timestamp

                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM android_usage WHERE timestamp = %s;",
                    (execution_timestamp,),
                )
                result = cursor.fetchone() or {"cnt": 0}
                already_done = int(result["cnt"]) > 0

                if not already_done:
                    cursor.execute(
                        """
                        SELECT
                          machine_id,
                          package_name AS application_id,
                          last_used,
                          duration_seconds,
                          execution_timestamp
                        FROM android_tmp
                        WHERE execution_timestamp = %s
                          AND last_used BETWEEN %s AND %s;
                        """,
                        (execution_timestamp, period_start, period_end),
                    )
                    active_entries: list[dict[str, Any]] = cursor.fetchall() or []

                    if active_entries:
                        cursor.executemany(
                            """
                            INSERT INTO android_usage
                              (machine_id, application_id, last_used, duration_seconds, timestamp)
                            VALUES (%s, %s, %s, %s, %s);
                            """,
                            [
                                (
                                    e["machine_id"],
                                    e["application_id"],
                                    e["last_used"],
                                    e["duration_seconds"],
                                    e["execution_timestamp"],
                                )
                                for e in active_entries
                            ],
                        )
                        conn.commit()
                        logger.info("✅ Données insérées pour %s", execution_timestamp)
                    else:
                        logger.info("🔍 Aucune donnée active pour %s", execution_timestamp)
                else:
                    logger.info("📆 %s déjà traité, pas d'insertion.", execution_timestamp)

                prev_execution_timestamp = execution_timestamp

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur MySQL lors du traitement : %s", exc)
        try:
            conn.rollback()
        except Exception:
            logger.exception("Rollback a échoué")

    # ---- Phase 2: purge -----------------------------------------------------
    try:
        with get_dict_cursor(conn) as cursor:
            cursor.execute("SELECT MAX(timestamp) AS last_timestamp FROM android_usage;")
            result = cursor.fetchone() or {"last_timestamp": None}
            last_timestamp = result["last_timestamp"]

            if last_timestamp is not None:
                cursor.execute(
                    """
                    DELETE FROM android_tmp
                    WHERE execution_timestamp < %s - INTERVAL 12 HOUR;
                    """,
                    (last_timestamp,),
                )
                conn.commit()
                logger.info("🗑️ Purge android_tmp avant %s.", last_timestamp - timedelta(hours=12))
            else:
                logger.info("📆 Aucune donnée dans android_usage, pas de purge.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur purge android_tmp : %s", exc)
        try:
            conn.rollback()
        except Exception:
            logger.exception("Rollback a échoué")
    finally:
        try:
            conn.close()
        finally:
            logger.info("🔌 Connexion MySQL fermée.")
