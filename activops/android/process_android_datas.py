from __future__ import annotations

import os
from datetime import timedelta
from typing import Any, Optional

from activops.db.db_connection import get_db_connection, get_dict_cursor
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


script_dir = os.path.dirname(os.path.abspath(__file__))


@with_child_logger
def process_android_datas(logger: Optional[LoggerProtocol] = None) -> None:
    """Traite les données Android: agrégation -> insertion -> purge."""
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("❌ Impossible d'établir la connexion MySQL")
        return

    try:
        # ---- Phase 1: lecture + insertions ---------------------------------
        with get_dict_cursor(conn) as cursor:  # ⬅️ CURSOR DICT
            # 1) timestamps distincts
            cursor.execute(
                """
                SELECT DISTINCT execution_timestamp AS execution_timestamp
                FROM android_tmp
                ORDER BY execution_timestamp ASC;
                """
            )
            timestamps: list[dict[str, Any]] = cursor.fetchall()

            prev_execution_timestamp: Optional[Any] = None

            for row in timestamps:
                # Debug type/échantillon si besoin
                # logger.info("Row type=%s ; sample=%s", type(row), row)

                execution_timestamp = row["execution_timestamp"]

                # Déterminer period_start
                if (
                    prev_execution_timestamp is not None
                    and execution_timestamp.date() == prev_execution_timestamp.date()
                ):
                    period_start = prev_execution_timestamp
                else:
                    period_start = execution_timestamp - timedelta(minutes=10)
                    logger.info(
                        "🌙 Nouveau jour détecté, period_start initialisé à %s",
                        period_start,
                    )

                period_end = execution_timestamp

                # 2) Vérifier si déjà traité
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM android_usage WHERE timestamp = %s;",
                    (execution_timestamp,),
                )
                result = cursor.fetchone() or {"cnt": 0}
                already_done = int(result["cnt"]) > 0

                if not already_done:
                    # 3) Récupérer les entrées actives
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
                    active_entries: list[dict[str, Any]] = cursor.fetchall()

                    if active_entries:
                        # Astuce perf: executemany (optionnel)
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
                        logger.info(
                            "🔍 Aucune donnée active à insérer pour %s",
                            execution_timestamp,
                        )
                else:
                    logger.info("ℹ️ %s déjà traité, pas d'insertion.", execution_timestamp)

                prev_execution_timestamp = execution_timestamp

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur MySQL lors du traitement des données : %s", exc)
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Rollback a échoué")

    # ---- Phase 2: purge -----------------------------------------------------
    try:
        with get_dict_cursor(conn) as cursor:  # ⬅️ CURSOR DICT ICI AUSSI
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
                logger.info(
                    "🗑️ Suppression des entrées de android_tmp antérieures à %s.",
                    last_timestamp - timedelta(hours=12),
                )
            else:
                logger.info("ℹ️ Aucune donnée dans android_usage, pas de purge de android_tmp.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur lors du nettoyage de android_tmp : %s", exc)
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Rollback a échoué")
    finally:
        try:
            conn.close()
        finally:
            logger.info("🔌 Connexion MySQL fermée.")
