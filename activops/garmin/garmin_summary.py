from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytz

from activops.db.db_connection import get_db_connection
from activops.db.types import CursorProtocol
from activops.garmin.models import GarminClientProtocol, SummaryData
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger

LOCAL_TZ = pytz.timezone("Europe/Paris")


@with_child_logger
def convert_utc_to_local(utc_time_str: str, logger: LoggerProtocol | None = None) -> str | None:
    """
    Convertit un timestamp UTC (sans suffixe Z) en heure locale "YYYY-mm-dd HH:MM:SS".

    Ex: "2025-08-29T07:12:45.123" ou "2025-08-29T07:12:45"
    """
    logger = ensure_logger(logger, __name__)
    try:
        fmt = "%Y-%m-%dT%H:%M:%S.%f" if "." in utc_time_str else "%Y-%m-%dT%H:%M:%S"
        utc_time = datetime.strptime(utc_time_str, fmt)
        logger.info("🔍 Tentative de conversion : %s", utc_time_str)
        utc_time = utc_time.replace(tzinfo=UTC)
        local_time = utc_time.astimezone(LOCAL_TZ)
        return local_time.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logger.error("❌ Erreur de conversion du timestamp : %s → %s", utc_time_str, e)
        return None


@with_child_logger
def get_last_recorded_date(logger: LoggerProtocol | None = None) -> str | None:
    """
    Retourne la dernière date (YYYY-mm-dd) enregistrée en base.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        return None
    try:
        cursor: CursorProtocol = conn.cursor(dictionary=True)
        cursor.execute("SELECT date FROM garmin_summary ORDER BY date DESC LIMIT 1;")
        row = cursor.fetchone()
        return row["date"] if row else None
    except Exception as err:  # pylint: disable=broad-except
        logger.error("Erreur récupération dernière date en base: %s", err)
        return None
    finally:
        conn.close()


def get_days_to_update(last_sync_time: datetime) -> list[str]:
    """
    Jours à mettre à jour: de la dernière date en base jusqu'à la date de last_sync (inclus).
    """
    last_recorded = get_last_recorded_date()
    if not last_recorded:
        return []
    if isinstance(last_recorded, str):
        last_recorded_date = datetime.strptime(last_recorded, "%Y-%m-%d").date()
    else:
        # par sécurité, si un driver renvoie déjà un date/datetime
        last_recorded_date = getattr(last_recorded, "date", lambda: last_recorded)()

    last_sync_date = last_sync_time.date()

    days: list[str] = []
    day = last_recorded_date
    while day <= last_sync_date:
        days.append(day.strftime("%Y-%m-%d"))
        day += timedelta(days=1)
    return days


@with_child_logger
def fetch_average_heart_rate(date_to_check: str | None = None, logger: LoggerProtocol | None = None) -> int | None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        return None
    try:
        cursor: CursorProtocol = conn.cursor()
        if date_to_check is None:
            date_to_check = datetime.now().strftime("%Y-%m-%d")
        cursor.execute("SELECT AVG(avg_heart_rate) FROM garmin_heart_rate WHERE date = %s;", (date_to_check,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            val = cast(float, result[0])
            return round(float(val))
        logger.warning("⚠️ Aucune FC moyenne trouvée pour %s", date_to_check)
        return None
    except Exception as err:  # pylint: disable=broad-except
        logger.error("Erreur récupération FC moyenne: %s", err)
        return None
    finally:
        conn.close()


@with_child_logger
def fetch_summary(
    client: GarminClientProtocol, date_to_check: str | None = None, logger: LoggerProtocol | None = None
) -> SummaryData | None:
    logger = ensure_logger(logger, __name__)
    date_str = date_to_check or datetime.now().strftime("%Y-%m-%d")
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        data = client.get_user_summary(date_str)
        raw_last_sync = data.get("lastSyncTimestampGMT")
        logger.info("🕒 lastSyncTimestampGMT brut : %s", raw_last_sync)
        last_sync = convert_utc_to_local(raw_last_sync) if raw_last_sync is not None else None

        if last_sync is None and date_str == today:
            logger.warning("⚠️ Aucune synchro détectée pour aujourd'hui (%s), on attend.", date_str)
            return None

        weight: float | None
        weight_data = client.get_daily_weigh_ins(date_str)
        if weight_data and isinstance(weight_data, list) and len(weight_data) > 0 and "weight" in weight_data[0]:
            weight = weight_data[0]["weight"]
        else:
            weight = None

        avg_hr = fetch_average_heart_rate(date_str, logger=logger)

        return {
            "date": date_str,
            "calories": int(data.get("totalKilocalories", 0) or 0),
            "steps": int(data.get("totalSteps", 0) or 0),
            "stress": int(data.get("averageStressLevel", 0) or 0),
            "intense_minutes": int(data.get("moderateIntensityMinutes", 0) or 0)
            + int(data.get("vigorousIntensityMinutes", 0) or 0),
            "sleep": (data.get("sleepingSeconds", 0) or 0) / 3600,
            "weight": weight,
            "average_heart_rate": avg_hr,
            "last_sync": last_sync,
            "last_updated": last_updated,
        }
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Erreur récupération summary: %s", e)
        return None


@with_child_logger
def update_summary_db(summary_data: SummaryData, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        return
    try:
        cursor: CursorProtocol = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS garmin_summary (
              date DATE PRIMARY KEY,
              calories INT,
              steps INT,
              stress INT,
              intense_minutes INT,
              sleep FLOAT,
              weight FLOAT,
              average_heart_rate INT,
              last_sync TIMESTAMP NULL,
              last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
        )
        conn.commit()

        cursor.execute(
            """
            INSERT INTO garmin_summary
              (date, calories, steps, stress, intense_minutes, sleep, weight,
               average_heart_rate, last_sync, last_updated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              calories=VALUES(calories), steps=VALUES(steps), stress=VALUES(stress),
              intense_minutes=VALUES(intense_minutes), sleep=VALUES(sleep),
              weight=VALUES(weight), average_heart_rate=VALUES(average_heart_rate),
              last_sync=VALUES(last_sync), last_updated=VALUES(last_updated);
            """,
            (
                summary_data.get("date"),
                summary_data.get("calories"),
                summary_data.get("steps"),
                summary_data.get("stress"),
                summary_data.get("intense_minutes"),
                summary_data.get("sleep"),
                summary_data.get("weight"),
                summary_data.get("average_heart_rate"),
                summary_data.get("last_sync"),
                summary_data.get("last_updated"),
            ),
        )
        conn.commit()
        logger.info("✅ Données summary mises à jour pour %s", summary_data.get("date"))
    except Exception as err:  # pylint: disable=broad-except
        logger.error("Erreur mise à jour summary: %s", err)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()
