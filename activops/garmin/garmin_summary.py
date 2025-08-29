from datetime import datetime, timedelta, timezone

import pytz

from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger
from activops.db.db_connection import get_db_connection

LOCAL_TZ = pytz.timezone("Europe/Paris")


@with_child_logger
def convert_utc_to_local(utc_time_str, logger: LoggerProtocol | None = None):
    """
    Convertit un timestamp UTC (GMT) en heure locale.
    """
    logger = ensure_logger(logger, __name__)
    try:
        if "." in utc_time_str:  # 🔥 Vérifie si on a des millisecondes
            utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S")

        logger.info(f"🔍 Tentative de conversion : {utc_time_str}")
        utc_time = utc_time.replace(tzinfo=timezone.utc)
        local_time = utc_time.astimezone(pytz.timezone("Europe/Paris"))
        return local_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )  # 🔥 Retourne une string bien formatée

    except ValueError as e:
        logger.error(f"❌ Erreur de conversion du timestamp : {utc_time_str} → {e}")
        return None

@with_child_logger
def get_last_recorded_date(logger: LoggerProtocol | None = None):
    """
    Récupère la dernière date enregistrée dans la base.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if not conn:
        return None
    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT date FROM garmin_summary ORDER BY date DESC LIMIT 1;"
        cursor.execute(query)
        result = cursor.fetchone()
        conn.close()
        return result["date"] if result else None
    except Exception as err:
        logger.error(f"Erreur récupération dernière date en base: {err}")
        return None


def get_days_to_update(last_sync_time):
    """
    Détermine quels jours doivent être mis à jour en fonction de la dernière synchro Garmin.
    """
    last_recorded_date = get_last_recorded_date()
    if not last_recorded_date:
        return []  # 🔥 Si aucune donnée en base, pas de mise à jour

    if isinstance(last_recorded_date, str):
        last_recorded_date = datetime.strptime(last_recorded_date, "%Y-%m-%d").date()

    last_sync_date = last_sync_time.date()

    # 🔥 Génère une liste des jours entre la dernière synchro Garmin et le dernier enregistrement en base
    days_to_update = []
    while last_recorded_date <= last_sync_date:
        days_to_update.append(last_recorded_date.strftime("%Y-%m-%d"))
        last_recorded_date += timedelta(days=1)

    return days_to_update

@with_child_logger
def fetch_summary(client, date_to_check=None, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    if date_to_check is None:
        date_to_check = datetime.now().strftime("%Y-%m-%d")
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        data = client.get_user_summary(date_to_check)
        logger.info(
            f"🕒 Données brutes lastSyncTimestampGMT : {data.get('lastSyncTimestampGMT')}"
        )
        raw_last_sync = data.get("lastSyncTimestampGMT", None)
        logger.info(f"🔍 lastSyncTimestampGMT brut avant conversion : {raw_last_sync}")

        if raw_last_sync is not None:
            last_sync = convert_utc_to_local(raw_last_sync)
        else:
            last_sync = None

        if last_sync is None and date_to_check == today:
            logger.warning(
                f"⚠️ Aucune synchro détectée pour aujourd'hui ({date_to_check}), on attend."
            )
            return None  # On stoppe uniquement si c'est aujourd'hui

        weight_data = client.get_daily_weigh_ins(date_to_check)
        # print("weight_data :", weight_data)  # Debug

        if (
            weight_data
            and isinstance(weight_data, list)
            and len(weight_data) > 0
            and "weight" in weight_data[0]
        ):
            weight = weight_data[0]["weight"]
        else:
            weight = None  # 🔥 Si la donnée n'existe pas ou est invalide, on met None

        # print("weight :", weight)  # Debug
        avg_hr = fetch_average_heart_rate(date_to_check) or 0
        # print ("avg_hr : %s", avg_hr)

        return {
            "date": date_to_check,
            "calories": data.get("totalKilocalories", 0),
            "steps": data.get("totalSteps", 0),
            "stress": data.get("averageStressLevel", 0),
            "intense_minutes": data.get("moderateIntensityMinutes", 0)
            + data.get("vigorousIntensityMinutes", 0),
            "sleep": data.get("sleepingSeconds", 0) / 3600,
            "weight": weight,
            "average_heart_rate": avg_hr,
            "last_sync": last_sync,
            "last_updated": last_updated,
        }
    except Exception as e:
        logger.error(f"Erreur récupération summary: {e}")
        return None

@with_child_logger
def fetch_average_heart_rate(date_to_check=None, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        query = """
        SELECT AVG(avg_heart_rate) FROM garmin_heart_rate WHERE date = %s;
        """
        if date_to_check is None:
            date_to_check = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"🔍 Recherche FC moyenne pour {date_to_check}")  # Debug

        cursor.execute(query, (date_to_check,))
        result = cursor.fetchone()

        if result and result[0] is not None:
            avg_hr = round(result[0])
        else:
            logger.warning(
                f"⚠️ Aucune donnée de fréquence cardiaque trouvée pour {date_to_check}"
            )
            avg_hr = None  # Évite une erreur si pas de données

        conn.close()
        return avg_hr
    except Exception as err:
        logger.error(f"Erreur récupération FC moyenne: {err}")
        return None

@with_child_logger
def update_summary_db(summary_data, logger: LoggerProtocol | None = None):
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if not conn:
        return
    try:
        cursor = conn.cursor()
        query = """
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
        cursor.execute(query)
        conn.commit()

        query = """
        INSERT INTO garmin_summary\
            (date, calories, steps, stress, intense_minutes, sleep, weight, average_heart_rate, last_sync, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        calories=VALUES(calories), steps=VALUES(steps), stress=VALUES(stress),
        intense_minutes=VALUES(intense_minutes), sleep=VALUES(sleep),
        weight=VALUES(weight), average_heart_rate=VALUES(average_heart_rate),
        last_sync=VALUES(last_sync), last_updated=VALUES(last_updated);
        """
        cursor.execute(
            query,
            (
                summary_data["date"],
                summary_data["calories"],
                summary_data["steps"],
                summary_data["stress"],
                summary_data["intense_minutes"],
                summary_data["sleep"],
                summary_data["weight"],
                summary_data["average_heart_rate"],
                summary_data["last_sync"],
                summary_data["last_updated"],
            ),
        )
        conn.commit()
        cursor.close()
        logger.info(f"✅ Données summary mises à jour pour {summary_data['date']}")
    except Exception as err:
        logger.error(f"Erreur mise à jour summary: {err}")
    finally:
        conn.close()
