from __future__ import annotations

from datetime import datetime
import json
import os

from activops.db.db_connection import get_db_connection
from activops.db.types import CursorProtocol
from activops.garmin.garmin_client import get_garmin_client
from activops.garmin.models import ActivityRow, GarminClientProtocol
from activops.utils.logger import LoggerProtocol, ensure_logger, get_logger, with_child_logger
from activops.utils.safe_runner import safe_main

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("garmin_import")


def debug_activity(client: GarminClientProtocol) -> None:
    activities = client.get_activities(start=10, limit=1)
    if activities:
        print("📌 Données complètes de l'activité Garmin :")
        for k, v in activities[0].items():
            print(f"{k}: {v}")
    else:
        print("⚠️ Aucune activité trouvée.")


@with_child_logger
def fetch_activities(
    client: GarminClientProtocol, limit: int = 10, logger: LoggerProtocol | None = None
) -> list[ActivityRow] | None:
    """
    Récupère les dernières activités Garmin et corrige les types pour MySQL.
    """
    logger = ensure_logger(logger, __name__)
    try:
        activities = client.get_activities(start=0, limit=limit)
        result: list[ActivityRow] = []

        for activity in activities:
            raw_date = activity.get("startTimeLocal", "1970-01-01 00:00:00")
            try:
                # on garde str pour DB TIME/DATETIME
                dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S")
                start_time_local = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                start_time_local = None

            row: ActivityRow = {
                "activity_id": activity.get("activityId"),
                "activity_name": activity.get("activityName", "Unknown"),
                "activity_type": (activity.get("activityType") or {}).get("typeKey", "Unknown"),
                "startTimeLocal": start_time_local,
                "duration": activity.get("duration"),
                "distance": activity.get("distance"),
                "calories": activity.get("calories"),
                "avg_heart_rate": activity.get("averageHR"),
                "max_heart_rate": activity.get("maxHR"),
                "steps": activity.get("steps"),
                "elevation_gain": activity.get("elevationGain"),
                "elevation_loss": activity.get("elevationLoss"),
                "average_speed": activity.get("averageSpeed"),
                "max_speed": activity.get("maxSpeed"),
                "moving_time": activity.get("movingDuration"),
                "elapsed_time": activity.get("elapsedDuration"),
                "averageRunningCadence": activity.get("averageRunningCadenceInStepsPerMinute"),
                "maxRunningCadence": activity.get("maxRunningCadenceInStepsPerMinute"),
                "hrTimeInZone_1": activity.get("hrTimeInZone_1"),
                "hrTimeInZone_2": activity.get("hrTimeInZone_2"),
                "hrTimeInZone_3": activity.get("hrTimeInZone_3"),
                "hrTimeInZone_4": activity.get("hrTimeInZone_4"),
                "hrTimeInZone_5": activity.get("hrTimeInZone_5"),
                "minTemperature": activity.get("minTemperature"),
                "maxTemperature": activity.get("maxTemperature"),
                "ownerId": activity.get("ownerId"),
                "ownerDisplayName": activity.get("ownerDisplayName", "Unknown"),
                "ownerFullName": activity.get("ownerFullName", "Unknown"),
                "deviceId": activity.get("deviceId"),
                "manufacturer": activity.get("manufacturer", "Unknown"),
                "startLatitude": activity.get("startLatitude"),
                "startLongitude": activity.get("startLongitude"),
                "locationName": activity.get("locationName", "Unknown"),
                "json_data": json.dumps(activity, ensure_ascii=False),
            }
            result.append(row)

        return result
    except Exception as e:  # pylint: disable=broad-except
        logger.error("❌ Erreur lors de la récupération des activités : %s", e)
        return None


def clean_activity_data(activity: ActivityRow) -> ActivityRow:
    """
    Assure que tous les champs existent (None si absent).
    """
    # rien à faire : ActivityRow est déjà "total=False", on laisse None par défaut
    return activity


@with_child_logger
def save_garmin_data_to_json(
    activities: list[ActivityRow], filename: str = "garmin_activities.json", logger: LoggerProtocol | None = None
) -> None:
    logger = ensure_logger(logger, __name__)
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(activities, f, indent=4, ensure_ascii=False)
        logger.info("✅ Données Garmin sauvegardées dans %s", filename)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("❌ Erreur lors de la sauvegarde JSON : %s", e)


@with_child_logger
def save_activities_to_db(activities: list[ActivityRow] | None, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    if not activities:
        logger.warning("⚠️ Aucune activité à enregistrer.")
        return

    conn = get_db_connection(logger=logger)
    if conn is None:
        logger.error("❌ Impossible de se connecter à la base de données.")
        return

    try:
        cursor: CursorProtocol = conn.cursor()
        query = """
        INSERT INTO garmin_activities 
          (activity_id, activity_name, activity_type, duration, distance, calories,
           avg_heart_rate, max_heart_rate, steps, elevation_gain, elevation_loss,
           average_speed, max_speed, moving_time, elapsed_time, json_data, startTimeLocal,
           averageRunningCadence, maxRunningCadence, hrTimeInZone_1, hrTimeInZone_2,
           hrTimeInZone_3, hrTimeInZone_4, hrTimeInZone_5, minTemperature, maxTemperature,
           ownerId, ownerDisplayName, ownerFullName, deviceId, manufacturer, startLatitude,
           startLongitude, locationName)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          activity_type=VALUES(activity_type),
          duration=VALUES(duration),
          distance=VALUES(distance),
          calories=VALUES(calories),
          avg_heart_rate=VALUES(avg_heart_rate),
          max_heart_rate=VALUES(max_heart_rate),
          steps=VALUES(steps),
          elevation_gain=VALUES(elevation_gain),
          elevation_loss=VALUES(elevation_loss),
          average_speed=VALUES(average_speed),
          max_speed=VALUES(max_speed),
          moving_time=VALUES(moving_time),
          elapsed_time=VALUES(elapsed_time),
          ownerDisplayName=VALUES(ownerDisplayName),
          ownerFullName=VALUES(ownerFullName),
          locationName=VALUES(locationName),
          json_data=VALUES(json_data);
        """
        cols = [
            "activity_id",
            "activity_name",
            "activity_type",
            "duration",
            "distance",
            "calories",
            "avg_heart_rate",
            "max_heart_rate",
            "steps",
            "elevation_gain",
            "elevation_loss",
            "average_speed",
            "max_speed",
            "moving_time",
            "elapsed_time",
            "json_data",
            "startTimeLocal",
            "averageRunningCadence",
            "maxRunningCadence",
            "hrTimeInZone_1",
            "hrTimeInZone_2",
            "hrTimeInZone_3",
            "hrTimeInZone_4",
            "hrTimeInZone_5",
            "minTemperature",
            "maxTemperature",
            "ownerId",
            "ownerDisplayName",
            "ownerFullName",
            "deviceId",
            "manufacturer",
            "startLatitude",
            "startLongitude",
            "locationName",
        ]
        for activity in activities:
            a = clean_activity_data(activity)
            params = tuple(a.get(c) for c in cols)
            cursor.execute(query, params)

        conn.commit()
        logger.info("✅ Activités enregistrées avec succès.")
    except Exception as e:  # pylint: disable=broad-except
        logger.error("❌ Erreur lors de l'insertion en base : %s", e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


@safe_main
def main() -> None:
    client = get_garmin_client(logger=logger)
    if not client:
        logger.error("❌ Impossible de se connecter à Garmin, arrêt du script.")
        return
    logger.info("✅ Connexion réussie à Garmin Connect !")

    activities = fetch_activities(client=client, limit=10, logger=logger)
    save_activities_to_db(activities, logger=logger)


if __name__ == "__main__":
    main()
