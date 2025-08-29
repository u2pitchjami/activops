import json
import os
from datetime import datetime

from activops.db.db_connection import get_db_connection
from activops.garmin.garmin_client import connect_db, get_garmin_client
from activops.utils.safe_runner import safe_main
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger, get_logger

# Chemin dynamique basé sur le script en cours
script_dir = os.path.dirname(os.path.abspath(__file__))

logger = get_logger("garmin_import")


def debug_activity(client):
    activities = client.get_activities(
        start=10, limit=1
    )  # Récupère 1 seule activité pour le test
    if activities:
        print("📌 Données complètes de l'activité Garmin :")
        for key, value in activities[0].items():
            print(f"{key}: {value}")

    else:
        print("⚠️ Aucune activité trouvée.")

# 📌 Récupération des activités Garmin
@with_child_logger
def fetch_activities(client, limit=10, logger: LoggerProtocol | None = None):
    """
    Récupère les dernières activités Garmin et corrige les types pour MySQL.
    """
    logger = ensure_logger(logger, __name__)
    try:
        activities = client.get_activities(start=0, limit=limit, logger=logger)
        result = []

        for activity in activities:
            # 🔥 Extraction et conversion de la date d'activité
            raw_date = activity.get("startTimeLocal", "1970-01-01 00:00:00")
            try:
                activity_date = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                activity_date = None

            activity_data = {
                "activity_id": activity.get("activityId"),
                "activity_name": activity.get("activityName", "Unknown"),
                "activity_type": activity.get("activityType", {}).get(
                    "typeKey", "Unknown"
                ),
                "startTimeLocal": activity_date,  # ✅ Ajout du champ date
                "duration": activity.get("duration", 0),
                "distance": activity.get("distance", 0),
                "calories": activity.get("calories", 0),
                "avg_heart_rate": activity.get("averageHR", None),
                "max_heart_rate": activity.get("maxHR", None),
                "steps": activity.get("steps", None),
                "elevation_gain": activity.get("elevationGain", None),
                "elevation_loss": activity.get("elevationLoss", None),
                "average_speed": activity.get("averageSpeed", None),
                "max_speed": activity.get("maxSpeed", None),
                "moving_time": activity.get("movingDuration", None),  # 🔄 Renommé
                "elapsed_time": activity.get("elapsedDuration", None),  # 🔄 Renommé
                "averageRunningCadence": activity.get(
                    "averageRunningCadenceInStepsPerMinute", None
                ),  # ✅ Ajouté
                "maxRunningCadence": activity.get(
                    "maxRunningCadenceInStepsPerMinute", None
                ),  # ✅ Ajouté
                "hrTimeInZone_1": activity.get("hrTimeInZone_1", None),  # ✅ Ajouté
                "hrTimeInZone_2": activity.get("hrTimeInZone_2", None),  # ✅ Ajouté
                "hrTimeInZone_3": activity.get("hrTimeInZone_3", None),  # ✅ Ajouté
                "hrTimeInZone_4": activity.get("hrTimeInZone_4", None),  # ✅ Ajouté
                "hrTimeInZone_5": activity.get("hrTimeInZone_5", None),  # ✅ Ajouté
                "minTemperature": activity.get("minTemperature", None),  # ✅ Ajouté
                "maxTemperature": activity.get("maxTemperature", None),  # ✅ Ajouté
                "ownerId": activity.get("ownerId", None),  # ✅ Ajouté
                "ownerDisplayName": activity.get(
                    "ownerDisplayName", "Unknown"
                ),  # ✅ Ajouté
                "ownerFullName": activity.get("ownerFullName", "Unknown"),  # ✅ Ajouté
                "deviceId": activity.get("deviceId", None),  # ✅ Ajouté
                "manufacturer": activity.get("manufacturer", "Unknown"),  # ✅ Ajouté
                "startLatitude": activity.get("startLatitude", None),  # ✅ Ajouté
                "startLongitude": activity.get("startLongitude", None),  # ✅ Ajouté
                "locationName": activity.get("locationName", "Unknown"),  # ✅ Ajouté
                "json_data": json.dumps(
                    activity, ensure_ascii=False
                ),  # Stockage brut en JSON
            }
            result.append(activity_data)

        return result

    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des activités : {e}")
        return None


def clean_activity_data(activity):
    """
    Vérifie que tous les champs nécessaires sont présents et convertit `None` proprement pour MySQL.
    """
    required_fields = [
        "activity_id",
        "activity_name",
        "activity_type",
        "startTimeLocal",
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
        "json_data",
    ]

    for field in required_fields:
        if field not in activity or activity[field] is None:
            activity[field] = None  # Assurer que la valeur est `NULL` en SQL

    return activity

@with_child_logger
def save_garmin_data_to_json(activities, filename="garmin_activities.json", logger: LoggerProtocol | None = None):
    """
    Sauvegarde les données Garmin récupérées dans un fichier JSON.
    """
    logger = ensure_logger(logger, __name__)
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(activities, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Données Garmin sauvegardées dans {filename}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la sauvegarde JSON : {e}")


# 📌 Enregistrement des activités dans MySQL
@with_child_logger
def save_activities_to_db(activities, logger: LoggerProtocol | None = None):
    """
    Sauvegarde les activités Garmin dans MySQL, en insérant les nouvelles et mettant à jour les existantes.
    """
    logger = ensure_logger(logger, __name__)
    if not activities:
        logger.warning("⚠️ Aucune activité à enregistrer.")
        return

    conn = get_db_connection(logger=logger)
    if not conn:
        logger.error("❌ Impossible de se connecter à la base de données.")
        return

    try:
        cursor = conn.cursor()

        query = """
    INSERT INTO garmin_activities 
    (activity_id, activity_name, activity_type, duration, distance, calories, avg_heart_rate, max_heart_rate, steps,
    elevation_gain, elevation_loss, average_speed, max_speed, moving_time, elapsed_time, json_data, startTimeLocal,
    averageRunningCadence, maxRunningCadence, hrTimeInZone_1, hrTimeInZone_2, hrTimeInZone_3, hrTimeInZone_4,
    hrTimeInZone_5, minTemperature, maxTemperature, ownerId, ownerDisplayName, ownerFullName, deviceId, manufacturer,
    startLatitude, startLongitude, locationName)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        activity_type = VALUES(activity_type), 
        duration = VALUES(duration),
        distance = VALUES(distance),
        calories = VALUES(calories),
        avg_heart_rate = VALUES(avg_heart_rate),
        max_heart_rate = VALUES(max_heart_rate),
        steps = VALUES(steps),
        elevation_gain = VALUES(elevation_gain),
        elevation_loss = VALUES(elevation_loss),
        average_speed = VALUES(average_speed),
        max_speed = VALUES(max_speed),
        moving_time = VALUES(moving_time),
        elapsed_time = VALUES(elapsed_time),
        ownerDisplayName = VALUES(ownerDisplayName),
        ownerFullName = VALUES(ownerFullName),
        locationName = VALUES(locationName),
        json_data = VALUES(json_data);
"""  # 🔥 `timestamp` est auto-géré par MySQL, donc PAS DANS L'INSERT !

        for activity in activities:
            activity = clean_activity_data(
                activity
            )  # 🔥 Corrige les valeurs manquantes

            expected_columns = [
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
                "json_data",  # 🔥 `json_data` ici
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
            ]  # 🔥 `timestamp` est auto-géré, donc PAS DANS CETTE LISTE !

            ordered_values = [
                activity[col] for col in expected_columns
            ]  # 🔥 On aligne exactement avec MySQL

            cursor.execute(
                query, tuple(ordered_values)
            )  # 🔥 Plus aucun problème de mismatch !

        conn.commit()
        logger.info("✅ Activités enregistrées avec succès.")

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'insertion en base : {e}")

    finally:
        cursor.close()
        conn.close()


# 📌 Script principal
@safe_main
def main():
    client = get_garmin_client(logger=logger)
    if not client:
        logger.error("❌ Impossible de se connecter à Garmin, arrêt du script.")
        return

    logger.info("✅ Connexion réussie à Garmin Connect !")
    # debug_activity(client)
    activities = fetch_activities(client=client, limit=10, logger=logger)
    # save_garmin_data_to_json(activities)
    save_activities_to_db(activities, logger=logger)


if __name__ == "__main__":
    main()
