from collections import defaultdict
from datetime import datetime

from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger
from activops.db.db_connection import get_db_connection


@with_child_logger
def get_garmin_heart_rate(client, date_to_check=None, logger: LoggerProtocol | None = None) -> None:
    logger = ensure_logger(logger, __name__)
    if date_to_check is None:
        date_to_check = datetime.now().strftime("%Y-%m-%d")
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    heart_rate_dict = defaultdict(list)

    try:
        # Récupération des données de fréquence cardiaque
        heart_rates = client.get_heart_rates(date_to_check).get("heartRateValues", [])

        # Regroupement par tranche de 10 minutes
        heart_rate_dict = defaultdict(list)

        for entry in heart_rates:
            timestamp_ms, heart_rate = entry
            if heart_rate is not None:  # Vérifier que la valeur est valide
                timestamp_sec = timestamp_ms // 1000  # Conversion en secondes
                time_human = datetime.fromtimestamp(timestamp_sec).strftime(
                    "%H:%M"
                )  # Format HH:MM

                # Arrondir aux 10 minutes les plus proches
                minute = int(time_human.split(":")[1])
                rounded_minute = (minute // 10) * 10
                rounded_time = f"{time_human[:3]}{rounded_minute:02}:00"

                # Ajouter la FC à la bonne plage horaire
                heart_rate_dict[rounded_time].append(heart_rate)

    except Exception as e:
        logger.error(f"Erreur récupération heart_rate: {e}")
        return None

    # 🔥 Si le dictionnaire est vide, on ne tente pas d'insérer en base
    if not heart_rate_dict:
        logger.warning(
            f"⚠️ Aucune donnée de fréquence cardiaque disponible pour {date_to_check}."
        )
        return

    # Connexion à la base de données
    conn = get_db_connection(logger=logger)
    if not conn:
        return
    try:
        cursor = conn.cursor()

        # Insérer les moyennes en base
        for time_slot, values in heart_rate_dict.items():
            avg_hr = round(sum(values) / len(values))  # Moyenne arrondie
            # print(f"🕒 {time_slot} → 💓 Moyenne FC : {avg_hr}")  # Debug avant d'insérer

            insert_query = """
            INSERT INTO garmin_heart_rate (date, time_slot, avg_heart_rate)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE avg_heart_rate=VALUES(avg_heart_rate);
            """
            cursor.execute(insert_query, (date_to_check, time_slot, avg_hr))

        conn.commit()
        logger.info("✅ Données FC moyennées sur 10 min insérées en base !")

    except Exception as err:
        logger.error(f"Erreur insertion en base: {err}")
    finally:
        conn.close()
