import os
from datetime import datetime

from activops.garmin.garmin_client import get_garmin_client
from activops.utils.safe_runner import safe_main
from activops.garmin.garmin_heart_rate import get_garmin_heart_rate
from activops.garmin.garmin_summary import fetch_summary, get_days_to_update, update_summary_db

from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger, get_logger

# Chemin dynamique basé sur le script en cours
script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("garmin_import")

@safe_main
def main():
    client = get_garmin_client(logger=logger)
    if not client:
        logger.error("❌ Impossible de se connecter à Garmin, arrêt du script.")
        return

    logger.info("✅ Connexion réussie à Garmin Connect !")

    date_today = datetime.now().strftime("%Y-%m-%d")
    get_garmin_heart_rate(client=client, logger=logger)
    summary_today = fetch_summary(client=client, logger=logger)

    if not summary_today or not summary_today["last_sync"]:
        logger.warning(
            f"⏳ Aucune synchro détectée pour aujourd'hui ({date_today}), on attend."
        )
        return

    last_sync_time = datetime.strptime(summary_today["last_sync"], "%Y-%m-%d %H:%M:%S")
    days_to_update = get_days_to_update(last_sync_time)

    logger.info(f"📆 Jours à mettre à jour : {days_to_update}")

    for date_to_update in days_to_update:
        if date_to_update == date_today:
            summary = summary_today  # 🔥 Évite un appel API inutile
        else:
            get_garmin_heart_rate(client=client, date_to_check=date_to_update, logger=logger)
            summary = fetch_summary(client=client, date_to_check=date_to_update, logger=logger)

        if summary:
            update_summary_db(summary_data=summary, logger=logger)
            logger.info(f"✅ Données mises à jour pour {date_to_update}")

    logger.info("🎉 Mise à jour complète terminée.")


if __name__ == "__main__":
    main()
