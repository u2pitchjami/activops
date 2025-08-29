import csv
import os
import shutil
import time
from datetime import datetime

from activops.android.process_android_datas import process_android_datas
from activops.db.db_connection import get_db_connection
from activops.utils.safe_runner import safe_main
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger, get_logger
from activops.utils.config import IMPORT_DIR

# Chemin dynamique basé sur le script en cours
script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("android_import")

@with_child_logger
def get_machine_id(device_name: str, logger: LoggerProtocol | None = None) -> int | None:
    """
    Récupère le machine_id depuis la table machines en fonction du device_name.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if not conn:
        return None

    cursor = conn.cursor()
    cursor.execute(
        "SELECT machine_id FROM machines WHERE machine_name = %s", (device_name,)
    )
    result = cursor.fetchone()

    machine_id = result[0] if result else None
    if machine_id:
        logger.info(f"[✅] Machine ID trouvé pour {device_name} : {machine_id}")
    else:
        logger.info(f"[❌] Machine {device_name} non trouvée en base !")

    cursor.close()
    conn.close()
    return machine_id

@with_child_logger
def process_log_file(file_path: str, logger: LoggerProtocol | None = None) -> None:
    """
    Traite un fichier de log et insère les données dans la table temporaire.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if not conn:
        return
    cursor = conn.cursor()

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        first_row = next(reader, None)  # Lire la première ligne pour choper device_name

        if not first_row:
            logger.info(f"[⚠] Fichier vide, ignoré : {file_path}")
            return

        device_name = first_row["device_name"]  # ✅ Récupérer le device_name ici
        machine_id = get_machine_id(device_name)

        if not machine_id:
            logger.error(
                f"[❌] Impossible de récupérer machine_id pour {device_name}, fichier ignoré."
            )
            return

        # Revenir au début du fichier après la première ligne lue
        csvfile.seek(0)
        next(reader)  # Ignorer l'en-tête

        for row in reader:
            execution_timestamp = row["execution_timestamp"]
            package_name = row["package_name"]
            last_used = datetime.strptime(row["last_used"], "%Y-%m-%d %H:%M:%S")
            duration_seconds = int(row["duration_seconds"])

            try:
                cursor.execute(
                    """
                    INSERT INTO android_tmp (machine_id, execution_timestamp, package_name, last_used, duration_seconds)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                    (
                        machine_id,
                        execution_timestamp,
                        package_name,
                        last_used,
                        duration_seconds,
                    ),
                )
                conn.commit()
                logger.info(
                    f"[✅] {device_name} | {package_name} ({duration_seconds}s) inséré dans android_tmp"
                )

            except Exception as e:
                logger.error(f"[❌] Erreur MySQL : {e}")

    cursor.close()
    conn.close()

@safe_main
def scan_and_process_logs():
    """
    Scan le dossier de logs et traite tous les fichiers correspondant au pattern.
    """

    # 2️⃣ Sleep de 30 secondes pour éviter le décalage avec Android
    logger.info("⏳ Pause de 30 secondes pour attendre la fin de l'envoi Android...")
    time.sleep(30)    

    print("[📂] IMPORT_DIR", IMPORT_DIR)

    files = [
        f
        for f in os.listdir(IMPORT_DIR)
        if f.startswith("recap_android_") and f.endswith(".csv")
    ]
    if not files:
        logger.info("[📂] Aucun fichier à traiter.")
        return

    for file_name in sorted(
        files
    ):  # Trier par nom pour traiter dans l'ordre chronologique
        file_path = os.path.join(IMPORT_DIR, file_name)
        logger.info(f"[🔍] Traitement du fichier : {file_name}")
        file_date = datetime.now().strftime("%y%m%d")
        process_log_file(file_path=file_path, logger=logger)
        # Construire le chemin du dossier d’archive
        archive_subdir = os.path.join(IMPORT_DIR, file_date)
        os.makedirs(archive_subdir, exist_ok=True)  # Créer le dossier si nécessaire

        # Construire le chemin du fichier archivé
        archived_file_path = os.path.join(archive_subdir, f"{file_name}.processed")

        # Déplacer le fichier dans l'archive
        shutil.move(file_path, archived_file_path)
        logger.info(f"📂 Fichier archivé : {archived_file_path}")


if __name__ == "__main__":
    scan_and_process_logs()

    process_android_datas()
