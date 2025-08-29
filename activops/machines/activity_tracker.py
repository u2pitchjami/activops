import json
import os
import subprocess
from datetime import datetime

from activops.utils.safe_runner import safe_main
import pytz
from activops.utils.logger import get_logger
from activops.db.db_connection import get_db_connection
from activops.utils.config import JSON_DIR_MACHINES, TRACKING_FILE, USER

# Chemin dynamique basé sur le script en cours
script_dir = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = JSON_DIR_MACHINES
PARIS_TZ = pytz.timezone("Europe/Paris")

logger = get_logger("imports_vm")

# Nouveau chemin des JSON
os.makedirs(JSON_DIR, exist_ok=True)  # Crée le dossier s'il n'existe pas

# Génération du nom de fichier avec la date
TODAY = datetime.now(PARIS_TZ).strftime("%Y-%m-%d")
JSON_FILE = os.path.join(JSON_DIR, f"activity_{TODAY}.json")

# Paramètres de suivi
WATCHED_DIRS = ["/home/pipo/bin/", "/home/pipo/dev/", "/home/pipo/docker/"]
MONITORING_PERIOD = 10
IGNORED_PROCESSES = {"ps", "grep", "migration", "watchdog", "idle"}
EXCLUDED_PATTERNS = [".git", ".log", ".tmp", "__pycache__"]


def get_recent_file_changes():
    """
    Récupère les fichiers modifiés récemment en excluant les fichiers cachés et non pertinents.
    """
    try:
        recent_files = []

        # Construire la commande avec exclusions
        exclude_cmd = " ".join(
            [f"! -path '*/{pattern}/*'" for pattern in EXCLUDED_PATTERNS]
        )
        command = f'find {" ".join(WATCHED_DIRS)} -type f \
            -mmin -{MONITORING_PERIOD} {exclude_cmd} -printf "%p | %TY-%Tm-%Td %TH:%TM:%TS\\n"'

        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        files = result.stdout.strip().split("\n")
        recent_files.extend(
            [
                {"file": f.split(" | ")[0], "timestamp": f.split(" | ")[1]}
                for f in files
                if " | " in f
            ]
        )

        return recent_files
    except Exception as e:
        logger.error(f"❌ Erreur récupération fichiers modifiés : {e}")
        return []


def get_active_processes():
    """
    Récupère uniquement les processus interactifs de l'utilisateur.
    """
    try:
        command = f'ps -u {USER} -o tty,comm | grep "pts"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode != 0 or not result.stdout:
            logger.info("ℹ️ Aucun processus interactif trouvé ou commande vide.")
            return []

        processes = []
        for line in result.stdout.strip().split("\n"):
            parts = line.split(None, 1)
            if len(parts) == 2:
                tty, cmd = parts
                cmd = cmd.strip()

                if cmd and cmd not in IGNORED_PROCESSES:
                    processes.append({"tty": tty, "cmd": cmd})

        return processes

    except Exception as e:
        logger.error(f"❌ Erreur récupération processus interactifs : {e}")
        return []


def track_persistent_processes(process_list):
    """
    Suit les processus en cours et ne garde que ceux ouverts depuis >15 min.
    """
    try:
        if not process_list:
            logger.info("🔍 Aucun processus utilisateur actif.")
            return []

        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, encoding="utf-8") as f:
                content = f.read()
                if not content.strip():
                    history = {}
                else:
                    history = json.loads(content)
        else:
            history = {}

        now = datetime.now(PARIS_TZ)
        updated_history = {}

        for proc in process_list:
            process_name = proc["cmd"].split(" ")[0]
            if process_name in history:
                updated_history[process_name] = history[process_name]
            else:
                updated_history[process_name] = now.isoformat()

        with open(TRACKING_FILE, "w", encoding="utf-8") as f:
            json.dump(updated_history, f, indent=4)

        persistent_processes = [
            {"process": p, "start_time": updated_history[p]}
            for p in updated_history
            if (now - datetime.fromisoformat(updated_history[p])).total_seconds() / 60
            > 15
        ]

        return persistent_processes

    except json.JSONDecodeError as e:
        logger.error(f"❌ Erreur JSON dans {TRACKING_FILE} : {e}")
    except Exception as e:
        logger.error(f"❌ Erreur suivi processus persistants : {e}")

    return []


def save_json(data):
    """
    Ajoute les nouvelles données au fichier JSON au lieu de l'écraser.
    """
    json_file = f"{JSON_DIR}activity_{datetime.now().strftime('%Y-%m-%d')}.json"

    # Charger l'ancien contenu s'il existe
    if os.path.exists(json_file):
        with open(json_file, encoding="utf-8") as f:
            try:
                existing_data = json.load(f)
                if isinstance(existing_data, list):
                    existing_data.append(data)
                else:
                    existing_data = [existing_data, data]
            except json.JSONDecodeError:
                existing_data = [data]
    else:
        existing_data = [data]

    # Sauvegarder le fichier mis à jour
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=4)

    logger.info(f"✅ Données ajoutées dans {json_file}")


def insert_data_into_db(data):
    """
    Insère les données dans MariaDB en évitant les doublons intelligemment.
    """   
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        check_query = """
        SELECT COUNT(*) FROM activity_vm
        WHERE hostname = %s AND record_timestamp = %s AND (modified_file_path = %s OR process_name = %s)
        """

        insert_query = """
        INSERT INTO activity_vm
        (hostname, record_timestamp, process_name, process_start_time, modified_file_path, modified_file_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        record_timestamp = datetime.fromisoformat(data["timestamp"]).replace(
            tzinfo=None
        )
        hostname = data["hostname"]
        process_list = data.get("persistent_apps", [])
        modified_files = data.get("modified_files", [])

        for file in modified_files:
            cursor.execute(
                check_query, (hostname, record_timestamp, file["file"], None)
            )
            if cursor.fetchone()[0] == 0:  # Si aucune ligne existante
                cursor.execute(
                    insert_query,
                    (
                        hostname,
                        record_timestamp,
                        None,
                        None,
                        file["file"],
                        datetime.fromisoformat(file["timestamp"]).replace(tzinfo=None),
                    ),
                )

        for process in process_list:
            cursor.execute(
                check_query, (hostname, record_timestamp, None, process["process"])
            )
            if cursor.fetchone()[0] == 0:  # Si aucune ligne existante
                cursor.execute(
                    insert_query,
                    (
                        hostname,
                        record_timestamp,
                        process["process"],
                        datetime.fromisoformat(process["start_time"]).replace(
                            tzinfo=None
                        ),
                        None,
                        None,
                    ),
                )

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ Import en base terminé sans doublons.")

    except Exception as e:
        logger.error(f"❌ Erreur lors de l'insertion en base MariaDB : {e}")


def cleanup_old_json():
    """
    Supprime les fichiers JSON de plus de 15 jours.
    """
    try:
        threshold = datetime.now(PARIS_TZ).timestamp() - (
            15 * 86400
        )  # 15 jours en secondes
        for filename in os.listdir(JSON_DIR):
            file_path = os.path.join(JSON_DIR, filename)
            if os.path.isfile(file_path) and filename.startswith("activity_"):
                file_time = os.path.getmtime(file_path)
                if file_time < threshold:
                    os.remove(file_path)
                    logger.info(f"🗑️ Fichier supprimé : {filename}")
    except Exception as e:
        logger.error(f"❌ Erreur lors du nettoyage des anciens JSON : {e}")


# Exécution principale
if __name__ == "__main__":
    logger.info("🚀 Début du suivi des processus et fichiers")

    active_processes = get_active_processes()
    persistent_processes = track_persistent_processes(active_processes)
    recent_files = get_recent_file_changes()

    context = {
        "hostname": os.uname().nodename,
        "timestamp": datetime.now(PARIS_TZ).isoformat(),
        "persistent_apps": persistent_processes,
        "modified_files": recent_files,
    }

    if persistent_processes or recent_files:
        save_json(context)  # Stockage en local
        insert_data_into_db(context)  # Envoi direct à MariaDB
        cleanup_old_json()
    else:
        logger.info("⚠️ Aucune activité détectée.")

    logger.info("✅ Fin du script.")
