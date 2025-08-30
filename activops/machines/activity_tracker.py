from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

import pytz

from activops.db.db_connection import get_db_connection
from activops.machines.models import FileChange, MachineContext, PersistentProcess, ProcessInfo
from activops.utils.config import JSON_DIR_MACHINES, TRACKING_FILE, USER
from activops.utils.logger import LoggerProtocol, ensure_logger, get_logger

# ---------------------------------------------------------------------------

script_dir = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = Path(JSON_DIR_MACHINES)
JSON_DIR.mkdir(parents=True, exist_ok=True)

PARIS_TZ = pytz.timezone("Europe/Paris")
logger = get_logger("imports_vm")

WATCHED_DIRS: tuple[str, ...] = ("/home/pipo/bin/", "/home/pipo/dev/", "/home/pipo/docker/")
MONITORING_PERIOD = 10  # minutes
IGNORED_PROCESSES = {"ps", "grep", "migration", "watchdog", "idle"}
EXCLUDED_PATTERNS: tuple[str, ...] = (".git", ".log", ".tmp", "__pycache__")

TODAY = datetime.now(PARIS_TZ).strftime("%Y-%m-%d")
JSON_FILE = JSON_DIR / f"activity_{TODAY}.json"

# ---------------------------------------------------------------------------


def _now_paris_iso() -> str:
    return datetime.now(PARIS_TZ).isoformat()


def _as_naive(dt_iso: str) -> datetime:
    """
    Convertit un ISO (avec ou sans TZ) vers datetime naive (UTC-stripped) pour DB.
    """
    try:
        # supporte dates ISO timezone-aware
        dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            return dt.astimezone(UTC).replace(tzinfo=None)
        return dt
    except Exception:
        # fallback: format "YYYY-mm-dd HH:MM:SS"
        try:
            return datetime.strptime(dt_iso, "%Y-%m-%d %H:%M:%S")
        except Exception:
            # dernier recours: now naive
            return datetime.utcnow().replace(tzinfo=None)


# ---------------------------------------------------------------------------


def get_recent_file_changes(logger: LoggerProtocol | None = None) -> list[FileChange]:
    """
    Récupère les fichiers modifiés récemment en excluant certains patterns.

    Implémentation basée sur `find`. Alternative pure Python possible si tu préfères.
    """
    logger = ensure_logger(logger, __name__)
    try:
        # Construire la commande `find` (on reste simple ici)
        exclude_parts = []
        for pat in EXCLUDED_PATTERNS:
            exclude_parts.extend(["!", "-path", f"*/{pat}/*"])

        cmd: list[str] = [
            "find",
            *WATCHED_DIRS,
            "-type",
            "f",
            "-mmin",
            f"-{MONITORING_PERIOD}",
            *exclude_parts,
            "-printf",
            "%p | %TY-%Tm-%Td %TH:%TM:%TS\n",
        ]

        # On garde shell=False pour éviter les surprises
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode not in (0, 1):  # find renvoie 1 si aucun fichier
            logger.warning("find a retourné %s: %s", result.returncode, result.stderr.strip())

        files = result.stdout.strip().split("\n") if result.stdout else []
        out: list[FileChange] = []
        for line in files:
            if " | " not in line:
                continue
            path, ts = line.split(" | ", 1)
            ts = ts.split(".")[0]  # tronque secondes décimales si présentes
            out.append({"file": path, "timestamp": ts})
        return out
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur récupération fichiers modifiés : %s", exc)
        return []


def get_active_processes(logger: LoggerProtocol | None = None) -> list[ProcessInfo]:
    """
    Récupère uniquement les processus interactifs de l'utilisateur (TTY pts/*).
    """
    logger = ensure_logger(logger, __name__)
    try:
        # ps -u USER -o tty,comm | grep "pts"
        cmd = ["ps", "-u", USER, "-o", "tty,comm"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout:
            logger.info("Aucun processus interactif trouvé ou sortie vide.")
            return []

        processes: list[ProcessInfo] = []
        for line in result.stdout.strip().splitlines():
            if "pts" not in line:  # filtre simple
                continue
            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            tty, comm = parts
            comm = comm.strip()
            if comm and comm not in IGNORED_PROCESSES:
                processes.append({"tty": tty, "cmd": comm})
        return processes
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur récupération processus interactifs : %s", exc)
        return []


def track_persistent_processes(
    process_list: list[ProcessInfo],
    logger: LoggerProtocol | None = None,
) -> list[PersistentProcess]:
    """
    Suit les processus en cours et ne garde que ceux ouverts depuis > 15 min.
    """
    logger = ensure_logger(logger, __name__)
    try:
        if not process_list:
            logger.info("🔍 Aucun processus utilisateur actif.")
            return []

        track_path = Path(TRACKING_FILE)
        history: dict[str, str] = {}
        if track_path.exists():
            try:
                content = track_path.read_text(encoding="utf-8")
                if content.strip():
                    history = json.loads(content)
            except json.JSONDecodeError as exc:
                logger.warning("JSON invalide dans %s: %s", track_path, exc)
                history = {}

        now_iso = _now_paris_iso()
        updated: dict[str, str] = {}

        for proc in process_list:
            process_name = proc["cmd"].split(" ")[0]
            if process_name in history:
                updated[process_name] = history[process_name]
            else:
                updated[process_name] = now_iso

        track_path.write_text(json.dumps(updated, indent=4), encoding="utf-8")

        now_naive = _as_naive(now_iso)
        out: list[PersistentProcess] = []
        for name, started_iso in updated.items():
            started_naive = _as_naive(started_iso)
            minutes = (now_naive - started_naive).total_seconds() / 60.0
            if minutes > 15:
                out.append({"process": name, "start_time": started_iso})
        return out

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur suivi processus persistants : %s", exc)
        return []


def save_json(data: MachineContext, logger: LoggerProtocol | None = None) -> None:
    """
    Ajoute les nouvelles données au fichier JSON du jour (append logique).
    """
    logger = ensure_logger(logger, __name__)
    json_file = JSON_DIR / f"activity_{datetime.now(PARIS_TZ).strftime('%Y-%m-%d')}.json"

    try:
        existing: list[MachineContext]
        if json_file.exists():
            try:
                content = json_file.read_text(encoding="utf-8")
                loaded = json.loads(content) if content.strip() else []
                if isinstance(loaded, list):
                    existing = loaded
                else:
                    existing = [loaded]
            except json.JSONDecodeError:
                existing = []
        else:
            existing = []

        existing.append(data)
        json_file.write_text(json.dumps(existing, indent=4), encoding="utf-8")
        logger.info("✅ Données ajoutées dans %s", json_file)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur save_json(%s): %s", json_file, exc)


def insert_data_into_db(data: MachineContext, logger: LoggerProtocol | None = None) -> None:
    """
    Insère les données dans MariaDB en évitant les doublons.

    Préfère une contrainte UNIQUE + INSERT IGNORE / ON DUPLICATE KEY UPDATE.
    """
    logger = ensure_logger(logger, __name__)
    try:
        conn = get_db_connection(logger=logger)
        if conn is None:
            logger.error("Connexion DB indisponible")
            return

        record_ts = _as_naive(data["timestamp"])
        hostname = data["hostname"]
        process_list = data.get("persistent_apps", [])
        modified_files = data.get("modified_files", [])

        # Recommandé: index/contrainte unique sur (hostname, record_timestamp, modified_file_path, process_name)
        # Exemple DDL:
        # ALTER TABLE activity_vm
        #   ADD UNIQUE KEY uq_activity (hostname, record_timestamp, modified_file_path, process_name);

        check_query = """
        SELECT COUNT(*) AS cnt
        FROM activity_vm
        WHERE hostname = %s AND record_timestamp = %s
          AND ((modified_file_path = %s AND %s IS NULL) OR (process_name = %s AND %s IS NULL))
        """

        insert_query = """
        INSERT INTO activity_vm
          (hostname, record_timestamp, process_name, process_start_time, modified_file_path, modified_file_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        with conn.cursor(dictionary=True) as cursor:
            # fichiers modifiés
            for f in modified_files:
                file_path = f.get("file")
                ts_iso = f.get("timestamp")
                if not file_path or not ts_iso:
                    continue
                mod_ts = _as_naive(ts_iso)
                cursor.execute(check_query, (hostname, record_ts, file_path, None, None, file_path))
                row = cursor.fetchone() or {"cnt": 0}
                if int(row["cnt"]) == 0:
                    cursor.execute(
                        insert_query,
                        (hostname, record_ts, None, None, file_path, mod_ts),
                    )

            # processus persistants
            for p in process_list:
                pname = p.get("process")
                pstart_iso = p.get("start_time")
                if not pname or not pstart_iso:
                    continue
                pstart = _as_naive(pstart_iso)
                cursor.execute(check_query, (hostname, record_ts, None, pname, pname, None))
                row = cursor.fetchone() or {"cnt": 0}
                if int(row["cnt"]) == 0:
                    cursor.execute(
                        insert_query,
                        (hostname, record_ts, pname, pstart, None, None),
                    )

        conn.commit()
        logger.info("✅ Import en base terminé sans doublons.")
        conn.close()
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur lors de l'insertion en base MariaDB : %s", exc)


def cleanup_old_json(logger: LoggerProtocol | None = None) -> None:
    """
    Supprime les JSON > 15 jours.
    """
    logger = ensure_logger(logger, __name__)
    try:
        threshold = datetime.now(PARIS_TZ).timestamp() - (15 * 86400)
        for p in JSON_DIR.iterdir():
            if p.is_file() and p.name.startswith("activity_"):
                if p.stat().st_mtime < threshold:
                    p.unlink()
                    logger.info("🗑️ Fichier supprimé : %s", p.name)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("❌ Erreur nettoyage JSON : %s", exc)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("🚀 Début du suivi des processus et fichiers")

    active = get_active_processes(logger=logger)
    persistent = track_persistent_processes(active, logger=logger)
    recent = get_recent_file_changes(logger=logger)

    context: MachineContext = {
        "hostname": os.uname().nodename,
        "timestamp": _now_paris_iso(),
        "persistent_apps": persistent,
        "modified_files": recent,
    }

    if persistent or recent:
        save_json(context, logger=logger)
        insert_data_into_db(context, logger=logger)
        cleanup_old_json(logger=logger)
    else:
        logger.info("⚠️ Aucune activité détectée.")

    logger.info("✅ Fin du script.")
