from __future__ import annotations

import csv
from datetime import datetime
import os
from pathlib import Path
import shutil
import time

from activops.android.process_android_datas import process_android_datas
from activops.db.db_connection import get_db_connection, get_dict_cursor
from activops.utils.config import IMPORT_DIR
from activops.utils.logger import LoggerProtocol, ensure_logger, get_logger, with_child_logger
from activops.utils.safe_runner import safe_main

script_dir = os.path.dirname(os.path.abspath(__file__))
logger = get_logger("android_import")


@with_child_logger
def get_machine_id(device_name: str, logger: LoggerProtocol | None = None) -> int | None:
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        return None

    result: int | None = None
    try:
        with get_dict_cursor(conn) as cursor:
            cursor.execute(
                "SELECT machine_id FROM machines WHERE machine_name = %s",
                (device_name,),
            )
            row = cursor.fetchone()
            if row and row.get("machine_id") is not None:
                result = int(row["machine_id"])
                logger.info("[✅] Machine ID pour %s : %s", device_name, result)
            else:
                logger.info("[❌] Machine %s non trouvée en base !", device_name)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("get_machine_id(%s) a échoué: %s", device_name, exc)
        result = None
    finally:
        conn.close()

    return result


@with_child_logger
def process_log_file(file_path: str, logger: LoggerProtocol | None = None) -> int:
    """
    Traite un CSV Android → insert dans android_tmp.

    Retourne le nombre de lignes insérées.
    """
    logger = ensure_logger(logger, __name__)
    conn = get_db_connection(logger=logger)
    if conn is None:
        return 0

    inserted = 0
    try:
        path = Path(file_path)
        with path.open(newline="", encoding="utf-8") as csvfile:
            # 1) lire la 1ère ligne via DictReader pour extraire device_name
            reader = csv.DictReader(csvfile)
            first_row = next(reader, None)
            if not first_row:
                logger.info("[⚠] Fichier vide, ignoré : %s", file_path)
                return 0

            device_name = (first_row.get("device_name") or "").strip()
            if not device_name:
                logger.error("[❌] device_name manquant dans %s", file_path)
                return 0

            machine_id = get_machine_id(device_name, logger=logger)
            if not machine_id:
                logger.error("[❌] Pas de machine_id pour %s, fichier ignoré.", device_name)
                return 0

            # 2) revenir au début et recréer un DictReader propre
            csvfile.seek(0)
            reader = csv.DictReader(csvfile)

            params: list[tuple[object, object, object, object, object]] = []
            for row in reader:
                try:
                    execution_timestamp = row.get("execution_timestamp")
                    package_name = row.get("package_name")
                    last_used_str = row.get("last_used")
                    duration_str = row.get("duration_seconds")

                    if not (execution_timestamp and package_name and last_used_str and duration_str):
                        logger.warning("[⚠] Ligne incomplète ignorée: %s", row)
                        continue

                    try:
                        last_used = datetime.strptime(last_used_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        logger.warning("[⚠] last_used invalide: %s", last_used_str)
                        continue

                    try:
                        duration_seconds = int(duration_str)
                    except (TypeError, ValueError):
                        logger.warning("[⚠] duration_seconds invalide: %s", duration_str)
                        continue

                    params.append((machine_id, execution_timestamp, package_name, last_used, duration_seconds))
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception("[⚠] Ligne ignorée (%s): %s", file_path, exc)

        if params:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO android_tmp
                      (machine_id, execution_timestamp, package_name, last_used, duration_seconds)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    params,
                )
            conn.commit()
            inserted = len(params)
            logger.info("[✅] %s | %d lignes insérées dans android_tmp", device_name, inserted)
        else:
            logger.info("[⏳] Aucune ligne valide à insérer pour %s", file_path)

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("[❌] process_log_file(%s) a échoué: %s", file_path, exc)
        try:
            conn.rollback()
        except Exception:
            logger.exception("Rollback a échoué")
    finally:
        conn.close()
    return inserted


@safe_main
def scan_and_process_logs() -> None:
    """
    Scan le dossier et traite tous les recap_android_*.csv, archive ensuite.
    """
    logger.info("⏳ Pause 30s pour laisser Android finir l'envoi...")
    time.sleep(30)

    base = Path(IMPORT_DIR)
    files = sorted(p for p in base.iterdir() if p.name.startswith("recap_android_") and p.suffix == ".csv")
    if not files:
        logger.info("[📂] Aucun fichier à traiter.")
        return

    date_dir = base / datetime.now().strftime("%y%m%d")
    date_dir.mkdir(exist_ok=True)

    for path in files:
        logger.info("[🔍] Traitement : %s", path.name)
        inserted = process_log_file(file_path=str(path), logger=logger)
        archived = date_dir / f"{path.name}.processed"
        shutil.move(str(path), str(archived))
        logger.info("📂 Archivé (%d lignes) : %s", inserted, archived)


if __name__ == "__main__":
    scan_and_process_logs()
    process_android_datas()
