#!/usr/bin/env python3
import csv
from datetime import datetime, timedelta
import io
import logging
import socket
import subprocess

import mysql.connector
from mysql.connector import Error

from pc_to_brainops.config import DB_HOST, DB_NAME, DB_PASS, DB_PATH, DB_USER, LOG_FILE, SQLCE_CMD
from pc_to_brainops.types import CursorProtocol

# --- LOGGING ---
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("import_windows_direct")


def get_machine_info() -> tuple[str, str]:
    """
    Retourne hostname et IP de la machine.
    """
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return hostname, ip_address


def fetch_from_sqlce(ip_address: str) -> list[list[str]]:
    """
    Exécuter SqlCeCmd40.exe et retourner les lignes filtrées.
    """
    past12h = (datetime.now() - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
    SELECT '{ip_address}' AS ip_address,
           CONVERT(NVARCHAR, Timestamp, 120) AS Timestamp,
           UserID,
           UserName,
           ApplicationID,
           ApplicationName,
           WindowID,
           WindowTitle,
           Duration
    FROM Recap
    WHERE Timestamp >= '{past12h}'
    """
    conn_str = f"Data Source={DB_PATH}"

    result = subprocess.run(
        [SQLCE_CMD, "-d", conn_str, "-q", query, "-s", "|"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        logger.critical("Erreur SQLCE: %s", result.stderr)
        raise RuntimeError(result.stderr)

    reader = csv.reader(io.StringIO(result.stdout), delimiter="|")
    rows = list(reader)[2:]  # ignorer header + tirets
    logger.info("✅ %d lignes récupérées depuis SQL CE", len(rows))
    return rows


def ensure_machine(cursor: CursorProtocol, hostname: str, ip_address: str) -> int:
    """
    S'assurer que la machine existe dans la table machines.
    """
    cursor.execute("SELECT machine_id FROM machines WHERE machine_name = %s", (hostname,))
    row = cursor.fetchone()
    if row:
        mach_id: int = row[0]
        return mach_id

    cursor.execute(
        "INSERT INTO machines (machine_name, os, ip_address) VALUES (%s, %s, %s)", (hostname, "Windows", ip_address)
    )
    cursor.execute("SELECT LAST_INSERT_ID();")
    machine_id: int = cursor.fetchone()[0]
    logger.info("✅ Nouvelle machine ajoutée : %s (id=%d)", hostname, machine_id)
    return machine_id


def normalize_row(row: list[str]) -> list[str] | None:
    """
    Normaliser une ligne pour obtenir exactement 9 colonnes.
    """
    if len(row) == 9:
        return row
    if len(row) > 9:
        fixed = [*row[:7], " ".join(row[7:-1]).strip(), row[-1]]
        return fixed
    logger.warning("Ligne ignorée (colonnes insuffisantes) : %s", row)
    return None


def insert_into_recap(rows: list[list[str]], machine_id: int) -> None:
    """
    Insérer les lignes directement dans recap.
    """
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset="utf8mb4"
        )
        cursor = conn.cursor()

        for row in rows:
            normalized = normalize_row(row)
            if not normalized:
                continue

            try:
                cursor.execute(
                    """
                    INSERT IGNORE INTO recap
                    (machine_id, ip_address, timestamp, user_id, user_name, application_id,
                    application_name, window_id, window_title, duration)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    [machine_id, *normalized],
                )
            except Error as e:
                logger.error("Erreur insertion ligne (%s): %s", e, normalized)

        conn.commit()
        logger.info("✅ Import terminé (%d lignes)", len(rows))

    except Error as e:
        logger.critical("Erreur connexion MariaDB: %s", e)
    finally:
        if "conn" in locals() and conn.is_connected():
            conn.close()


def main() -> None:
    try:
        hostname, ip_address = get_machine_info()
        rows = fetch_from_sqlce(ip_address)
        if rows:
            conn = mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME, charset="utf8mb4"
            )
            cursor = conn.cursor()
            machine_id = ensure_machine(cursor, hostname, ip_address)
            conn.commit()
            conn.close()

            insert_into_recap(rows, machine_id)
        else:
            logger.info("Aucune donnée à importer.")
    except Exception as e:
        logger.critical("Erreur critique: %s", e)


if __name__ == "__main__":
    main()
