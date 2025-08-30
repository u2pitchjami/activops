from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from activops.db.db_connection import get_db_connection
from activops.db.types import CursorProtocol
from activops.garmin.models import GarminClientProtocol
from activops.utils.logger import LoggerProtocol, ensure_logger, with_child_logger


@with_child_logger
def get_garmin_heart_rate(
    client: GarminClientProtocol,
    date_to_check: str | None = None,
    logger: LoggerProtocol | None = None,
) -> None:
    logger = ensure_logger(logger, __name__)
    date_str = date_to_check or datetime.now().strftime("%Y-%m-%d")

    heart_rate_dict: defaultdict[str, list[int]] = defaultdict(list)

    try:
        hr_payload = client.get_heart_rates(date_str)
        heart_rates = hr_payload.get("heartRateValues", [])  # [[timestamp_ms, value], ...]
        for entry in heart_rates:
            if not isinstance(entry, list | tuple) or len(entry) != 2:
                continue
            timestamp_ms, hr = entry
            if hr is None:
                continue
            try:
                ts_sec = int(timestamp_ms) // 1000
                hhmm = datetime.fromtimestamp(ts_sec).strftime("%H:%M")
                minute = int(hhmm.split(":")[1])
                rounded_minute = (minute // 10) * 10
                # format TIME compatible MySQL : "HH:MM:00"
                time_slot = f"{hhmm[:3]}{rounded_minute:02}:00"
                heart_rate_dict[time_slot].append(int(hr))
            except Exception:
                continue
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Erreur récupération heart_rate: %s", e)
        return

    if not heart_rate_dict:
        logger.warning("⚠️ Aucune donnée de FC disponible pour %s.", date_str)
        return

    conn = get_db_connection(logger=logger)
    if conn is None:
        return
    try:
        cursor: CursorProtocol = conn.cursor()
        insert_query = """
            INSERT INTO garmin_heart_rate (date, time_slot, avg_heart_rate)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE avg_heart_rate=VALUES(avg_heart_rate);
        """
        for time_slot, values in heart_rate_dict.items():
            avg_hr = round(sum(values) / len(values))
            cursor.execute(insert_query, (date_str, time_slot, avg_hr))
        conn.commit()
        logger.info("✅ Données FC moyennées 10 min insérées pour %s", date_str)
    except Exception as err:  # pylint: disable=broad-except
        logger.error("Erreur insertion FC en base: %s", err)
        try:
            conn.rollback()
        except Exception:  # pylint: disable=broad-except
            pass
    finally:
        conn.close()
