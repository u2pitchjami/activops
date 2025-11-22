# config.py
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

# Chargement du .env à la racine du projet
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

# --- Fonctions utilitaires ---


def get_required(key: str) -> str:
    value = os.getenv(key)
    if value is None:
        print(f"[CONFIG ERROR] La variable {key} est requise mais absente.")
        sys.exit(1)
    return value


def get_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).lower() in ("true", "1", "yes")


def get_str(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def get_int(key: str, default: int = 0) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        print(f"[CONFIG ERROR] La variable {key} doit être un entier.")
        sys.exit(1)


# --- Variables d'environnement accessibles globalement ---


DB_HOST = (get_required("DB_HOST"),)
DB_USER = (get_required("DB_USER"),)
DB_PASS = (get_required("DB_PASSWORD"),)
DB_NAME = (get_required("DB_NAME"),)

SQLCE_CMD = get_str("SQLCE_CMD")
DB_PATH = get_str("DB_PATH")
IP_ADDRESS = get_str("IP_ADDRESS")
LOG_FILE = get_str("LOG_FILE")
