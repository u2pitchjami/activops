from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any, cast

import requests

from activops.trakt.models import JsonObj
from activops.utils.config import (
    ACCESS_TOKEN,
    API_KEY,
    API_SECRET,
    JSON_DIR,
    REDIRECT_URI,
    REFRESH_TOKEN,
)
from activops.utils.logger import get_logger

API_URL = "https://api.trakt.tv"
logger = get_logger("TraktClient")

# ✅ chemin .env corrigé (parent.parent)
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


class TraktClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "Trakt_JSON_Importer/1.0",
                "trakt-api-key": API_KEY,
                "trakt-api-version": "2",
            }
        )

        # Tokens initiaux (chargés depuis .env via load_dotenv en amont)
        self._access_token: str = ACCESS_TOKEN
        self._refresh_token: str = REFRESH_TOKEN

        self._created_at: int = int(os.getenv("TRAKT_CREATED_AT", "0"))
        self._expires_in: int = int(os.getenv("TRAKT_EXPIRES_IN", "0"))

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------

    def _access_token_expired(self, margin: int = 60) -> bool:
        """
        Détermine si l'access_token est expiré (ou proche).
        """
        if self._created_at <= 0 or self._expires_in <= 0:
            return True

        return time.time() > (self._created_at + self._expires_in - margin)

    def _update_env(self, key: str, value: str) -> None:
        """
        Met à jour une clé dans le fichier .env sans le casser.
        """
        try:
            lines: list[str] = []

            if ENV_PATH.exists():
                lines = ENV_PATH.read_text(encoding="utf-8").splitlines()

            updated = False
            for i, line in enumerate(lines):
                if line.startswith(f"{key}="):
                    lines[i] = f"{key}={value}"
                    updated = True
                    break

            if not updated:
                lines.append(f"{key}={value}")

            ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Impossible de mettre à jour .env (%s): %s", key, exc)

    # ------------------------------------------------------------------
    # OAuth
    # ------------------------------------------------------------------

    def refresh_access_token(self) -> None:
        """
        Rafraîchit le token Trakt UNIQUEMENT quand nécessaire.
        """
        logger.info("🔄 Rafraîchissement du token Trakt…")

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": API_KEY,
            "client_secret": API_SECRET,
            "redirect_uri": REDIRECT_URI,
        }

        try:
            response = requests.post(
                f"{API_URL}/oauth/token",
                json=payload,
                timeout=15,
            )
            response.raise_for_status()

        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else "?"
            logger.error("❌ Impossible de rafraîchir le token Trakt (code %s).", status)
            logger.error("➡️ Token probablement invalide ou expiré.")
            raise

        except requests.RequestException as exc:
            logger.error("❌ Erreur réseau lors du refresh Trakt: %s", exc)
            raise

        tokens = response.json()

        # Mise à jour mémoire
        self._access_token = tokens["access_token"]
        self._refresh_token = tokens["refresh_token"]
        self._created_at = int(tokens["created_at"])
        self._expires_in = int(tokens["expires_in"])

        # Mise à jour process
        os.environ.update(
            {
                "ACCESS_TOKEN": self._access_token,
                "REFRESH_TOKEN": self._refresh_token,
                "TRAKT_CREATED_AT": str(self._created_at),
                "TRAKT_EXPIRES_IN": str(self._expires_in),
            }
        )

        # Persistance .env
        self._update_env("ACCESS_TOKEN", self._access_token)
        self._update_env("REFRESH_TOKEN", self._refresh_token)
        self._update_env("TRAKT_CREATED_AT", str(self._created_at))
        self._update_env("TRAKT_EXPIRES_IN", str(self._expires_in))

        logger.info("✅ Token Trakt rafraîchi avec succès")

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def trakt_get(self, endpoint: str) -> JsonObj | list[JsonObj]:
        """
        Appel GET Trakt avec gestion propre du refresh.
        """
        if self._access_token_expired():
            logger.info("🔄 Access token expiré → refresh")
            self.refresh_access_token()

        headers = {"Authorization": f"Bearer {self._access_token}"}
        response = self.session.get(f"{API_URL}{endpoint}", headers=headers, timeout=20)

        if response.status_code == 401:
            logger.warning("401 Trakt → refresh forcé")
            self.refresh_access_token()
            headers = {"Authorization": f"Bearer {self._access_token}"}
            response = self.session.get(f"{API_URL}{endpoint}", headers=headers, timeout=20)

        response.raise_for_status()

        data: Any = response.json()
        if isinstance(data, list):
            return cast(list[JsonObj], data)
        return cast(JsonObj, data)

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup_endpoint(self, endpoint: str, filename: str) -> None:
        JSON_DIR.mkdir(parents=True, exist_ok=True)

        data = self.trakt_get(endpoint)
        out_file = JSON_DIR / filename

        with out_file.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("📥 Sauvegarde %s → %s", endpoint, out_file)
