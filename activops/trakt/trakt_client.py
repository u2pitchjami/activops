from __future__ import annotations

import json
import os
from pathlib import Path
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
        self._access_token: str = ACCESS_TOKEN
        self._refresh_token: str = REFRESH_TOKEN

    def _update_env(self, key: str, value: str) -> None:
        dotenv_path = Path(__file__).resolve().parent / ".env"
        try:
            if dotenv_path.exists():
                lines = dotenv_path.read_text(encoding="utf-8").splitlines()
                updated = False
                for i, line in enumerate(lines):
                    if line.startswith(f"{key}="):
                        lines[i] = f"{key}={value}"
                        updated = True
                        break
                if not updated:
                    lines.append(f"{key}={value}")
                dotenv_path.write_text("\n".join(lines), encoding="utf-8")
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Impossible de mettre à jour .env: %s", exc)

    def refresh_access_token(self) -> None:
        logger.info("🔄 Rafraîchissement du token…")
        data = {
            "refresh_token": self._refresh_token,
            "client_id": API_KEY,
            "client_secret": API_SECRET,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "refresh_token",
        }
        r = requests.post(f"{API_URL}/oauth/token", json=data, timeout=15)
        r.raise_for_status()
        tokens: dict[str, str] = r.json()
        self._access_token = tokens["access_token"]
        self._refresh_token = tokens["refresh_token"]

        # met aussi à jour l'environnement process (si d'autres modules s'en servent)
        os.environ["ACCESS_TOKEN"] = self._access_token
        os.environ["REFRESH_TOKEN"] = self._refresh_token

        # et .env local si présent
        self._update_env("ACCESS_TOKEN", self._access_token)
        self._update_env("REFRESH_TOKEN", self._refresh_token)
        logger.info("✅ Token rafraîchi")

    def trakt_get(self, endpoint: str) -> JsonObj | list[JsonObj]:
        headers = {"Authorization": f"Bearer {self._access_token}"}
        r = self.session.get(f"{API_URL}{endpoint}", headers=headers, timeout=20)
        if r.status_code == 401:
            self.refresh_access_token()
            headers = {"Authorization": f"Bearer {self._access_token}"}
            r = self.session.get(f"{API_URL}{endpoint}", headers=headers, timeout=20)
        r.raise_for_status()
        data: Any = r.json()
        if isinstance(data, list):
            return cast(list[JsonObj], data)
        return cast(JsonObj, data)

    def backup_endpoint(self, endpoint: str, filename: str) -> None:
        JSON_DIR.mkdir(parents=True, exist_ok=True)
        data = self.trakt_get(endpoint)
        out_file = JSON_DIR / filename
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info("📥 Sauvegarde %s → %s", endpoint, out_file)
