import json
import os
from pathlib import Path

import requests

API_URL = "https://api.trakt.tv"
JSON_DIR = Path(os.getenv("JSON_DIR", "./json_dir"))


class TraktClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "Trakt_JSON_Importer/1.0",
                "trakt-api-key": os.getenv("API_KEY"),
                "trakt-api-version": "2",
            }
        )

    def refresh_access_token(self):
        print("🔄 Rafraîchissement du token...")
        data = {
            "refresh_token": os.getenv("REFRESH_TOKEN"),
            "client_id": os.getenv("API_KEY"),
            "client_secret": os.getenv("API_SECRET"),
            "redirect_uri": os.getenv("REDIRECT_URI"),
            "grant_type": "refresh_token",
        }
        r = requests.post(f"{API_URL}/oauth/token", json=data)
        r.raise_for_status()
        tokens = r.json()
        os.environ["ACCESS_TOKEN"] = tokens["access_token"]
        os.environ["REFRESH_TOKEN"] = tokens["refresh_token"]

        # mettre à jour le .env
        self.update_env("ACCESS_TOKEN", tokens["access_token"])
        self.update_env("REFRESH_TOKEN", tokens["refresh_token"])

        print("✅ Token rafraîchi")

    def update_env(self, key, value):
        dotenv_path = Path(__file__).parent / ".env"
        lines = dotenv_path.read_text().splitlines()
        updated = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = f"{key}={value}"
                updated = True
        if not updated:
            lines.append(f"{key}={value}")
        dotenv_path.write_text("\n".join(lines))

    def trakt_get(self, endpoint):
        headers = {"Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}"}
        r = self.session.get(f"{API_URL}{endpoint}", headers=headers)
        if r.status_code == 401:
            self.refresh_access_token()
            headers = {"Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}"}
            r = self.session.get(f"{API_URL}{endpoint}", headers=headers)
        r.raise_for_status()
        return r.json()

    def backup_endpoint(self, endpoint, filename):
        JSON_DIR.mkdir(parents=True, exist_ok=True)
        data = self.trakt_get(endpoint)
        out_file = JSON_DIR / filename
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"📥 Sauvegarde {endpoint} → {out_file}")
