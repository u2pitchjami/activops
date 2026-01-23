"""
Script complet d'authentification OAuth pour Trakt.

Permet de générer une URL OAuth + d'échanger le code → token complet. Compatible Pylint & logs ActivOps.
"""

from __future__ import annotations

import json
from urllib.parse import quote

import requests

from activops.trakt.models import TraktTokens
from activops.utils.config import API_KEY, API_SECRET, REDIRECT_URI
from activops.utils.logger import get_logger

logger = get_logger("TraktAuth")


def generate_oauth_url() -> str:
    """
    Construit l'URL OAuth à ouvrir dans le navigateur.
    """
    return f"https://trakt.tv/oauth/authorize?response_type=code&client_id={API_KEY}&redirect_uri={quote(REDIRECT_URI)}"


def exchange_code(code: str) -> TraktTokens:
    """
    Échange un code OAuth contre un access_token + refresh_token.
    """
    payload = {
        "code": code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    try:
        response = requests.post("https://api.trakt.tv/oauth/token", json=payload, timeout=10)
        response.raise_for_status()
        data: TraktTokens = response.json()
        return data
    except requests.RequestException as exc:  # pylint: disable=broad-except
        logger.error("Erreur OAuth lors de l'échange du code: %s", exc)
        raise


def update_env_var(key: str, value: str) -> None:
    """
    Met à jour le fichier .env local avec une variable donnée.
    """
    from pathlib import Path

    dotenv_path = Path(__file__).resolve().parent.parent / ".env"

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
            dotenv_path.write_text("".join(lines), encoding="utf-8")
        else:
            dotenv_path.write_text(f"{key}={value}", encoding="utf-8")
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Impossible de mettre à jour .env: %s", exc)


def save_tokens(tokens: TraktTokens) -> None:
    """
    Enregistre les tokens OAuth dans le fichier .env avec validations et logs.
    """

    access = tokens.get("access_token")
    refresh = tokens.get("refresh_token")

    if not access or not refresh:
        logger.error("Tokens manquants dans la réponse OAuth : %s", tokens)
        raise ValueError("Réponse OAuth incomplète : access_token ou refresh_token absent")

    logger.info("💾 Mise à jour du fichier .env avec les nouveaux tokens…")
    update_env_var("ACCESS_TOKEN", access)
    update_env_var("REFRESH_TOKEN", refresh)
    logger.info("✅ Tokens enregistrés avec succès dans .env")


def main() -> None:
    """
    Point d'entrée du script d'authentification.
    """
    url = generate_oauth_url()
    print("➡️ Ouvre cette URL dans ton navigateur :")
    print(url)

    code = input("\n👉 Colle ici le code obtenu dans l'URL de redirection : ").strip()
    tokens = exchange_code(code)
    save_tokens(tokens)

    logger.info("🎉 Token récupéré avec succès !")
    print(json.dumps(tokens, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
