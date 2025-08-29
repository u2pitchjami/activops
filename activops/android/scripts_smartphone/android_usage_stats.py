import os
import re
import subprocess
import time
from datetime import datetime, timedelta

# 📌 Récupérer le nom du téléphone (device_name)
device_name = subprocess.run(
    "getprop ro.product.model", shell=True, capture_output=True, text=True
).stdout.strip()
if not device_name:
    device_name = subprocess.run(
        "settings get secure android_id", shell=True, capture_output=True, text=True
    ).stdout.strip()

# 📌 Déterminer la tranche horaire d’exécution
# Récupérer l'heure actuelle
now = datetime.now()

# Si on est à minuit passé mais en tout début de journée, on ajuste l'horodatage
if now.hour == 0 and now.minute == 0 and now.second <= 5:
    execution_timestamp = (now - timedelta(seconds=now.second + 1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
else:
    execution_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

print(f"🕒 Timestamp envoyé : {execution_timestamp}")

# 📌 Exécuter `dumpsys usagestats` et récupérer ACTIVITY_RESUMED + ACTIVITY_PAUSED
cmd = "su -c 'dumpsys usagestats | grep -E \"ACTIVITY_RESUMED|ACTIVITY_PAUSED\"'"
process = subprocess.run(cmd, shell=True, capture_output=True, text=True)

# 📌 Vérifier si la sortie est valide
if process.returncode != 0 or not process.stdout:
    print("❌ Erreur : aucune donnée récupérée depuis dumpsys usagestats.")
    exit(1)

# 📌 Extraire les événements avec regex
pattern = re.compile(
    r'time="([\d-]+ [\d:]+)".*type=(ACTIVITY_RESUMED|ACTIVITY_PAUSED) package=([\w\.]+)'
)

# 📌 Stocker les événements par application
app_events = {}

for line in process.stdout.splitlines():
    match = pattern.search(line)
    if match:
        timestamp, event_type, package_name = match.groups()

        # Convertir le timestamp Android en timestamp UNIX
        event_time = time.mktime(time.strptime(timestamp, "%Y-%m-%d %H:%M:%S"))

        # 📌 Ne garder que les événements du jour
        today_start = time.mktime(
            time.strptime(time.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S")
        )
        if event_time >= today_start:
            if package_name not in app_events:
                app_events[package_name] = []
            app_events[package_name].append((event_time, event_type))

# 📌 Calculer les durées d'utilisation par application avec gestion du passage minuit
app_usage = {}
for package, events in app_events.items():
    events.sort()  # Trier chronologiquement
    total_duration_today = 0
    last_resumed = None

    for event_time, event_type in events:
        if event_type == "ACTIVITY_RESUMED":
            last_resumed = event_time  # Enregistre le moment d'ouverture
        elif event_type == "ACTIVITY_PAUSED" and last_resumed:
            session_duration = event_time - last_resumed
            if 0 < session_duration < 3600:  # 📌 Filtre les valeurs aberrantes (>1h)
                total_duration_today += session_duration
            last_resumed = None  # Reset pour la prochaine session

    app_usage[package] = int(total_duration_today)  # Stocker la durée pour aujourd’hui

# 📌 Définir le nom du fichier en incluant `device_name` et la date
log_dir = "/data/data/com.termux/files/home/android_logs/csv/"
os.makedirs(log_dir, exist_ok=True)  # Créer le dossier s'il n'existe pas

log_filename = (
    f"recap_android_{device_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
)
log_file = os.path.join(log_dir, log_filename)

# 📌 Écrire les données finales dans un fichier CSV
with open(log_file, "w") as f:
    f.write("device_name,execution_timestamp,package_name,last_used,duration_seconds\n")
    for app, duration in app_usage.items():
        last_used = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(max(event[0] for event in app_events[app])),
        )
        f.write(f"{device_name},{execution_timestamp},{app},{last_used},{duration}\n")

print(f"✔ Stats enregistrées avec {log_file}")
