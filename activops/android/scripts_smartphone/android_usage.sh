#!/bin/bash

# 📌 Config
LOG_DIR="$HOME/android_logs"
LOG_FILE="$LOG_DIR/android_usage_$(date +'%Y-%m-%d_%H-%M-%S').csv"
MACHINE_ID="4"  # ⚠️ Remplace par ton vrai machine_id
TIMESTAMP=$(date +'%Y-%m-%d %H:%M:00")  # ✅ Correction du guillemet ici

mkdir -p "$LOG_DIR"

echo "Extracting Active Android Usage Stats..."
echo "machine_id,timestamp,application_id,application_name,duration_seconds" > "$LOG_FILE"

# 📌 Extraction des événements ACTIVITY_RESUMED uniquement pour les applications utilisateur
su -c "dumpsys usagestats | grep 'ACTIVITY_RESUMED'" | while read -r line; do
    APP_ID=$(echo "$line" | sed -n 's/.*package=\([^ ]*\).*/\1/p')
    APP_TIME=$(echo "$line" | sed -n 's/.*time="\([^"]*\)".*/\1/p')

    if [[ -n "$APP_ID" && -n "$APP_TIME" ]]; then
        # Exclure les apps système
        if [[ "$APP_ID" != com.android.* && "$APP_ID" != com.miui.* && "$APP_ID" != com.google.android.* ]]; then
            # Convertir la date Android en timestamp UNIX
            APP_TIMESTAMP=$(date -d "$APP_TIME" +%s 2>/dev/null)

            # Calculer la durée (différence entre maintenant et la dernière utilisation)
            NOW=$(date +%s)
            DURATION=$((NOW - APP_TIMESTAMP))

            # Empêcher les valeurs négatives ou trop grandes
            if [[ "$DURATION" -lt 0 || "$DURATION" -gt 86400 ]]; then
                DURATION=0
            fi

            echo "$MACHINE_ID,$TIMESTAMP,$APP_ID,$APP_ID,$DURATION" >> "$LOG_FILE"
        fi
    fi
done

echo "Logs filtrés avec durée extraits et stockés dans : $LOG_FILE"
