#!/data/data/com.termux/files/usr/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
source ${SCRIPT_DIR}/.config.cfg


# 🔹 Vérifier si des fichiers existent
FILES=$(ls $LOGS_DIR | grep "recap_android_")
if [ -z "$FILES" ]; then
    echo "[❌] Aucun fichier à envoyer."
    exit 0
fi

# Vérifier si SSH est actif sur Termux
if ! pgrep sshd > /dev/null; then
    echo "❌ SSHD n'est pas actif sur Termux. Tentative de démarrage..."
    sshd
    sleep 3  # Attendre un peu que SSH démarre
    if ! pgrep sshd > /dev/null; then
        echo "🚨 Impossible de démarrer SSHD. Abandon de l'envoi."
        exit 1
    fi
fi

# Vérifier si le serveur distant est joignable

if ! ping -c 2 -W 2 "$VM_IP" > /dev/null; then
    echo "🚨 Le serveur $VM_IP est injoignable. Abandon de l'envoi."
    exit 1
fi


echo "[📂] Envoi des fichiers Android vers Unraid ..."

# 🔹 Envoi vers la VM Ubuntu
scp -P 2100 $LOGS_DIR/recap_android_* $VM_USER@$VM_IP:$VM_DEST
VM_STATUS=$?

# 🔹 Suppression des fichiers SI ET SEULEMENT SI les 2 transferts ont réussi
if [ $VM_STATUS -eq 0 ]; then
    rm -f $LOGS_DIR/recap_android_*
    echo "[🗑] Fichiers envoyés supprimés localement."
else
    echo "[⚠] Attention : Envoi échoué, fichiers conservés."
fi