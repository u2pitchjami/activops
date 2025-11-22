$venvPath = "C:\Users\Thierry\Documents\Vscode\brainops\venv\Scripts\Activate.ps1"
$pythonScript = "C:\Users\Thierry\Documents\Vscode\brainops\import_win_to_mariadb.py"
$logFile = "C:\Users\Thierry\Documents\Vscode\brainops\scheduled_debug.log"

try {
    # Activer le venv
    . $venvPath

    # Lancer le script Python
    python $pythonScript >> $logFile 2>&1

    Write-Output "✅ Script exécuté avec succès" >> $logFile
}
 catch {
     Write-Output "❌ Erreur lors de l'exécution : " >> $logFile
}
