param(
    [string]$RepoPath = "D:\CC2 Academy\Discord bot"
)

$pythonExe = Join-Path $RepoPath ".venv\Scripts\python.exe"
$botEntry = Join-Path $RepoPath "discordwelcomebot.py"

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found: $pythonExe"
}
if (-not (Test-Path $botEntry)) {
    throw "Bot entry file not found: $botEntry"
}

Set-Location $RepoPath

while ($true) {
    & $pythonExe $botEntry
    $exitCode = $LASTEXITCODE

    Write-Host "Bot exited with code $exitCode. Restarting in 5 seconds..."
    Start-Sleep -Seconds 5
}
