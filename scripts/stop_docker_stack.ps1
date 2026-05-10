# Stop AstraTune Docker stack (compose down)
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$DockerDir = Join-Path $RepoRoot "docker"
Set-Location $DockerDir
docker compose down
Write-Host "docker compose down finished." -ForegroundColor Green