# Start only pg_target and pg_sandbox (no API). Same compose/env as start_docker_stack.ps1
$ErrorActionPreference = "Stop"
if ($env:USE_DOCKER_MIRROR -eq "1") {
    if (-not $env:POSTGRES_BASE_IMAGE) {
        $env:POSTGRES_BASE_IMAGE = "docker.m.daocloud.io/library/postgres:14.12"
    }
}
$RepoRoot = Split-Path -Parent $PSScriptRoot
$DockerDir = Join-Path $RepoRoot "docker"
Set-Location $DockerDir
$defaultsEnv = Join-Path $DockerDir "stack.defaults.env"

if ($env:USE_OFFICIAL_DOCKER_HUB -eq "1") {
    docker compose up -d pg_target pg_sandbox
} elseif (Test-Path $defaultsEnv) {
    docker compose --env-file stack.defaults.env up -d pg_target pg_sandbox
} else {
    docker compose up -d pg_target pg_sandbox
}
Write-Host "Started pg_target:5432 and pg_sandbox:5433" -ForegroundColor Green
Write-Host "Import logs: docker logs -f astratune_pg_target" -ForegroundColor Cyan
