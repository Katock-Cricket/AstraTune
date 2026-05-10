# Start AstraTune Docker: BIRD target PG:5432, sandbox PG:5433, query API:8010
# Default: docker compose up -d (uses existing images; no rebuild). First-time missing images still build as needed.
# Force rebuild: .\start_docker_stack.ps1 -Build
# By default loads docker/stack.defaults.env (DaoCloud base images + Tsinghua PyPI) so Hub/token IPv6 issues are avoided.
# Official Docker Hub: $env:USE_OFFICIAL_DOCKER_HUB = "1"
# Extra override: $env:USE_DOCKER_MIRROR = "1" still merges mirror vars into process env (optional).
param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"
if ($env:USE_DOCKER_MIRROR -eq "1") {
    if (-not $env:PYTHON_BASE_IMAGE) {
        $env:PYTHON_BASE_IMAGE = "docker.m.daocloud.io/library/python:3.11-slim"
    }
    if (-not $env:POSTGRES_BASE_IMAGE) {
        $env:POSTGRES_BASE_IMAGE = "docker.m.daocloud.io/library/postgres:14.12"
    }
    if (-not $env:PIP_INDEX_URL) {
        $env:PIP_INDEX_URL = "https://pypi.tuna.tsinghua.edu.cn/simple"
    }
}
$RepoRoot = Split-Path -Parent $PSScriptRoot
$DockerDir = Join-Path $RepoRoot "docker"

if (-not (Test-Path $DockerDir)) {
    Write-Error "docker directory not found: $DockerDir"
}

Set-Location $DockerDir
$defaultsEnv = Join-Path $DockerDir "stack.defaults.env"
$dumps = Join-Path $DockerDir "postgre_table_dumps"
if (-not (Test-Path $dumps) -or ((Get-ChildItem $dumps -ErrorAction SilentlyContinue | Measure-Object).Count -eq 0)) {
    Write-Warning "postgre_table_dumps is empty or missing. Unzip BIRD dumps into: $dumps"
    Write-Warning "Or copy: benchmark\evaluation\postgre_table_dumps -> docker\postgre_table_dumps"
}

$upArgs = @("up", "-d")
if ($Build) {
    $upArgs += "--build"
}

if ($env:USE_OFFICIAL_DOCKER_HUB -eq "1") {
    docker compose @upArgs
} elseif (Test-Path $defaultsEnv) {
    docker compose --env-file stack.defaults.env @upArgs
} else {
    Write-Warning "Missing docker/stack.defaults.env; falling back to docker compose defaults (docker.io)."
    docker compose @upArgs
}
$dc = $LASTEXITCODE
if ($dc -ne 0) {
    Write-Host "docker compose failed (exit $dc). Try stack.defaults.env, USE_OFFICIAL_DOCKER_HUB=1, or .\start_docker_stack.ps1 -Build after Dockerfile changes." -ForegroundColor Red
    exit $dc
}
if ($Build) {
    Write-Host "docker compose up -d --build OK. Workdir: $DockerDir" -ForegroundColor Green
} else {
    Write-Host "docker compose up -d OK (no --build). Workdir: $DockerDir" -ForegroundColor Green
}
