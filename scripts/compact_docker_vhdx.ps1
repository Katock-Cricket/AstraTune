#!/usr/bin/env pwsh
# 压缩 Docker Desktop WSL 虚拟磁盘（docker_data.vhdx）
# 使用前须：docker compose down →（可选 docker builder prune）→ 启动 Docker 执行 fstrim：
#   wsl -d docker-desktop -u root -- fstrim -v /mnt/docker-desktop-disk
# → 退出 Docker Desktop → wsl --shutdown → 本脚本（管理员）
# 默认路径来自 settings-store.json 的 CustomWslDistroDir（本机 D:\DockerDesktopWSL）

param(
    [string]$VhdxPath = "D:\DockerDesktopWSL\disk\docker_data.vhdx",
    [switch]$SkipAdminCheck
)

$ErrorActionPreference = "Stop"

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not $SkipAdminCheck -and -not (Test-Admin)) {
    Write-Error "请以管理员身份运行 PowerShell 后再执行本脚本。"
}

if (-not (Test-Path -LiteralPath $VhdxPath)) {
    Write-Error "找不到 vhdx: $VhdxPath"
}

$d = Get-Volume D -ErrorAction SilentlyContinue
if ($d) {
    $gb = [math]::Round($d.SizeRemaining / 1GB, 2)
    Write-Host "D: 当前剩余空间: ${gb} GB"
    if ($gb -lt 5) {
        Write-Warning "D: 空间极低；压缩前请确保 Docker 已完全停止。"
    }
}

$diskpartScript = @"
select vdisk file="$VhdxPath"
attach vdisk readonly
compact vdisk
detach vdisk
exit
"@

$tmp = Join-Path $env:TEMP "compact_docker_vhdx_$(Get-Date -Format yyyyMMdd_HHmmss).txt"
$diskpartScript | Out-File -FilePath $tmp -Encoding ASCII

Write-Host "开始 diskpart compact（只读 attach）..."
Write-Host "脚本: $tmp"
& diskpart /s $tmp
$code = $LASTEXITCODE
Remove-Item -Force $tmp -ErrorAction SilentlyContinue

if ($code -ne 0) {
    Write-Error "diskpart 退出码 $code。请确认 Docker Desktop 已退出且 wsl --shutdown 已执行。"
}

$d2 = Get-Volume D -ErrorAction SilentlyContinue
if ($d2) {
    $gb2 = [math]::Round($d2.SizeRemaining / 1GB, 2)
    Write-Host "压缩完成。D: 剩余空间: ${gb2} GB"
}
Write-Host "请重新启动 Docker Desktop，然后 docker compose up -d。"
