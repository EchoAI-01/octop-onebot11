<#
.SYNOPSIS
    Install the Octop OneBot 11 (NapCat) channel plugin on NEC8.

.DESCRIPTION
    Uploads plugins/octop-onebot11, installs it through the Octop CLI, attaches
    the Octop container to the MaiBot Docker network, and creates the channel.
    NapCat does not need to be logged in for this step.

.EXAMPLE
    .\install.ps1 -Token "your-napcat-ws-token"

.EXAMPLE
    .\install.ps1 -Agent Eous -WsUrl "ws://maim-bot-napcat:3001" -Token "dev-token"
#>
param(
    [string]$Remote = "NEC8",
    [string]$Agent = "Eous",
    [string]$WsUrl = "ws://maim-bot-napcat:3001",
    [string]$Token = "",
    [string]$Name = "QQ (NapCat)",
    [switch]$SkipUpload
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$pluginDir = Join-Path $root "plugins\octop-onebot11"
$toolsDir = Join-Path $root "plugins\octop-onebot11-tools"
$remoteUploads = "/home/chen/Docker/Octop/data/uploads"

function Invoke-Remote([string]$Command) {
    ssh $Remote "$Command"
    if ($LASTEXITCODE -ne 0) { throw "remote command failed: $Command" }
}

Write-Host "[1/6] ensure remote upload directory" -ForegroundColor Cyan
Invoke-Remote "mkdir -p $remoteUploads"

if (-not $SkipUpload) {
    Write-Host "[2/6] upload plugin" -ForegroundColor Cyan
    scp -r $pluginDir "${Remote}:${remoteUploads}/"
    if ($LASTEXITCODE -ne 0) { throw "scp channel plugin failed" }
    scp -r $toolsDir "${Remote}:${remoteUploads}/"
    if ($LASTEXITCODE -ne 0) { throw "scp tools plugin failed" }
} else {
    Write-Host "[2/6] upload skipped" -ForegroundColor Yellow
}

Write-Host "[3/6] install plugin" -ForegroundColor Cyan
Invoke-Remote "docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11 --force"
Invoke-Remote "docker exec octop octop plugin install /data/.octop/uploads/octop-onebot11-tools --force"

Write-Host "[4/6] attach Octop to maibot_maim_bot" -ForegroundColor Cyan
ssh $Remote "docker network connect maibot_maim_bot octop 2>/dev/null; true"

Write-Host "[5/6] create channel" -ForegroundColor Cyan
$config = @{ ws_url = $WsUrl; group_reply_mode = "mention" }
if ($Token) { $config.token = $Token }
$configJson = $config | ConvertTo-Json -Compress
# ConvertTo-Json escapes nothing problematic here; single quotes keep it intact on the remote side.
$remoteCmd = "docker exec octop octop channel create --agent $Agent --kind onebot11 --name '$Name' --config '$configJson'"
ssh $Remote "$remoteCmd"

Write-Host "[6/6] restart octop so the plugin and channel load in the server process" -ForegroundColor Cyan
ssh $Remote "docker restart octop"

Write-Host ""
Write-Host "Done. Check status with:" -ForegroundColor Green
Write-Host "  ssh $Remote `"docker exec octop octop channel list --agent $Agent`""
Write-Host "  ssh $Remote `"docker logs --tail 100 octop | grep -i onebot11`""
Write-Host ""
Write-Host "After NapCat logs in, create a WebSocket server on 0.0.0.0:3001 with the same token,"
Write-Host "then update the channel if needed:"
Write-Host "  ssh $Remote `"docker exec octop octop channel patch <channel_id> --config '{\"token\":\"$Token\"}'`""
