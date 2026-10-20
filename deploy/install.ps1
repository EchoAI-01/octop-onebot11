<#
.SYNOPSIS
    Install the Octop OneBot 11 (NapCat) plugins into a Dockerised Octop over SSH.

.DESCRIPTION
    Windows helper for Octop running in a Docker container.  On Linux, macOS,
    WSL or Git Bash prefer deploy/install.sh (it also handles systemd installs,
    hot reload and post-install verification).

    Steps: upload plugins/octop-onebot11 and plugins/octop-onebot11-tools to
    the Docker host, install them with the Octop CLI inside the container,
    optionally attach the container to the Docker network NapCat lives in, and
    restart the container.  NapCat does not need to be logged in for this.

    The script deliberately does NOT create the channel: that needs the access
    token, and anything passed on a command line ends up in shell history and
    process lists.  Add the channel in the dashboard (token field is a password
    input) or follow the hints printed at the end.

.PARAMETER Remote
    SSH target of the Docker host (user@host or an ssh config alias).

.PARAMETER HostUploadsDir
    Absolute directory on the Docker host that is bind-mounted into the
    container at ContainerUploadsDir (no spaces).

.PARAMETER ContainerUploadsDir
    Where HostUploadsDir appears inside the container.  Default: /data/.octop/uploads

.PARAMETER Container
    Octop container name.  Default: octop

.PARAMETER Network
    Optional Docker network to attach the Octop container to (the network that
    contains NapCat), so ws://<napcat-container>:3001 resolves.

.PARAMETER SkipUpload
    Reuse the plugin files that are already on the Docker host.

.PARAMETER NoRestart
    Do not restart the container (restart or hot-reload it yourself).

.EXAMPLE
    .\install.ps1 -Remote user@docker-host -HostUploadsDir /srv/octop/data/uploads

.EXAMPLE
    .\install.ps1 -Remote user@docker-host -HostUploadsDir /srv/octop/data/uploads -Network napcat_net
#>
param(
    [Parameter(Mandatory = $true)][string]$Remote,
    [Parameter(Mandatory = $true)][string]$HostUploadsDir,
    [string]$ContainerUploadsDir = "/data/.octop/uploads",
    [string]$Container = "octop",
    [string]$Network = "",
    [switch]$SkipUpload,
    [switch]$NoRestart
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$plugins = @("octop-onebot11", "octop-onebot11-tools")

if ($HostUploadsDir -notmatch '^/[A-Za-z0-9_./-]+$') {
    throw "-HostUploadsDir must be an absolute path without spaces or quotes"
}
if ($Container -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') {
    throw "-Container must be a plain container name"
}
if ($Network -and $Network -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') {
    throw "-Network must be a plain network name"
}
if ($ContainerUploadsDir -notmatch '^/[A-Za-z0-9_./-]+$') {
    throw "-ContainerUploadsDir must be an absolute path without spaces or quotes"
}

function Invoke-Remote([string]$Command) {
    ssh $Remote $Command
    if ($LASTEXITCODE -ne 0) { throw "remote command failed: $Command" }
}

$hostDir = $HostUploadsDir.TrimEnd('/')
$containerDir = $ContainerUploadsDir.TrimEnd('/')

Write-Host "[1/5] ensure remote upload directory" -ForegroundColor Cyan
Invoke-Remote "mkdir -p '$hostDir'"

if (-not $SkipUpload) {
    Write-Host "[2/5] upload plugins" -ForegroundColor Cyan
    foreach ($plugin in $plugins) {
        # Drop the previous copy first so files removed from a plugin do not linger.
        Invoke-Remote "rm -rf '$hostDir/$plugin'"
        scp -r (Join-Path $root "plugins\$plugin") "${Remote}:${hostDir}/"
        if ($LASTEXITCODE -ne 0) { throw "scp $plugin failed" }
        Invoke-Remote "find '$hostDir/$plugin' -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null; true"
    }
} else {
    Write-Host "[2/5] upload skipped" -ForegroundColor Yellow
}

Write-Host "[3/5] install plugins inside the container" -ForegroundColor Cyan
foreach ($plugin in $plugins) {
    Invoke-Remote "docker exec $Container octop plugin install '$containerDir/$plugin' --force"
}

if ($Network) {
    Write-Host "[4/5] attach $Container to Docker network $Network" -ForegroundColor Cyan
    ssh $Remote "docker network connect $Network $Container 2>/dev/null; true"
} else {
    Write-Host "[4/5] no -Network given, skipping network attach" -ForegroundColor Yellow
}

if (-not $NoRestart) {
    Write-Host "[5/5] restart the container so the plugins load" -ForegroundColor Cyan
    Invoke-Remote "docker restart $Container"
} else {
    Write-Host "[5/5] restart skipped (-NoRestart)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Done. Next steps:" -ForegroundColor Green
Write-Host "  1. Dashboard -> Agent -> Channels -> add 'OneBot 11 (NapCat)' and fill in ws_url / token."
Write-Host "     Reverse mode (NapCat dials Octop) needs a token unless it listens on 127.0.0.1 only."
Write-Host "  2. Change an existing channel with scripts/patch_channel.py (read-merge-write; secrets via"
Write-Host "     --set-env). Never call 'octop channel patch --config' directly: it REPLACES the whole config."
Write-Host "  3. Check the startup log:"
Write-Host "       ssh $Remote `"docker logs --tail 100 $Container | grep -i onebot11`""
