<#
.SYNOPSIS
    Install the Octop OneBot 11 (NapCat) plugins into a Dockerised Octop over SSH.

.DESCRIPTION
    Windows helper for Octop running in a Docker container.  On Linux, macOS,
    WSL or Git Bash prefer deploy/install.sh (it also handles systemd installs,
    hot reload and post-install verification).

    Steps: upload plugins/octop-onebot11, plugins/octop-onebot11-tools and plugins/octop-onebot11-skills to
    the Docker host, install them with the Octop CLI inside the container,
    optionally attach the container to the Docker network NapCat lives in, and
    restart the container.  NapCat does not need to be logged in for this.

    Verification status: this script has been reviewed and regression-checked
    statically (see tools/test_plugin_regression.py), but it has NOT been run on
    a Windows machine by the maintainers.  Run it with -WhatIf first: that
    prints every remote command without executing anything.  Reports from a real
    Windows run are welcome.

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

.PARAMETER NoVerify
    Skip waiting for the "applied N plugin channel kind(s)" startup log line.

.PARAMETER WhatIf
    Print the ssh/scp commands that would run, without running any of them.

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
    [switch]$NoRestart,
    [switch]$NoVerify,
    [switch]$WhatIf
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$plugins = @("octop-onebot11", "octop-onebot11-tools", "octop-onebot11-skills")

# A value starting with "-" would be parsed by ssh/scp as an option
# (e.g. -oProxyCommand=...), which is a command-injection vector.
if ($Remote -notmatch '^[A-Za-z0-9_][A-Za-z0-9_.@:-]*$' -or $Remote.StartsWith('-')) {
    throw "-Remote must be user@host or an ssh config alias (no spaces, must not start with '-')"
}
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
if ($HostUploadsDir.Contains('..') -or $ContainerUploadsDir.Contains('..')) {
    throw "upload directories must not contain '..'"
}
foreach ($tool in @("ssh", "scp")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool was not found on PATH (install the Windows OpenSSH client: Settings > Optional features)"
    }
}
foreach ($plugin in $plugins) {
    if (-not (Test-Path -LiteralPath (Join-Path (Join-Path $root "plugins") $plugin) -PathType Container)) {
        throw "missing plugin directory: plugins/$plugin (run the script from a full checkout)"
    }
}

function Invoke-Remote([string]$Command, [switch]$IgnoreFailure) {
    if ($WhatIf) {
        Write-Host "  [WhatIf] ssh -- $Remote $Command" -ForegroundColor DarkGray
        return
    }
    # "--" ends ssh option parsing, so $Remote can never be taken for an option.
    ssh -- $Remote $Command
    if ($LASTEXITCODE -ne 0 -and -not $IgnoreFailure) { throw "remote command failed: $Command" }
}

function Copy-ToRemote([string]$LocalDir, [string]$RemoteDir) {
    # Copy the directory *contents* into an already-created target: copying the
    # directory itself would nest it as <target>/<name>/<name> when it exists.
    if ($WhatIf) {
        Write-Host "  [WhatIf] scp -r $LocalDir/. ${Remote}:${RemoteDir}/" -ForegroundColor DarkGray
        return
    }
    scp -r -- (Join-Path $LocalDir ".") "${Remote}:${RemoteDir}/"
    if ($LASTEXITCODE -ne 0) { throw "scp to $RemoteDir failed" }
}

$hostDir = $HostUploadsDir.TrimEnd('/')
$containerDir = $ContainerUploadsDir.TrimEnd('/')

Write-Host "[1/5] ensure remote upload directory" -ForegroundColor Cyan
Invoke-Remote "mkdir -p '$hostDir'"

if (-not $SkipUpload) {
    Write-Host "[2/5] upload plugins" -ForegroundColor Cyan
    foreach ($plugin in $plugins) {
        # Drop the previous copy first so files removed from a plugin do not linger.
        Invoke-Remote "rm -rf '$hostDir/$plugin' && mkdir -p '$hostDir/$plugin'"
        Copy-ToRemote (Join-Path (Join-Path $root "plugins") $plugin) "$hostDir/$plugin"
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
    # Re-running the script finds the container already attached: that is fine.
    Invoke-Remote "docker network connect $Network $Container 2>/dev/null; true" -IgnoreFailure
} else {
    Write-Host "[4/5] no -Network given, skipping network attach" -ForegroundColor Yellow
}

$since = ""
if (-not $WhatIf) {
    $since = (& ssh -- $Remote "date -u +%Y-%m-%dT%H:%M:%SZ" | Select-Object -First 1)
    if ($LASTEXITCODE -ne 0 -or $since -notmatch '^[0-9TZ:-]+$') { $since = "" }
}

if (-not $NoRestart) {
    Write-Host "[5/5] restart the container so the plugins load" -ForegroundColor Cyan
    Invoke-Remote "docker restart $Container"
    if (-not $NoVerify -and -not $WhatIf) {
        Write-Host "      waiting for the plugin to register ..." -ForegroundColor Cyan
        $applied = ""
        for ($attempt = 0; $attempt -lt 15 -and -not $applied; $attempt++) {
            Start-Sleep -Seconds 2
            $sinceArg = if ($since) { "--since '$since' " } else { "" }
            $applied = (& ssh -- $Remote "docker logs $sinceArg$Container 2>&1 | grep 'applied [0-9]* plugin channel kind' | tail -1") -join ""
        }
        if ($applied -match 'onebot11') {
            Write-Host "      $applied" -ForegroundColor Green
        } else {
            Write-Warning "did not see the onebot11 channel kind in the fresh startup log; check: docker logs $Container | grep -i onebot11"
        }
    }
} else {
    Write-Host "[5/5] restart skipped (-NoRestart)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Done. Next steps:" -ForegroundColor Green
Write-Host "  1. Dashboard -> Agent -> Channels -> add 'OneBot 11 (NapCat)' and fill in ws_url / token."
Write-Host "     Reverse mode (NapCat dials Octop) needs a token unless it listens on 127.0.0.1 only."
Write-Host "  2. Change an existing channel with scripts/patch_channel.py (read-merge-write; secrets via"
Write-Host "     --set-env). Never call 'octop channel patch --config' directly: it REPLACES the whole config."
Write-Host "  3. Optional: turn the mode / reply-policy fields into Chinese dropdowns by running, on a machine with"
Write-Host "     Python 3:  python scripts/patch_dashboard.py --host $Remote --container $Container"
Write-Host "  4. Check the startup log:"
Write-Host "       ssh $Remote `"docker logs --tail 100 $Container | grep -i onebot11`""
