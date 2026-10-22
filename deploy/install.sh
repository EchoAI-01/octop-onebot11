#!/usr/bin/env bash
# Cross-deployment installer for the octop-onebot11 plugins (R2).
#
# Supports two Octop deployment shapes:
#   systemd — Octop runs on the host as octop.service (bare metal / VM)
#   docker  — Octop runs as a container (default name: octop)
#
# The script uploads the plugin directories, installs them with the Octop CLI,
# makes them take effect (hot reload when a token is given, restart otherwise)
# and verifies the "applied ... plugin channel kind(s)" startup log line.
#
# Examples (replace user@octop-host with your own SSH target):
#   ./deploy/install.sh --host user@octop-host
#   ./deploy/install.sh --host user@octop-host --mode docker --container octop
#   ./deploy/install.sh --mode systemd                          # run on the Octop host itself
#   ./deploy/install.sh --host user@octop-host --plugin tools   # only the tool plugin
#   ./deploy/install.sh --host user@octop-host --plugin skills  # only the agent skill (docs for the agent)
#   OCTOP_RELOAD_TOKEN=... ./deploy/install.sh --host user@octop-host   # hot reload, no restart
#
# The reload token can also be given as --reload-token; the environment
# variable is preferred because command-line arguments are visible to every
# local user in the process list.
#
# Dashboard dropdowns: Octop's stock dashboard renders plugin "select" fields
# as plain text boxes.  Unless --no-dashboard-patch is given, the channel plugin
# install is followed by scripts/patch_dashboard.py, which turns the connection
# mode / reply-policy fields into Chinese dropdowns with help text.  The step is
# best-effort (a changed dashboard bundle only produces a warning), idempotent,
# and reversible with:  python3 scripts/patch_dashboard.py [--host H] --restore
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MODE="auto"          # auto | docker | systemd
HOST=""              # empty = run locally on this machine
CONTAINER="octop"    # docker container name
UPLOADS_DIR=""       # systemd: plugin upload dir (default: <remote $HOME>/.octop/uploads)
CLI_PATH=""          # systemd: octop binary (default: auto-detect)
RELOAD_TOKEN="${OCTOP_RELOAD_TOKEN:-}"  # admin token for POST /api/plugins/reload (optional)
API_URL=""           # e.g. http://127.0.0.1:17792 (default: auto-detect)
PLUGIN="all"         # channel | tools | skills | all
DASHBOARD_PATCH=1    # 1 = patch the dashboard bundle after installing the channel plugin

usage() {
  # Print only the leading comment block of this file.
  awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"
  exit 0
}

die() { echo "install.sh: $*" >&2; exit 1; }

# Run a single command string on the target host (or locally when --host is empty).
# The command string must be passed as ONE argument, preferably in single quotes
# so that $VARs expand on the target side, not here.
run() {
  if [[ -n "$HOST" ]]; then ssh "$HOST" "$1"; else bash -c "$1"; fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:?}"; shift 2 ;;
    --host) HOST="${2:?}"; shift 2 ;;
    --container) CONTAINER="${2:?}"; shift 2 ;;
    --uploads-dir) UPLOADS_DIR="${2:?}"; shift 2 ;;
    --cli) CLI_PATH="${2:?}"; shift 2 ;;
    --reload-token) RELOAD_TOKEN="${2:?}"; shift 2 ;;
    --api-url) API_URL="${2:?}"; shift 2 ;;
    --plugin) PLUGIN="${2:?}"; shift 2 ;;
    --no-dashboard-patch) DASHBOARD_PATCH=0; shift ;;
    -h|--help) usage ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

[[ "$PLUGIN" =~ ^(channel|tools|skills|all)$ ]] || die "--plugin must be channel|tools|skills|all"
[[ "$MODE" =~ ^(auto|docker|systemd)$ ]] || die "--mode must be auto|docker|systemd"
[[ "$CONTAINER" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || die "--container must be a plain container name"
# These values are spliced into single-quoted remote shell strings.
for _value in "$UPLOADS_DIR" "$CLI_PATH" "$API_URL"; do
  [[ "$_value" != *"'"* ]] || die "single quotes are not allowed in --uploads-dir / --cli / --api-url"
done

PLUGINS=()
if [[ "$PLUGIN" == "all" || "$PLUGIN" == "channel" ]]; then PLUGINS+=("octop-onebot11"); fi
if [[ "$PLUGIN" == "all" || "$PLUGIN" == "tools" ]]; then PLUGINS+=("octop-onebot11-tools"); fi
if [[ "$PLUGIN" == "all" || "$PLUGIN" == "skills" ]]; then PLUGINS+=("octop-onebot11-skills"); fi

# --------------------------------------------------------------------------
# Detection
# --------------------------------------------------------------------------

detect_mode() {
  if [[ "$MODE" != "auto" ]]; then echo "$MODE"; return; fi
  local found=""
  if run 'systemctl is-active --quiet octop.service 2>/dev/null'; then found="systemd"; fi
  if run "docker ps --format '{{.Names}}' 2>/dev/null | grep -qx '$CONTAINER'"; then
    if [[ -n "$found" ]]; then echo "ambiguous"; else found="docker"; fi
  fi
  if [[ -z "$found" ]]; then die "auto-detect found neither an active octop.service nor container '$CONTAINER'; pass --mode explicitly"; fi
  echo "$found"
}

detect_cli() {
  if [[ -n "$CLI_PATH" ]]; then echo "$CLI_PATH"; return; fi
  local candidate
  for candidate in /opt/octop/venv/bin/octop /usr/local/bin/octop /usr/bin/octop; do
    if run "test -x '$candidate'"; then echo "$candidate"; return; fi
  done
  echo "octop" # last resort: whatever is on PATH on the target
}

detect_api_url() {
  if [[ -n "$API_URL" ]]; then echo "$API_URL"; return; fi
  local addr
  addr=$(run "ss -tlnp 2>/dev/null | awk '/users:\(\(\"octop/ && /0\.0\.0\.0:/ {print \$4; exit}'")
  [[ -n "$addr" ]] || return 0
  echo "http://127.0.0.1:${addr##*:}"
}

DEPLOY_MODE="$(detect_mode)"
if [[ "$DEPLOY_MODE" == "ambiguous" ]]; then
  die "both an active octop.service and container '$CONTAINER' exist; pass --mode docker|systemd explicitly"
fi
echo "[1/5] deploy mode: $DEPLOY_MODE (host: ${HOST:-local})"

# octop <args...> — invoke the Octop CLI on the target.
octop_cli() {
  if [[ "$DEPLOY_MODE" == "docker" ]]; then
    run "docker exec '$CONTAINER' octop $1"
  else
    run "'$(detect_cli)' $1"
  fi
}

# --------------------------------------------------------------------------
# Upload + install
# --------------------------------------------------------------------------

upload_dir() { # upload_dir <name> — places plugins/<name> on the target, prints its path
  local name="$1" src="$ROOT/plugins/$name" dst
  [[ -d "$src" ]] || die "missing plugin source: $src"
  if [[ "$DEPLOY_MODE" == "docker" ]]; then
    # Copy into the container via a host temp dir; no bind-mount assumptions.
    local tmp
    tmp="$(run 'mktemp -d')"
    if [[ -n "$HOST" ]]; then
      if ! rsync -a --delete --exclude '__pycache__' "$src/" "$HOST:$tmp/$name/" 2>/dev/null; then
        # No rsync on one side: copy the directory *contents* (copying "$src"
        # itself would nest it as $tmp/$name/$name).
        run "rm -rf '$tmp/$name' && mkdir -p '$tmp/$name'"
        scp -r "$src/." "$HOST:$tmp/$name/"
        run "find '$tmp/$name' -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true"
      fi
    else
      mkdir -p "$tmp/$name"
      cp -R "$src/." "$tmp/$name/"
    fi
    run "docker exec '$CONTAINER' rm -rf '/tmp/$name'"
    run "docker cp '$tmp/$name' '$CONTAINER:/tmp/$name'"
    run "rm -rf '$tmp'"
    echo "/tmp/$name"
  else
    dst="${UPLOADS_DIR:-$(run 'echo $HOME')/.octop/uploads}"
    run "mkdir -p '$dst/$name'"
    if [[ -n "$HOST" ]]; then
      if ! rsync -a --delete --exclude '__pycache__' "$src/" "$HOST:$dst/$name/" 2>/dev/null; then
        run "rm -rf '$dst/$name' && mkdir -p '$dst/$name'"
        scp -r "$src/." "$HOST:$dst/$name/"
      fi
      run "find '$dst/$name' -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true"
    else
      rsync -a --delete --exclude '__pycache__' "$src/" "$dst/$name/"
    fi
    echo "$dst/$name"
  fi
}

echo "[2/5] uploading plugins: ${PLUGINS[*]}"
for name in "${PLUGINS[@]}"; do
  target_dir="$(upload_dir "$name")"
  echo "[3/5] installing $name from $target_dir"
  octop_cli "plugin install '$target_dir' --force"
done

# --------------------------------------------------------------------------
# Take effect: hot reload preferred, restart as fallback
# --------------------------------------------------------------------------

reload_ok() {
  [[ -n "$RELOAD_TOKEN" ]] || return 1
  local url status
  url="$(detect_api_url)"
  [[ -n "$url" ]] || return 1
  # The token travels on stdin (curl -H @-), never inside a command line, so it
  # does not appear in `ps` here or on the target and cannot break the quoting.
  status="$(printf 'Authorization: Bearer %s\n' "$RELOAD_TOKEN" \
    | run "curl -s -o /dev/null -w '%{http_code}' -X POST -H @- '$url/api/plugins/reload'")" || return 1
  [[ "$status" == "200" ]]
}

echo "[4/5] applying plugins"
# Remember where the log stands *before* applying anything (reload or restart),
# so verification can only match lines written by this very deployment.
LOG_MARKER=0
SINCE=""
if [[ "$DEPLOY_MODE" == "docker" ]]; then
  SINCE="$(run 'date -u +%Y-%m-%dT%H:%M:%SZ')"
else
  LOG_MARKER="$(run 'wc -l < "$HOME/.octop/logs/octop.log" 2>/dev/null || echo 0')"
fi
if reload_ok; then
  echo "      hot reload succeeded (no restart needed)"
  sleep 2
else
  if [[ -n "$RELOAD_TOKEN" ]]; then echo "      hot reload failed, falling back to restart"; fi
  if [[ "$DEPLOY_MODE" == "docker" ]]; then
    run "docker restart '$CONTAINER'"
  else
    if ! run 'systemctl restart octop'; then
      # systemd start-rate limit (StartLimitBurst) after frequent restarts.
      echo "      restart hit the systemd start limit; retrying after reset-failed"
      run 'systemctl reset-failed octop.service'
      run 'systemctl restart octop'
    fi
  fi
fi

# --------------------------------------------------------------------------
# Dashboard dropdowns (best-effort)
# --------------------------------------------------------------------------

patch_dashboard() {
  local script="$ROOT/scripts/patch_dashboard.py" python_bin
  python_bin="$(command -v python3 || command -v python || true)"
  if [[ -z "$python_bin" ]]; then
    echo "      skipped: python3 not found on this machine"; return 1
  fi
  local -a args=()
  [[ -n "$HOST" ]] && args+=(--host "$HOST")
  if [[ "$DEPLOY_MODE" == "docker" ]]; then args+=(--container "$CONTAINER"); fi
  # bash 3.2 (macOS) + `set -u`: an empty array only expands safely this way.
  "$python_bin" "$script" ${args[@]+"${args[@]}"}
}

if [[ "$DASHBOARD_PATCH" == "1" && " ${PLUGINS[*]} " == *" octop-onebot11 "* ]]; then
  echo "[+] patching the dashboard so reply-policy fields render as dropdowns"
  if ! patch_dashboard; then
    echo "      WARNING: dashboard patch failed; plugin fields fall back to plain text boxes." >&2
    echo "      Run scripts/patch_dashboard.py manually, or ignore this if you do not need dropdowns." >&2
  else
    echo "      done — hard-reload the channels page once (Cmd/Ctrl+Shift+R)"
  fi
fi

# --------------------------------------------------------------------------
# Verify
# --------------------------------------------------------------------------

echo "[5/5] verifying"
boot_log=""
if [[ "$DEPLOY_MODE" == "docker" ]]; then
  run "docker ps --format '{{.Names}} {{.Status}}' | grep -q '^$CONTAINER ' || exit 1" || die "container not running"
  for _ in $(seq 1 15); do
    boot_log="$(run "docker logs --since '$SINCE' '$CONTAINER' 2>&1 | grep 'applied [0-9]* plugin channel kind' | tail -1" || true)"
    [[ -n "$boot_log" ]] && break
    sleep 2
  done
else
  run 'systemctl is-active --quiet octop.service' || die "octop.service is not active"
  for _ in $(seq 1 15); do
    boot_log="$(run "tail -n +$((LOG_MARKER + 1)) \"\$HOME/.octop/logs/octop.log\" 2>/dev/null | grep 'applied [0-9]* plugin channel kind' | tail -1" || true)"
    [[ -n "$boot_log" ]] && break
    sleep 2
  done
fi

if [[ " ${PLUGINS[*]} " == *" octop-onebot11 "* ]]; then
  if ! grep -q "applied [0-9]* plugin channel kind" <<<"$boot_log" || ! grep -q "onebot11" <<<"$boot_log"; then
    die "channel kind onebot11 not found in the fresh startup log; check octop logs for plugin errors"
  fi
  echo "      $(grep "applied [0-9]* plugin channel kind" <<<"$boot_log" | tail -1)"
else
  echo "      octop service is healthy; tool / skill plugins install silently (verify via plugin list)"
fi

octop_cli "plugin list" || true
echo "Done."
