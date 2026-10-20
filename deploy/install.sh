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
# Examples:
#   ./deploy/install.sh --host root@43.155.171.15
#   ./deploy/install.sh --host root@1.2.3.4 --mode docker --container octop
#   ./deploy/install.sh --mode systemd                      # run on the Octop host itself
#   ./deploy/install.sh --host root@1.2.3.4 --plugin tools  # only the tool plugin
#   ./deploy/install.sh --host root@1.2.3.4 --reload-token "$OCTOP_ADMIN_TOKEN"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MODE="auto"          # auto | docker | systemd
HOST=""              # empty = run locally on this machine
CONTAINER="octop"    # docker container name
UPLOADS_DIR=""       # systemd: plugin upload dir (default: <remote $HOME>/.octop/uploads)
CLI_PATH=""          # systemd: octop binary (default: auto-detect)
RELOAD_TOKEN=""      # admin token for POST /api/plugins/reload (optional)
API_URL=""           # e.g. http://127.0.0.1:17792 (default: auto-detect)
PLUGIN="all"         # channel | tools | all

usage() {
  sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
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
    -h|--help) usage ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

[[ "$PLUGIN" =~ ^(channel|tools|all)$ ]] || die "--plugin must be channel|tools|all"
[[ "$MODE" =~ ^(auto|docker|systemd)$ ]] || die "--mode must be auto|docker|systemd"

PLUGINS=()
if [[ "$PLUGIN" == "all" || "$PLUGIN" == "channel" ]]; then PLUGINS+=("octop-onebot11"); fi
if [[ "$PLUGIN" == "all" || "$PLUGIN" == "tools" ]]; then PLUGINS+=("octop-onebot11-tools"); fi

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
        run "mkdir -p '$tmp/$name'"
        scp -r "$src" "$HOST:$tmp/$name/"
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
        run "mkdir -p '$dst/$name'"
        scp -r "$src" "$HOST:$dst/"
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
  status="$(run "curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Authorization: Bearer $RELOAD_TOKEN' '$url/api/plugins/reload'")" || return 1
  [[ "$status" == "200" ]]
}

echo "[4/5] applying plugins"
LOG_MARKER=0 # octop.log line count before the restart; verify only lines after it
if reload_ok; then
  echo "      hot reload succeeded (no restart needed)"
  sleep 2
else
  if [[ -n "$RELOAD_TOKEN" ]]; then echo "      hot reload failed, falling back to restart"; fi
  if [[ "$DEPLOY_MODE" == "docker" ]]; then
    run "docker restart '$CONTAINER'"
  else
    LOG_MARKER="$(run 'wc -l < "$HOME/.octop/logs/octop.log" 2>/dev/null || echo 0')"
    if ! run 'systemctl restart octop'; then
      # systemd start-rate limit (StartLimitBurst) after frequent restarts.
      echo "      restart hit the systemd start limit; retrying after reset-failed"
      run 'systemctl reset-failed octop.service'
      run 'systemctl restart octop'
    fi
  fi
fi

# --------------------------------------------------------------------------
# Verify
# --------------------------------------------------------------------------

echo "[5/5] verifying"
if [[ "$DEPLOY_MODE" == "docker" ]]; then
  run "docker ps --format '{{.Names}} {{.Status}}' | grep -q '^$CONTAINER ' || die 'container not running'"
  sleep 4
  boot_log="$(run "docker logs --since 2m '$CONTAINER' 2>&1")"
else
  run 'systemctl is-active --quiet octop.service' || die "octop.service is not active"
  boot_log=""
  for _ in $(seq 1 15); do
    boot_log="$(run "tail -n +$((LOG_MARKER + 1)) \"\$HOME/.octop/logs/octop.log\" 2>/dev/null | grep 'applied [0-9]* plugin channel kind' | tail -1")"
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
  echo "      octop service is healthy; tool plugin installs silently (verify via plugin list)"
fi

octop_cli "plugin list" || true
echo "Done."
