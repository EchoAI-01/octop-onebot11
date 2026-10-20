#!/usr/bin/env python3
"""R1 Phase 1: safe read-merge-write wrapper around ``octop channel patch``.

Why this exists
---------------
``octop channel patch --config`` REPLACES the whole ``config_json`` (verified
on Octop v1.0.2): patching a single key silently drops every other key.  On
top of that ``octop channel get`` masks secrets (``"token": "secr****"``), so
a naive read-modify-write would store the mask as the real token.

This script closes both holes:

1. Reads the current config (``octop channel get``) — or a trusted base file.
2. Detects masked secret values and REFUSES to run unless every masked field
   is explicitly provided via ``--set`` (or a ``--base-file`` that contains
   the real value).  Masked values are never written back.
3. Shallow-merges ``--set key=value`` overrides on top.
4. Prints the merged config, then calls ``channel patch`` and optionally
   restarts the service (CLI writes only touch the database).

Examples
--------
    # Merge two keys on a remote host, then restart octop:
    python3 scripts/patch_channel.py --host root@43.155.171.15 \
        --cli /opt/octop/venv/bin/octop --agent VT9BCM --channel 01M5... \
        --set group_reply_mode=always --set trigger_keywords=日报,提醒 --restart

    # Dry run: only show the merged config:
    python3 scripts/patch_channel.py ... --set token=new-token --dry-run

    # Authoritative base file (keep it out of git, chmod 600):
    python3 scripts/patch_channel.py ... --base-file ~/onebot11.base.json --set ...

Value parsing: each ``--set`` value is first parsed as JSON (so ``5`` becomes
an int, ``true`` a bool), falling back to the raw string.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

MASK_SUFFIX = "****"


def _parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


def _run(argv: list[str], *, check: bool = True) -> str:
    result = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if check and result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"command failed ({result.returncode}): {shlex.join(argv)}")
    return result.stdout


def _octop(args: argparse.Namespace, cli_args: list[str]) -> str:
    if args.host:
        # ssh joins argv with spaces and re-parses on the remote shell, so
        # every argument (JSON payloads especially) must be shell-quoted.
        remote = " ".join(shlex.quote(str(part)) for part in [args.cli, *cli_args])
        return _run(["ssh", args.host, remote])
    return _run([args.cli, *cli_args])


def _looks_masked(value: Any) -> bool:
    return isinstance(value, str) and value.endswith(MASK_SUFFIX) and len(value) > len(MASK_SUFFIX)


def load_current_config(args: argparse.Namespace) -> dict[str, Any]:
    raw = _octop(args, ["channel", "get", args.channel, "--agent", args.agent])
    data = json.loads(raw)
    config = data.get("config")
    if not isinstance(config, dict):
        raise SystemExit(f"channel {args.channel} has no config object: {raw[:400]}")
    return dict(config)


def _apply_via_api(args: argparse.Namespace, merged: dict[str, Any]) -> str:
    """PATCH the channel through Octop's HTTP API — config takes effect at once.

    The gateway re-registers the channel on PATCH (update_channel → unregister
    → register), so no restart is needed.  Requires a dashboard JWT with the
    ``channels`` permission (``--api-token``).
    """
    url = f"{args.api_url.rstrip('/')}/api/v1/agents/{args.agent}/channels/{args.channel}"
    body = json.dumps({"config": merged}, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if args.api_token:
        headers["Authorization"] = f"Bearer {args.api_token}"
    request = urllib.request.Request(url, data=body, headers=headers, method="PATCH")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:400]
        raise SystemExit(f"API PATCH failed (HTTP {exc.code}): {detail}") from exc
    except (urllib.error.URLError, OSError) as exc:
        raise SystemExit(f"API PATCH unreachable: {exc}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-merge-write wrapper for `octop channel patch` (replaces whole config otherwise)."
    )
    parser.add_argument("--host", default="", help="SSH target running Octop (empty = local)")
    parser.add_argument("--cli", default="octop", help="octop binary on the target")
    parser.add_argument(
        "--api-url",
        default="",
        help="Octop HTTP API base (e.g. http://127.0.0.1:17792); write via PATCH — takes effect without restart",
    )
    parser.add_argument(
        "--api-token",
        default="",
        help="dashboard JWT for --api-url (Authorization: Bearer)",
    )
    parser.add_argument("--agent", required=True, help="agent id owning the channel")
    parser.add_argument("--channel", required=True, help="channel id to patch")
    parser.add_argument(
        "--set",
        dest="sets",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="config override; repeatable; VALUE parsed as JSON then raw string",
    )
    parser.add_argument(
        "--base-file",
        default="",
        help="trusted JSON file with the full config including real secrets (never write masked values)",
    )
    parser.add_argument("--dry-run", action="store_true", help="print merged config and exit")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="restart octop after patching (default: systemctl restart octop)",
    )
    parser.add_argument(
        "--restart-cmd",
        default="systemctl restart octop",
        help="command used by --restart (e.g. 'docker restart octop')",
    )
    args = parser.parse_args()

    overrides: dict[str, Any] = {}
    for item in args.sets:
        if "=" not in item:
            raise SystemExit(f"--set expects KEY=VALUE, got: {item!r}")
        key, _, raw_value = item.partition("=")
        key = key.strip()
        if not key:
            raise SystemExit(f"--set has an empty key: {item!r}")
        overrides[key] = _parse_value(raw_value)

    if args.base_file:
        with open(args.base_file, "r", encoding="utf-8") as handle:
            base = json.load(handle)
        if not isinstance(base, dict):
            raise SystemExit(f"--base-file must contain a JSON object: {args.base_file}")
        masked_in_base = [key for key, value in base.items() if _looks_masked(value)]
        if masked_in_base:
            raise SystemExit(
                f"--base-file contains masked-looking values for {masked_in_base}; "
                "fix the file with real values (or drop the keys)"
            )
    else:
        base = load_current_config(args)

    masked = sorted(key for key, value in base.items() if _looks_masked(value))
    missing = [key for key in masked if key not in overrides]
    if missing:
        raise SystemExit(
            "refusing to patch: fields {missing} are masked by `channel get` and would be "
            "lost or corrupted by the replace-style patch. Provide them via --set "
            "{missing}=VALUE (or pass --base-file with real values).".format(
                missing=", ".join(missing)
            )
        )

    merged = dict(base)
    for key in masked:
        merged.pop(key, None)  # never write the mask back
    merged.update(overrides)

    print("Merged config:")
    print(json.dumps(merged, ensure_ascii=False, indent=2))
    if args.dry_run:
        print("dry-run: nothing written")
        return 0

    if args.api_url:
        print(f"Patching channel {args.channel} via HTTP API (hot effect, no restart) ...")
        output = _apply_via_api(args, merged)
        print(output.strip()[:400] or "patched")
        print("applied: the gateway re-registered the channel with the new config")
        return 0

    payload = json.dumps(merged, ensure_ascii=False)
    print(f"Patching channel {args.channel} ...")
    output = _octop(
        args,
        ["channel", "patch", args.channel, "--agent", args.agent, "--config", payload],
    )
    print(output.strip() or "patched")
    print("NOTE: the running Gateway does not reload on its own — restart octop, call POST /api/plugins/reload, or use --api-url.")

    if args.restart:
        print(f"Restarting: {args.restart_cmd}")
        if args.host:
            _run(["ssh", args.host, args.restart_cmd])
        else:
            _run([*shlex.split(args.restart_cmd)])
        print("restarted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
