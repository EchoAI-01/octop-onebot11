#!/usr/bin/env python3
"""Find a NapCat on this machine and print the channel settings that reach it (R6).

Run it on the machine that runs Octop (that is the vantage point the channel
will connect from).  It only reads: nothing is written or reconfigured.

    python3 scripts/probe_napcat.py                       # 127.0.0.1, ports 3001 3000 3002 6700 8080
    python3 scripts/probe_napcat.py --port 3011 --docker  # extra port + look at docker containers
    python3 scripts/probe_napcat.py --token-env NAPCAT_TOKEN
    python3 scripts/probe_napcat.py --json                # machine-readable

Needs the ``websockets`` package (use the Octop virtualenv's python when the
system one lacks it).  Exit status: 0 when at least one OneBot 11 service
answered, 1 when none did.

To store a result in an existing channel without clobbering its other keys,
use ``scripts/patch_channel.py``; the summary prints the matching ``--set`` flags.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_probe():
    path = ROOT / "plugins" / "octop-onebot11" / "probe.py"
    spec = importlib.util.spec_from_file_location("octop_onebot11_probe", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses need the module registered
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", action="append", default=[], help="host to scan (repeatable; default 127.0.0.1)")
    parser.add_argument("--port", action="append", default=[], help="extra port to try (repeatable)")
    parser.add_argument("--token", default="", help="access token to try (prefer --token-env: argv is visible in ps)")
    parser.add_argument("--token-env", default="", help="name of an environment variable holding the token")
    parser.add_argument("--docker", action="store_true", help="also inspect running docker containers named like napcat")
    parser.add_argument("--timeout", type=float, default=3.0, help="per-endpoint timeout in seconds (default 3)")
    parser.add_argument("--json", action="store_true", help="print JSON instead of text")
    args = parser.parse_args()

    probe = load_probe()
    token = args.token or (os.environ.get(args.token_env, "") if args.token_env else "")
    hosts = tuple(args.host) or probe.DEFAULT_HOSTS
    remote = [host for host in hosts if not probe.is_local_address(host)]
    if remote:
        print(f"拒绝扫描非本机/内网地址: {', '.join(remote)}", file=sys.stderr)
        return 2
    extra = probe.parse_ports(",".join(args.port))
    ports = tuple(dict.fromkeys((*extra, *probe.DEFAULT_PORTS)))

    results = asyncio.run(
        probe.scan(hosts, ports, token=token, timeout=max(0.5, args.timeout), include_docker=args.docker)
    )
    usable = [c for c in results if c.usable]
    if args.json:
        print(json.dumps([c.as_dict() for c in results], ensure_ascii=False, indent=2))
    else:
        print(probe.render_text(results, token_given=bool(token)))
        logged_in = [c for c in usable if c.status == "onebot"]
        if logged_in:
            best = logged_in[0]
            print()
            print("写入已有通道（不会丢失其他配置项）：")
            print(
                "  python3 scripts/patch_channel.py --agent <AGENT_ID> --channel <CHANNEL_ID> "
                f"--set connection_mode=forward --set ws_url={best.url}"
                + (" --set-env token=<存放令牌的环境变量名>" if token else "")
            )
    return 0 if usable else 1


if __name__ == "__main__":
    raise SystemExit(main())
