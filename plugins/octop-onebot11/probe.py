"""Find a NapCat on this machine (R6): TCP pre-check plus a ``get_login_info`` probe.

Used by ``scripts/probe_napcat.py`` (command line) and the
``onebot11_probe_napcat`` agent tool, so an operator no longer has to guess
``ws_url`` / ``token``.  Nothing here writes configuration: it only *reports*
what answers and what the channel settings would be.

A candidate is classified as

* ``onebot``       - answered the OneBot handshake (``logged_in`` says whether QQ is signed in),
* ``auth_required``- the WebSocket server exists but wants a token,
* ``not_onebot``   - something listens but does not speak OneBot over WebSocket,
* ``closed``       - nothing listens.

Only loopback / private addresses are scanned unless the caller names a host
explicitly, and every connection is short-lived (one handshake, two requests).
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from typing import Any

# NapCat's default WebSocket server is 3001 (HTTP 3000); the rest are common
# alternatives (a second account, go-cqhttp's 6700, generic 8080).
DEFAULT_PORTS: tuple[int, ...] = (3001, 3000, 3002, 6700, 8080)
DEFAULT_HOSTS: tuple[str, ...] = ("127.0.0.1",)

_MAX_CANDIDATES = 64


@dataclass
class Candidate:
    host: str
    port: int
    status: str = "closed"  # onebot | auth_required | not_onebot | closed
    logged_in: bool = False
    user_id: str = ""
    nickname: str = ""
    app_name: str = ""
    app_version: str = ""
    detail: str = ""
    label: str = ""  # e.g. docker container name

    @property
    def url(self) -> str:
        host = f"[{self.host}]" if ":" in self.host and not self.host.startswith("[") else self.host
        return f"ws://{host}:{self.port}"

    @property
    def usable(self) -> bool:
        return self.status in ("onebot", "auth_required")

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "url": self.url}


def is_local_address(host: str) -> bool:
    """Loopback, private, link-local or ``localhost`` - never a public address."""
    value = host.strip().strip("[]").lower()
    if value in ("localhost", "host.docker.internal"):
        return True
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return address.is_loopback or address.is_private or address.is_link_local


def parse_ports(text: str) -> tuple[int, ...]:
    """``"3001, 3002"`` -> ``(3001, 3002)``; invalid / out-of-range entries are skipped."""
    ports: dict[int, None] = {}
    for item in re.split(r"[,\s;]+", str(text or "")):
        if item.isdigit() and 1 <= int(item) <= 65535:
            ports.setdefault(int(item), None)
    return tuple(ports)


async def _await_echo(ws: Any, echo: str, timeout: float) -> dict[str, Any] | None:
    """Read frames until the reply carrying ``echo`` arrives (events are skipped)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return None
        try:
            raw = await asyncio.wait_for(ws.recv(), remaining)
        except asyncio.TimeoutError:
            return None
        try:
            frame = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if isinstance(frame, dict) and frame.get("echo") == echo:
            return frame


async def probe_endpoint(
    host: str,
    port: int,
    *,
    token: str = "",
    timeout: float = 3.0,
    label: str = "",
) -> Candidate:
    """Classify one ``host:port`` (see the module docstring for the statuses)."""
    candidate = Candidate(host=host, port=int(port), label=label)
    try:
        from websockets.asyncio.client import connect
        from websockets.exceptions import InvalidHandshake, InvalidStatus
    except ImportError:  # pragma: no cover - the plugin always has websockets
        candidate.status, candidate.detail = "not_onebot", "websockets 未安装"
        return candidate
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with connect(
            candidate.url,
            additional_headers=headers,
            open_timeout=timeout,
            ping_interval=None,
            max_size=1 << 20,
        ) as ws:
            await ws.send(json.dumps({"action": "get_login_info", "params": {}, "echo": "probe-login"}))
            login = await _await_echo(ws, "probe-login", timeout)
            if login is None:
                candidate.status, candidate.detail = "not_onebot", "WebSocket 已连通但没有 OneBot 应答"
                return candidate
            candidate.status = "onebot"
            data = login.get("data") if isinstance(login.get("data"), dict) else {}
            if login.get("status") == "ok" and data.get("user_id"):
                candidate.logged_in = True
                candidate.user_id = str(data.get("user_id"))
                candidate.nickname = str(data.get("nickname") or "")
            else:
                candidate.detail = "OneBot 服务可用，但 QQ 尚未登录"
            await ws.send(json.dumps({"action": "get_version_info", "params": {}, "echo": "probe-version"}))
            version = await _await_echo(ws, "probe-version", timeout)
            info = version.get("data") if isinstance(version, dict) and isinstance(version.get("data"), dict) else {}
            candidate.app_name = str(info.get("app_name") or "")
            candidate.app_version = str(info.get("app_version") or "")
    except InvalidStatus as exc:
        code = getattr(getattr(exc, "response", None), "status_code", 0)
        if code in (401, 403):
            candidate.status = "auth_required"
            candidate.detail = "需要访问令牌（token）" if not token else "令牌被拒绝（token 不正确）"
        else:
            candidate.status, candidate.detail = "not_onebot", f"HTTP {code}"
    except ConnectionRefusedError:
        pass  # nothing listens: stays "closed"
    except (TimeoutError, asyncio.TimeoutError):
        candidate.detail = "连接超时（端口被过滤或服务无响应）"
    except (InvalidHandshake, OSError, ValueError) as exc:
        candidate.status = "not_onebot"
        candidate.detail = f"{type(exc).__name__}"
    except Exception as exc:  # noqa: BLE001 - websockets raises assorted ConnectionClosed subclasses
        candidate.status = "not_onebot"
        candidate.detail = type(exc).__name__
    return candidate


def docker_candidates(timeout: float = 5.0) -> list[tuple[str, int, str]]:
    """``(host, port, container)`` for running containers that look like NapCat.

    Published ports map to 127.0.0.1; unpublished ones fall back to the
    container's bridge IP with NapCat's default port.  Silent (empty) when
    docker is missing or not permitted.
    """
    docker = shutil.which("docker")
    if not docker:
        return []
    try:
        listing = subprocess.run(
            [docker, "ps", "--format", "{{.Names}}\t{{.Image}}\t{{.Ports}}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if listing.returncode != 0:
        return []
    found: list[tuple[str, int, str]] = []
    for line in listing.stdout.splitlines():
        name, _, rest = line.partition("\t")
        image, _, ports = rest.partition("\t")
        if "napcat" not in f"{name} {image}".lower():
            continue
        published = [int(m) for m in re.findall(r":(\d+)->", ports)]
        if published:
            found.extend(("127.0.0.1", port, name) for port in published)
            continue
        try:
            inspect = subprocess.run(
                [docker, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", name],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        for address in inspect.stdout.split():
            if is_local_address(address):
                found.append((address, DEFAULT_PORTS[0], name))
    return found


async def scan(
    hosts: tuple[str, ...] = DEFAULT_HOSTS,
    ports: tuple[int, ...] = DEFAULT_PORTS,
    *,
    token: str = "",
    timeout: float = 3.0,
    include_docker: bool = False,
) -> list[Candidate]:
    """Probe every ``host x port`` (plus docker candidates); usable results first."""
    targets: dict[tuple[str, int], str] = {}
    for host in hosts:
        for port in ports:
            targets.setdefault((host, port), "")
    if include_docker:
        for host, port, name in await asyncio.to_thread(docker_candidates):
            targets.setdefault((host, port), name)
    pairs = list(targets.items())[:_MAX_CANDIDATES]
    results = await asyncio.gather(
        *[probe_endpoint(host, port, token=token, timeout=timeout, label=label) for (host, port), label in pairs]
    )
    order = {"onebot": 0, "auth_required": 1, "not_onebot": 2, "closed": 3}
    return sorted(results, key=lambda c: (order.get(c.status, 9), not c.logged_in, c.port))


def suggest_config(candidate: Candidate, token: str = "") -> dict[str, Any]:
    """Channel settings that would connect to this candidate (forward mode)."""
    config: dict[str, Any] = {"connection_mode": "forward", "ws_url": candidate.url}
    if token:
        config["token"] = token
    return config


def render_text(results: list[Candidate], *, token_given: bool = False) -> str:
    """Operator-facing summary (Chinese, plain text)."""
    usable = [c for c in results if c.usable]
    if not usable:
        scanned = "、".join(f"{c.host}:{c.port}" for c in results) or "（无）"
        others = [c for c in results if c.status == "not_onebot"]
        lines = [f"没有发现可用的 NapCat（已检查 {scanned}）。"]
        for c in others:
            lines.append(f"  - {c.url}：有服务在监听，但不是 OneBot WebSocket（{c.detail}）")
        lines.append(
            "排查建议：NapCat 是否已启动并在 OneBot 11 网络配置里启用了“WebSocket 服务器”？"
            "端口是否映射到本机（Docker 需 -p 3001:3001）？NapCat 在其他机器上时，请直接填它的地址，或改用反向连接。"
        )
        return "\n".join(lines)
    lines = [f"发现 {len(usable)} 个 OneBot 11 服务："]
    for index, c in enumerate(usable, 1):
        head = f"{index}. {c.url}"
        if c.label:
            head += f"（容器 {c.label}）"
        lines.append(head)
        if c.status == "auth_required":
            lines.append(f"     {c.detail}。请带上 NapCat 配置里的令牌重新探测，或直接把令牌填进通道。")
            continue
        who = f"QQ {c.user_id}" + (f"（{c.nickname}）" if c.nickname else "") if c.logged_in else c.detail
        impl = f"，{c.app_name} {c.app_version}".rstrip() if c.app_name else ""
        lines.append(f"     {who}{impl}")
        token_note = "令牌：使用探测时提供的令牌" if token_given else "令牌：无需（NapCat 未设置令牌）"
        lines.append(f"     建议填写：连接方式=正向，ws_url={c.url}；{token_note}")
    return "\n".join(lines)
