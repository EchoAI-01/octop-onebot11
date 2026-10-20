"""End-to-end check: real WebSocket transport + the plugin channel.

Starts ``mock_napcat.py`` as a subprocess, loads the plugin with lightweight
``octop_gateway`` stubs, and verifies:

1. login detection (``get_login_info``),
2. inbound event normalization / enqueue,
3. outbound ``send_group_msg`` action,
4. the no-login case keeps the channel alive with a degraded status.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import types
from dataclasses import dataclass, field
from pathlib import Path

import websockets

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
PORT = 39001


# ---------------------------------------------------------------------------
# Minimal octop_gateway stand-ins (real websockets is used)
# ---------------------------------------------------------------------------

pkg = types.ModuleType("octop_gateway")
pkg.__path__ = []
sys.modules["octop_gateway"] = pkg


@dataclass
class ChannelConfig:
    channel_id: str | None = None
    tenant_id: str | None = None
    show_thinking: bool = False
    show_tool_hints: bool = True
    group_context: object = None


@dataclass
class ChannelConstraints:
    reply_timeout: float = 0.0
    send_rate_limit: tuple[int, float] | None = None
    typing_keepalive_interval: float = 0.0
    show_thinking: bool = False
    show_tool_hints: bool = True


@dataclass
class GroupContextConfig:
    enabled: bool = False
    activation: str = "mention"
    history: str = "recent"
    history_limit: int = 10


class BaseChannel:
    def __init__(self, processor, *, channel_id=None, tenant_id=None, config=None, **kwargs):
        self._processor = processor
        self._channel_id = channel_id or "test-channel"
        self._tenant_id = tenant_id
        self.enqueued = []
        # Mirror the real BaseChannel: constraints are resolved during
        # __init__, before subclass constructors assign their own attributes.
        self._default_constraints()

    @property
    def channel_id(self):
        return self._channel_id

    @property
    def tenant_id(self):
        return self._tenant_id

    def enqueue(self, payload):
        self.enqueued.append(payload)

    async def _acquire_rate_slot(self):
        return None

    def resolve_push_subject(self, subject):
        return subject

    async def push_text(self, subject, text):
        await self._send_text(subject, text)

    async def load_media_bytes(self, part):
        return b"", "application/octet-stream"

    @staticmethod
    def _get_media_label(part):
        return "媒体"


channel_mod = types.ModuleType("octop_gateway.channel")
channel_mod.BaseChannel = BaseChannel
channel_mod.ChannelConfig = ChannelConfig
channel_mod.MessageProcessor = object
sys.modules["octop_gateway.channel"] = channel_mod

constraints_mod = types.ModuleType("octop_gateway.constraints")
constraints_mod.ChannelConstraints = ChannelConstraints
sys.modules["octop_gateway.constraints"] = constraints_mod

group_mod = types.ModuleType("octop_gateway.group_context")
group_mod.GroupContextConfig = GroupContextConfig
sys.modules["octop_gateway.group_context"] = group_mod


@dataclass
class TextContent:
    text: str
    type: str = "text"


@dataclass
class ImageContent:
    url: str = ""
    alt_text: str = ""
    data: str | None = None
    type: str = "image"


@dataclass
class AudioContent:
    url: str = ""
    type: str = "audio"


@dataclass
class VideoContent:
    url: str = ""
    type: str = "video"


@dataclass
class FileContent:
    url: str = ""
    filename: str = ""
    type: str = "file"


@dataclass
class ChannelSubject:
    subject_id: str
    chat_type: str = ""
    display_name: str = ""
    metadata: dict = field(default_factory=dict)
    first_seen: float = 0
    last_seen: float = 0


@dataclass
class InboundMessage:
    channel_id: str
    channel_type: str
    tenant_id: str | None
    channel_subject: ChannelSubject
    channel_session_id: str
    content: list
    metadata: dict = field(default_factory=dict)
    timestamp: float = 0

    @property
    def text(self):
        return "\n".join(part.text for part in self.content if isinstance(part, TextContent) and part.text)


models_mod = types.ModuleType("octop_gateway.models")
for name, value in {
    "TextContent": TextContent,
    "ImageContent": ImageContent,
    "AudioContent": AudioContent,
    "VideoContent": VideoContent,
    "FileContent": FileContent,
    "ChannelSubject": ChannelSubject,
    "InboundMessage": InboundMessage,
    "ContentPart": object,
}.items():
    setattr(models_mod, name, value)
sys.modules["octop_gateway.models"] = models_mod


def load_plugin():
    path = ROOT / "plugins" / "octop-onebot11" / "main.py"
    spec = importlib.util.spec_from_file_location("plugin_main_e2e", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["plugin_main_e2e"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def start_mock(
    log_path: Path,
    *,
    logged_in: bool,
    port: int = PORT,
    http_port: int = 0,
) -> subprocess.Popen:
    args = [
        PY,
        str(ROOT / "tools" / "mock_napcat.py"),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--token",
        "dev-token",
        "--log-file",
        str(log_path),
    ]
    if http_port:
        args += ["--http-port", str(http_port)]
    if logged_in:
        args += ["--logged-in", "--send-test-after", "4"]
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


async def wait_connected(channel, timeout=8.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if channel.is_connected:
            return True
        await asyncio.sleep(0.1)
    return False


async def scenario_logged_in(plugin, log_path: Path):
    proc = start_mock(log_path, logged_in=True)
    try:
        await asyncio.sleep(0.8)
        cfg = plugin.OneBot11Config(
            ws_url=f"ws://127.0.0.1:{PORT}",
            token="dev-token",
            group_reply_mode="mention",
        )
        channel = plugin.OneBot11Channel(lambda _m: None, config=cfg, channel_id="e2e", tenant_id="Eous")
        await channel.start()
        assert await wait_connected(channel), f"not connected; error={channel.runtime_error}"

        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and not channel.enqueued:
            await asyncio.sleep(0.1)
        assert channel.enqueued, "no inbound event enqueued"
        message = channel.parse_inbound(channel.enqueued[0])
        assert message.channel_subject.subject_id == "group:20002"
        assert message.metadata["bot_mentioned"] is True
        assert "离线联调测试消息" in message.text

        await channel._send_text(message.channel_subject, "收到，通道正常。")

        # Proactive path: no inbound message_id, bare group id.
        await channel.push_text(plugin.build_group_subject("20002"), "主动群消息")
        # Stale persisted session: chat_type says dm, group_id still identifies a group.
        stale = plugin.ChannelSubject(
            subject_id="group:20002",
            chat_type="dm",
            metadata={"group_id": "20002", "message_type": "group", "user_id": "10001"},
        )
        await channel.push_text(stale, "主动群消息2")

        # Rich proactive push: @ + image + text segments in one message.
        await channel.push_segments(
            plugin.build_group_subject("20002"),
            [
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "image", "data": {"file": "https://example.com/a.png"}},
                {"type": "text", "data": {"text": "富媒体主动消息"}},
            ],
        )

        await asyncio.sleep(0.8)
        log = log_path.read_text(encoding="utf-8")
        assert "send_group_msg target=20002" in log, log[-2000:]
        assert "收到，通道正常。" in log
        assert "主动群消息" in log
        assert '"qq": "10001"' in log, log[-2000:]
        assert "https://example.com/a.png" in log, log[-2000:]
        assert "富媒体主动消息" in log, log[-2000:]
        assert "send_private_msg" not in log, log[-2000:]

        await channel.stop()
        print("logged-in scenario passed")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def scenario_not_logged_in(plugin, log_path: Path):
    proc = start_mock(log_path, logged_in=False)
    try:
        await asyncio.sleep(0.8)
        cfg = plugin.OneBot11Config(
            ws_url=f"ws://127.0.0.1:{PORT}",
            token="dev-token",
            group_reply_mode="mention",
        )
        channel = plugin.OneBot11Channel(lambda _m: None, config=cfg, channel_id="e2e-nologin", tenant_id="Eous")
        await channel.start()
        await asyncio.sleep(1.5)
        assert not channel.is_connected, "must not report connected before login"
        assert channel.runtime_error, "must expose a degraded status"
        assert channel._task is not None and not channel._task.done(), "channel must stay alive"
        log = log_path.read_text(encoding="utf-8")
        assert "get_login_info" not in log  # mock does not log actions by design
        await channel.stop()
        print("not-logged-in scenario passed")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def scenario_outbox(plugin, log_path: Path):
    """Offline strict pushes park in the outbox and drain once NapCat appears."""
    port = free_port()
    cfg = plugin.OneBot11Config(
        ws_url=f"ws://127.0.0.1:{port}",
        token="dev-token",
        group_reply_mode="mention",
        reconnect_min_seconds=0.5,
        reconnect_max_seconds=2.0,
        startup_wait_seconds=0.5,
        outbox_persist_dir=tempfile.mkdtemp(),
    )
    channel = plugin.OneBot11Channel(lambda _m: None, config=cfg, channel_id="e2e-outbox", tenant_id="Eous")
    await channel.start()
    await asyncio.sleep(0.5)
    assert not channel.is_connected, "channel must start offline against a dead endpoint"

    # Strict push while offline parks the message instead of raising.
    await channel.push_text(plugin.build_group_subject("20002"), "排队后补发")
    assert len(channel._outbox) == 1, "message must be queued while disconnected"

    proc = start_mock(log_path, logged_in=True, port=port)
    try:
        assert await wait_connected(channel, timeout=15), f"not connected: {channel.runtime_error}"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and channel._outbox:
            await asyncio.sleep(0.1)
        assert not channel._outbox, f"outbox not drained: {channel.runtime_error}"
        log = log_path.read_text(encoding="utf-8")
        assert "send_group_msg target=20002" in log, log[-2000:]
        assert "排队后补发" in log, log[-2000:]
        await channel.stop()
        print("outbox scenario passed")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def scenario_http_fallback(plugin, log_path: Path):
    """Proactive sends go through the OneBot HTTP API while WS is down."""
    ws_port = free_port()
    http_port = free_port()
    dead_ws = free_port()
    proc = start_mock(log_path, logged_in=True, port=ws_port, http_port=http_port)
    try:
        await asyncio.sleep(0.8)
        cfg = plugin.OneBot11Config(
            ws_url=f"ws://127.0.0.1:{dead_ws}",  # nothing listens here
            token="dev-token",
            http_api_url=f"http://127.0.0.1:{http_port}",
            group_reply_mode="mention",
            reconnect_min_seconds=0.5,
            reconnect_max_seconds=2.0,
            startup_wait_seconds=0.5,
        )
        channel = plugin.OneBot11Channel(lambda _m: None, config=cfg, channel_id="e2e-http", tenant_id="Eous")
        await channel.start()
        await asyncio.sleep(0.5)
        assert not channel.is_connected, "WS is dead; login cannot be confirmed"

        await channel.push_text(plugin.build_group_subject("20002"), "HTTP 备用通道消息")
        await asyncio.sleep(0.8)
        assert not channel._outbox, "push must not queue when the HTTP fallback works"
        log = log_path.read_text(encoding="utf-8")
        assert "send_group_msg target=20002" in log, log[-2000:]
        assert "HTTP 备用通道消息" in log, log[-2000:]
        await channel.stop()
        print("http-fallback scenario passed")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ReverseNapCat:
    """Minimal NapCat reverse-WS client: answers actions, records requests."""

    def __init__(self, ws, user_id: int = 10000):
        self.ws = ws
        self.user_id = user_id
        self.actions: list[dict] = []
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._reader(), name="reverse-napcat-reader")

    async def _reader(self) -> None:
        async for raw in self.ws:
            payload = json.loads(raw)
            echo = payload.get("echo")
            if echo is None:
                continue
            self.actions.append(payload)
            data = {}
            if payload.get("action") == "get_login_info":
                data = {"user_id": self.user_id, "nickname": "反向测试机器人"}
            await self.ws.send(
                json.dumps({"status": "ok", "retcode": 0, "data": data, "echo": echo})
            )

    async def wait_action(self, action: str, index: int = 0, timeout: float = 6.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            matches = [item for item in list(self.actions) if item.get("action") == action]
            if len(matches) > index:
                return matches[index]
            await asyncio.sleep(0.05)
        raise AssertionError(
            f"no {action}[{index}] action; got {[item.get('action') for item in self.actions]}"
        )

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass


async def scenario_reverse(plugin):
    """NapCat dials in; inbound events and proactive sends share one socket."""
    port = free_port()
    cfg = plugin.OneBot11Config(
        connection_mode="reverse",
        listen_host="127.0.0.1",
        listen_port=port,
        listen_path="/onebot/v11/ws",
        token="dev-token",
        group_reply_mode="mention",
    )
    channel = plugin.OneBot11Channel(
        lambda _m: None, config=cfg, channel_id="e2e-reverse", tenant_id="Eous"
    )
    await channel.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and channel._server is None:
        await asyncio.sleep(0.05)
    assert channel._server is not None, f"reverse listener not bound: {channel.runtime_error}"
    url = f"ws://127.0.0.1:{port}/onebot/v11/ws"

    # A wrong token must be closed with policy violation 1008.
    try:
        async with websockets.connect(
            url, additional_headers={"Authorization": "Bearer wrong"}
        ) as bad:
            await asyncio.wait_for(bad.recv(), timeout=3)
    except Exception:
        pass
    else:
        raise AssertionError("bad token must be rejected")

    async with websockets.connect(
        url,
        additional_headers={
            "Authorization": "Bearer dev-token",
            "X-Self-ID": "10000",
        },
    ) as ws:
        napcat = ReverseNapCat(ws)
        await napcat.start()
        try:
            assert await wait_connected(channel), f"not connected: {channel.runtime_error}"

            event = {
                "post_type": "message",
                "message_type": "group",
                "message_id": 88001,
                "group_id": 20002,
                "user_id": 10001,
                "self_id": 10000,
                "message": [
                    {"type": "at", "data": {"qq": "10000"}},
                    {"type": "text", "data": {"text": " 反向通道测试"}},
                ],
                "raw_message": "[CQ:at,qq=10000] 反向通道测试",
                "sender": {"user_id": 10001, "nickname": "测试用户"},
            }
            await ws.send(json.dumps(event, ensure_ascii=False))

            deadline = time.monotonic() + 6
            while time.monotonic() < deadline and not channel.enqueued:
                await asyncio.sleep(0.05)
            assert channel.enqueued, "reverse inbound event not enqueued"
            message = channel.parse_inbound(channel.enqueued[0])
            assert message.channel_subject.subject_id == "group:20002"
            assert message.metadata["bot_mentioned"] is True

            await channel._send_text(message.channel_subject, "反向通道收到。")
            passive = await napcat.wait_action("send_group_msg")
            assert passive["params"]["group_id"] == 20002
            assert passive["params"]["message"][0]["data"]["text"] == "反向通道收到。"

            await channel.push_text(plugin.build_group_subject("20002"), "反向主动群消息")
            proactive = await napcat.wait_action("send_group_msg", index=1)
            assert "反向主动群消息" in json.dumps(proactive, ensure_ascii=False)

            await channel.push_text(plugin.build_private_subject("10001"), "反向主动私聊")
            private = await napcat.wait_action("send_private_msg")
            assert private["params"]["user_id"] == 10001
        finally:
            await napcat.close()

    deadline = time.monotonic() + 4
    while time.monotonic() < deadline and channel._ws is not None:
        await asyncio.sleep(0.05)
    assert channel._ws is None, "reverse client must be detached after close"
    assert not channel.is_connected
    await channel.stop()
    print("reverse scenario passed")


async def scenario_reverse_multi(plugin):
    """Two NapCat accounts coexist on one reverse channel (multi_account)."""
    port = free_port()
    cfg = plugin.OneBot11Config(
        connection_mode="reverse",
        listen_host="127.0.0.1",
        listen_port=port,
        listen_path="/onebot/v11/ws",
        token="dev-token",
        group_reply_mode="mention",
        multi_account=True,
    )
    channel = plugin.OneBot11Channel(
        lambda _m: None, config=cfg, channel_id="e2e-rev-multi", tenant_id="Eous"
    )
    await channel.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and channel._server is None:
        await asyncio.sleep(0.05)
    assert channel._server is not None, f"reverse listener not bound: {channel.runtime_error}"
    url = f"ws://127.0.0.1:{port}/onebot/v11/ws"

    async with websockets.connect(
        url,
        additional_headers={"Authorization": "Bearer dev-token", "X-Self-ID": "10000"},
    ) as ws_a:
        napcat_a = ReverseNapCat(ws_a, user_id=10000)
        await napcat_a.start()
        async with websockets.connect(
            url,
            additional_headers={"Authorization": "Bearer dev-token", "X-Self-ID": "20000"},
        ) as ws_b:
            napcat_b = ReverseNapCat(ws_b, user_id=20000)
            await napcat_b.start()
            try:
                assert await wait_connected(channel), f"not connected: {channel.runtime_error}"
                assert len(channel._sessions) == 2, "both accounts must coexist"

                event = {
                    "post_type": "message",
                    "message_type": "group",
                    "message_id": 88002,
                    "group_id": 30003,
                    "user_id": 10001,
                    "self_id": 20000,
                    "message": [
                        {"type": "at", "data": {"qq": "20000"}},
                        {"type": "text", "data": {"text": " 账号B的群消息"}},
                    ],
                    "raw_message": "[CQ:at,qq=20000] 账号B的群消息",
                    "sender": {"user_id": 10001, "nickname": "测试用户"},
                }
                await ws_b.send(json.dumps(event, ensure_ascii=False))

                deadline = time.monotonic() + 6
                while time.monotonic() < deadline and not channel.enqueued:
                    await asyncio.sleep(0.05)
                assert channel.enqueued, "reverse multi inbound event not enqueued"
                message = channel.parse_inbound(channel.enqueued[0])
                assert message.channel_subject.subject_id == "group:30003"
                assert message.metadata["self_id"] == "20000"

                # Passive replies route back to the account that received them.
                await channel._send_text(message.channel_subject, "回给账号B")
                b_reply = await napcat_b.wait_action("send_group_msg")
                assert b_reply["params"]["group_id"] == 30003

                # Account-targeted push goes to B; the default goes to the primary (A).
                await channel.push_text(
                    plugin.build_group_subject("20002", self_id="20000"), "定向账号B"
                )
                targeted = await napcat_b.wait_action("send_group_msg", index=1)
                assert targeted["params"]["group_id"] == 20002
                await channel.push_text(plugin.build_group_subject("20002"), "默认账号A")
                a_push = await napcat_a.wait_action("send_group_msg")
                assert a_push["params"]["group_id"] == 20002
            finally:
                await napcat_b.close()

    # Account A must stay healthy after B leaves.
    assert channel.is_connected, "account A must stay connected after B leaves"
    await napcat_a.close()
    await channel.stop()
    print("reverse multi-account scenario passed")


async def main():
    plugin = load_plugin()
    with tempfile.TemporaryDirectory() as tmp:
        await scenario_not_logged_in(plugin, Path(tmp) / "mock-nologin.log")
    with tempfile.TemporaryDirectory() as tmp:
        await scenario_logged_in(plugin, Path(tmp) / "mock-login.log")
    with tempfile.TemporaryDirectory() as tmp:
        await scenario_outbox(plugin, Path(tmp) / "mock-outbox.log")
    with tempfile.TemporaryDirectory() as tmp:
        await scenario_http_fallback(plugin, Path(tmp) / "mock-http.log")
    await scenario_reverse(plugin)
    await scenario_reverse_multi(plugin)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    asyncio.run(main())
