"""Contract checks against the real Octop packages (``octop_gateway`` + ``octop_harness``).

The other suites in ``tools/`` run the plugin on stand-ins for the Octop core.
This one loads both plugins through the real harness loader, registers the
channel with the real gateway, drives it with the real ``ChannelManager``
(queue -> worker -> ``handle_inbound`` -> ``GroupContextManager`` ->
``RateLimiter``) against an in-process fake NapCat, and calls the agent tools
as real LangChain ``StructuredTool`` objects.  It catches drift between the
plugin and the installed Octop release.

    /opt/octop/venv/bin/python tools/test_plugin_core_integration.py [check ...]

Nothing is installed or restarted: everything runs in this one process on
loopback ports picked by the OS.  When ``octop_gateway`` / ``octop_harness``
are not importable the suite reports SKIP and exits 0; set
``ONEBOT11_REQUIRE_CORE=1`` (CI does) to turn that into a failure.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import dataclasses
import json
import os
import re
import shutil
import socket
import sys
import tempfile
import time
import traceback
from pathlib import Path

# The plugin keeps its default state (history, outbox) under ~/.octop; point
# "home" at a scratch directory so a test run never touches the real one.
_SCRATCH = tempfile.mkdtemp(prefix="ob11-core-")
tempfile.tempdir = _SCRATCH
atexit.register(shutil.rmtree, _SCRATCH, ignore_errors=True)
os.environ["HOME"] = os.environ["USERPROFILE"] = _SCRATCH

ROOT = Path(__file__).resolve().parents[1]

try:
    from octop_gateway.channel import BaseChannel, ChannelCredentialsError
    from octop_gateway.channels import register_channel_kind, unregister_channel_kind
    from octop_gateway.manager import ChannelManager
    from octop_gateway.media import FileSystemMediaBackend
    from octop_gateway.models import (
        AudioContent,
        FileContent,
        ImageContent,
        InboundMessage,
        MessageEvent,
        VideoContent,
    )
    from octop_harness.plugins import PluginRegistry, build_plugin_tools, load_plugin_dir, unload_plugin
except ImportError as exc:  # pragma: no cover - depends on the environment
    if os.environ.get("ONEBOT11_REQUIRE_CORE") == "1":
        print(f"FAIL core integration: Octop packages are not importable ({exc})")
        raise SystemExit(1) from None
    print(f"SKIP core integration: Octop packages are not importable ({exc})")
    raise SystemExit(0) from None

from websockets.asyncio.server import serve as ws_serve  # noqa: E402
from websockets.exceptions import ConnectionClosed  # noqa: E402

CHANNEL_PLUGIN_ID = "octop-onebot11"
TOOLS_PLUGIN_ID = "octop-onebot11-tools"
CHANNEL_MODULE = "harness_plugin_octop_onebot11"
SEND_GROUP_TOOL = "onebot11_send_group"

CHECKS: list = []


def check(fn):
    CHECKS.append(fn)
    return fn


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def wait_for(predicate, timeout: float = 6.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


# ---------------------------------------------------------------------------
# Test doubles: a NapCat that answers actions, and a recording processor
# ---------------------------------------------------------------------------


class FakeNapCat:
    """Forward-mode OneBot 11 server: login info, ack every send, record frames."""

    def __init__(self, self_id: int = 10000, replies: dict | None = None):
        self.self_id = self_id
        # action -> reply data (or a callable(frame) returning data / an Exception to fail)
        self.replies = dict(replies or {})
        self.frames: list[dict] = []
        self.auth_headers: list[str] = []
        self.sockets: set = set()
        self.port = 0
        self._server = None
        self._counter = 0

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def __aenter__(self):
        self._server = await ws_serve(self._handler, "127.0.0.1", 0, ping_interval=None)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self._server.close()
        await self._server.wait_closed()

    async def _handler(self, ws):
        self.sockets.add(ws)
        self.auth_headers.append(ws.request.headers.get("Authorization", ""))
        try:
            async for raw in ws:
                frame = json.loads(raw)
                self.frames.append(frame)
                action = str(frame.get("action") or "")
                data = None
                if action == "get_login_info":
                    data = {"user_id": self.self_id, "nickname": "bot"}
                elif action.startswith("send_"):
                    self._counter += 1
                    data = {"message_id": 7000 + self._counter}
                elif action in self.replies:
                    reply = self.replies[action]
                    data = reply(frame) if callable(reply) else reply
                if isinstance(data, Exception):
                    answer = {"status": "failed", "retcode": 1200, "wording": str(data), "data": None}
                else:
                    answer = {"status": "ok", "retcode": 0, "data": data}
                await ws.send(json.dumps({**answer, "echo": frame.get("echo")}))
        except ConnectionClosed:
            pass
        finally:
            self.sockets.discard(ws)

    async def emit(self, event: dict) -> None:
        for ws in list(self.sockets):
            await ws.send(json.dumps(event, ensure_ascii=False))

    def sent(self, action: str | None = None) -> list[dict]:
        return [
            frame
            for frame in self.frames
            if str(frame.get("action") or "").startswith("send_")
            and (action is None or frame.get("action") == action)
        ]

    def called(self, action: str) -> list[dict]:
        return [frame for frame in self.frames if frame.get("action") == action]


def message_event(
    message_type: str,
    text: str,
    *,
    message_id: int,
    user_id: int = 10001,
    group_id: int = 20002,
    self_id: int = 10000,
    at_bot: bool = False,
) -> dict:
    segments = []
    if at_bot:
        segments.append({"type": "at", "data": {"qq": str(self_id)}})
    segments.append({"type": "text", "data": {"text": text}})
    event = {
        "post_type": "message",
        "message_type": message_type,
        "sub_type": "normal",
        "message_id": message_id,
        "user_id": user_id,
        "self_id": self_id,
        "time": int(time.time()),
        "message": segments,
        "raw_message": text,
        "sender": {"user_id": user_id, "nickname": f"U{user_id}", "card": ""},
    }
    if message_type == "group":
        event["group_id"] = group_id
    return event


class Recorder:
    """Message processor: remember every InboundMessage, answer with one text."""

    def __init__(self, reply=None):
        self.messages: list[InboundMessage] = []
        self._reply = reply

    async def __call__(self, message: InboundMessage):
        self.messages.append(message)
        reply = self._reply(message) if callable(self._reply) else (self._reply or f"echo:{message.text}")
        yield MessageEvent.text(reply)
        yield MessageEvent.completed()


@contextlib.asynccontextmanager
async def core(processor):
    """A real ChannelManager (media goes to a throwaway directory)."""
    with tempfile.TemporaryDirectory() as media_root:
        manager = ChannelManager(
            processor=processor,
            workers_per_channel=1,
            media_backend=FileSystemMediaBackend(media_root),
        )
        await manager.start()
        try:
            yield manager
        finally:
            await manager.stop()


def dashboard_config(napcat: FakeNapCat, **overrides) -> dict:
    """Config as the dashboard stores it: strings everywhere, token under its alias."""
    config = {
        "ws_url": napcat.url,
        "access_token": "  dev-token  ",
        "startup_wait_seconds": "0.3",
        "request_timeout_seconds": "5",
        "outbox_persist": "false",
        "group_history_limit": "5",
        "unknown_future_key": "ignored",
    }
    config.update(overrides)
    return config


async def add_channel(manager, napcat: FakeNapCat, channel_id: str, **overrides) -> BaseChannel:
    await manager.add_channel(
        channel_type="onebot11",
        config=dashboard_config(napcat, **overrides),
        channel_id=channel_id,
        tenant_id="it",
    )
    channel = manager.get_channel(channel_id)
    await wait_for(lambda: channel.is_connected, what=f"{channel_id} login")
    return channel


# ---------------------------------------------------------------------------
# Loader / registration contract
# ---------------------------------------------------------------------------

_loaded: dict = {}


def load_plugins() -> dict:
    """Load both plugins once through the real harness loader and gateway registry."""
    if _loaded:
        return _loaded
    PluginRegistry().clear()
    channel_plugin = load_plugin_dir(ROOT / "plugins" / CHANNEL_PLUGIN_ID, install_deps=False)
    tools_plugin = load_plugin_dir(ROOT / "plugins" / TOOLS_PLUGIN_ID, install_deps=False)
    registration = channel_plugin.channels[0]
    register_channel_kind(registration.kind, registration.channel_cls, plugin_id=CHANNEL_PLUGIN_ID)
    _loaded.update(
        channel_plugin=channel_plugin,
        tools_plugin=tools_plugin,
        registration=registration,
        module=sys.modules[CHANNEL_MODULE],
    )
    return _loaded


@check
async def the_agent_skill_loads_through_the_real_harness_and_parses():
    """kind=skill: the real loader registers the directory, and Octop's own parser reads our SKILL.md."""
    from octop_harness.skills.catalog import _parse_skill_frontmatter, _summary_dict

    skills_plugin = load_plugin_dir(ROOT / "plugins" / "octop-onebot11-skills", install_deps=False)
    try:
        assert skills_plugin.manifest.kind == "skill" and not skills_plugin.tools and not skills_plugin.channels
        assert skills_plugin.skills_dir is not None and skills_plugin.skills_dir.is_dir()
        found = sorted(
            path.name for path in skills_plugin.skills_dir.iterdir() if path.is_dir() and (path / "SKILL.md").is_file()
        )
        assert found == ["qq-onebot11"], found
        text = (skills_plugin.skills_dir / "qq-onebot11" / "SKILL.md").read_text(encoding="utf-8")
        meta, body = _parse_skill_frontmatter(text)
        assert meta.get("name") == "qq-onebot11" and body.lstrip().startswith("# QQ 通道工作指南"), (meta, body[:40])
        summary = _summary_dict("qq-onebot11", meta, enabled=True, kind="workspace")
        assert summary["description"].startswith("在 QQ") and summary["label"]["zh"] == "QQ 通道工作指南", summary
        assert (skills_plugin.skills_dir / "qq-onebot11" / "references" / "tools.md").is_file()
    finally:
        unload_plugin("octop-onebot11-skills")


@check
async def plugins_load_through_the_real_harness():
    loaded = load_plugins()
    channel_plugin, tools_plugin = loaded["channel_plugin"], loaded["tools_plugin"]
    registration, module = loaded["registration"], loaded["module"]

    assert channel_plugin.manifest.kind == "channel" and tools_plugin.manifest.kind == "tool"
    assert registration.kind == "onebot11", registration.kind
    assert issubclass(registration.channel_cls, BaseChannel)

    config_fields = {field.name for field in dataclasses.fields(module.OneBot11Config)}
    names = [field["name"] for field in registration.fields]
    assert len(names) == len(set(names)), "duplicate dashboard field"
    unknown = [name for name in names if name not in config_fields]
    assert not unknown, f"dashboard fields that are not config fields: {unknown}"
    by_name = {field["name"]: field for field in registration.fields}
    assert by_name["token"]["type"] == "password", by_name["token"]
    for name in ("token", "http_api_token", "access_token", "secret", "password"):
        if name in by_name:
            assert by_name[name]["type"] == "password", f"{name} must be a password input"

    tool_names = sorted(tool.name for tool in tools_plugin.tools)
    assert tool_names == sorted(
        [
            "onebot11_send_forward_group",
            "onebot11_send_group",
            "onebot11_send_private",
            "onebot11_send_voice",
            "onebot11_status",
            "onebot11_probe_napcat",
            "onebot11_list_groups",
            "onebot11_group_members",
            "onebot11_react",
            "onebot11_recall_message",
            "onebot11_group_admin",
            "onebot11_query_history",
            "onebot11_history_overview",
        ]
    ), tool_names
    for tool in tools_plugin.tools:
        fields = [field["name"] for field in tool.config_fields]
        if tool.name in (
            "onebot11_query_history",
            "onebot11_history_overview",
            "onebot11_list_groups",
            "onebot11_group_members",
            "onebot11_react",
            "onebot11_recall_message",
            "onebot11_group_admin",
        ):
            assert fields == ["channel_id", "allow_other_chats"], (tool.name, fields)
        elif tool.name == "onebot11_probe_napcat":
            assert fields == [], (tool.name, fields)
        else:
            assert fields == ["channel_id"], (tool.name, fields)


# ---------------------------------------------------------------------------
# Configuration as the real manager builds it
# ---------------------------------------------------------------------------


@check
async def dashboard_payload_builds_a_working_channel():
    load_plugins()
    async with FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(
                manager,
                napcat,
                "it-config",
                reply_with_quote="false",
                ignore_self_message="true",
                send_rate_limit_count="7",
            )
            config = channel._config
            assert config.token == "dev-token", repr(config.token)  # alias + stripped by the core
            assert napcat.auth_headers == ["Bearer dev-token"], napcat.auth_headers
            assert config.request_timeout_seconds == 5.0
            assert config.group_history_limit == 5 and config.group_context.history_limit == 5
            assert config.reply_with_quote is False and config.ignore_self_message is True
            assert config.outbox_persist is False
            assert not hasattr(config, "unknown_future_key")

            await napcat.emit(message_event("private", "你好", message_id=1))
            await wait_for(lambda: napcat.sent("send_private_msg"), what="private reply")
            reply = napcat.sent("send_private_msg")[0]["params"]
            assert reply["user_id"] == 10001
            # The model sees when each message was sent: "[MM-DD HH:MM] text".
            assert re.search(r"echo:\[\d\d-\d\d \d\d:\d\d\] 你好", json.dumps(reply, ensure_ascii=False)), reply
            assert len(recorder.messages) == 1
            message = recorder.messages[0]
            assert isinstance(message, InboundMessage)
            assert message.channel_subject.subject_id == "user:10001"
            assert message.metadata["sender_id"] == "10001"
            # Nicknames are labels, the QQ number is the identity.
            assert message.metadata["sender_name"] == "U10001(10001)", message.metadata["sender_name"]
            assert message.metadata["sender_nickname"] == "U10001"
            assert channel._rate_limiter is not None and channel._rate_limiter._max_calls == 7
            assert len(channel._rate_limiter._timestamps) == 1


@check
async def credentials_check_still_runs_before_start():
    load_plugins()
    async with core(Recorder()) as manager:
        try:
            await manager.add_channel(
                channel_type="onebot11",
                config={"connection_mode": "forward", "ws_url": "  "},
                channel_id="it-nocreds",
            )
        except ChannelCredentialsError as exc:
            assert exc.missing == ["ws_url"], exc.missing
        else:
            raise AssertionError("forward mode without ws_url must be rejected by the manager")
        assert manager.get_channel("it-nocreds") is None


@check
async def probe_leaves_no_live_channel_behind():
    module = load_plugins()["module"]
    async with FakeNapCat() as napcat:
        async with core(Recorder()) as manager:
            before = {channel.channel_id for channel in module.list_live_channels()}
            await manager.probe_channel(
                "onebot11",
                {
                    "ws_url": napcat.url,
                    "token": "dev-token",
                    "startup_wait_seconds": "0.3",
                    "outbox_persist": "false",
                },
            )
            after = {channel.channel_id for channel in module.list_live_channels()}
            assert after == before, f"probe leaked live channels: {after - before}"


@check
async def reverse_listener_without_token_is_refused_not_crashed():
    load_plugins()
    async with core(Recorder()) as manager:
        # No ws_url (reverse mode needs none) and no token on a non-loopback bind.
        await manager.add_channel(
            channel_type="onebot11",
            config={
                "connection_mode": "reverse",
                "listen_host": "0.0.0.0",
                "listen_port": str(free_port()),
                "startup_wait_seconds": "0.3",
                "outbox_persist": "false",
            },
            channel_id="it-reverse-open",
        )
        channel = manager.get_channel("it-reverse-open")
        await wait_for(
            lambda: channel.runtime_error and "token" in channel.runtime_error, what="refusal status"
        )
        assert channel._server is None, "open reverse listener must not be bound"
        assert not channel.is_connected


# ---------------------------------------------------------------------------
# Real group policy, rate limiter and delivery template
# ---------------------------------------------------------------------------


@check
async def mention_mode_buffers_chatter_with_the_real_group_policy():
    load_plugins()
    async with FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-mention", group_reply_mode="mention")

            await napcat.emit(message_event("group", "晚上吃什么", message_id=10, user_id=10002))
            await asyncio.sleep(0.5)
            assert recorder.messages == [], "passive chatter must not start an agent turn"
            assert not napcat.sent(), "passive chatter must not produce a reply"

            await napcat.emit(message_event("group", "总结一下", message_id=11, at_bot=True))
            await wait_for(lambda: napcat.sent("send_group_msg"), what="group reply")
            message = recorder.messages[0]
            context = message.group_context
            assert context is not None and context.capability_degraded is False
            senders = [item.sender_id for item in context.messages]
            assert senders == ["10002"], senders  # sender_id comes from metadata, not "unknown"
            assert "晚上吃什么" in context.messages[0].text
            assert message.metadata["sender_id"] == "10001"
            assert napcat.sent("send_group_msg")[0]["params"]["group_id"] == 20002
            assert len(channel._rate_limiter._timestamps) == 1


@check
async def always_mode_is_not_degraded_by_the_core():
    load_plugins()
    async with FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            await add_channel(manager, napcat, "it-always", group_reply_mode="always")
            await napcat.emit(message_event("group", "没有 @ 也要回复", message_id=20))
            await wait_for(lambda: napcat.sent("send_group_msg"), what="always-mode reply")
            context = recorder.messages[0].group_context
            assert context is not None
            assert context.activation == "always" and context.capability_degraded is False, context


@check
async def auto_mode_works_with_the_real_group_policy():
    """R8 end to end on the real core: only gate-approved messages start a turn, declines stay silent."""
    load_plugins()
    async with FakeNapCat() as napcat:
        answers = {"请问有人知道怎么配置吗": "〔不参与〕", "这个功能怎么开启呢？请问": "在设置页里打开即可"}

        def reply(message):
            return next((v for k, v in answers.items() if k in message.text), "echo")

        recorder = Recorder(reply=reply)
        async with core(recorder) as manager:
            channel = await add_channel(
                manager, napcat, "it-auto", group_reply_mode="auto", auto_reply_cooldown_seconds="0"
            )
            await napcat.emit(message_event("group", "哈哈哈哈", message_id=60))
            await napcat.emit(message_event("group", "今天我们去吃什么好呢大家", message_id=61))
            await asyncio.sleep(0.4)
            assert recorder.messages == [], [m.text for m in recorder.messages]

            # A question the gate approves reaches the agent; the agent declines -> nothing is sent.
            await napcat.emit(message_event("group", "请问有人知道怎么配置吗", message_id=62))
            await wait_for(lambda: len(recorder.messages) == 1, what="auto-engaged turn")
            engaged = recorder.messages[0]
            assert engaged.metadata["auto_engaged"] is True and engaged.metadata["trigger_reason"].startswith("auto:")
            assert "〔自主参与〕" in engaged.text and "请问有人知道怎么配置吗" in engaged.text
            context = engaged.group_context
            assert context is not None and context.capability_degraded is False, repr(context)
            assert context.activation == "mention", "an approved message is promoted to a mention for the core"
            assert [m.message_id for m in context.messages] == ["60", "61"], "the earlier chatter rides along as background"
            assert all(m.sender_name == "U10001(10001)" for m in context.messages), "QQ number identifies the speaker"
            await asyncio.sleep(0.4)
            assert napcat.sent("send_group_msg") == [], "a decline must never be posted to the group"
            assert channel.stats()["auto_engaged"] == 1 and channel.stats()["auto_declined"] == 1

            # A question the agent does answer is delivered.
            await napcat.emit(message_event("group", "这个功能怎么开启呢？请问", message_id=63))
            await wait_for(lambda: napcat.sent("send_group_msg"), what="auto reply")
            assert "在设置页里打开即可" in json.dumps(napcat.sent("send_group_msg")[0], ensure_ascii=False)
            assert channel.stats()["auto_engaged"] == 2


@check
async def a_long_reply_consumes_one_rate_slot():
    load_plugins()
    async with FakeNapCat() as napcat:
        async with core(Recorder(reply="长" * 700)) as manager:
            channel = await add_channel(manager, napcat, "it-split", max_text_chars="200")
            await napcat.emit(message_event("private", "写长文", message_id=30))
            await wait_for(lambda: len(napcat.sent("send_private_msg")) >= 4, what="split reply")
            await asyncio.sleep(0.3)
            assert len(napcat.sent("send_private_msg")) == 4, len(napcat.sent("send_private_msg"))
            assert len(channel._rate_limiter._timestamps) == 1, (
                "one logical reply must take exactly one slot, "
                f"got {len(channel._rate_limiter._timestamps)}"
            )


# ---------------------------------------------------------------------------
# parse_inbound with the real pydantic content models
# ---------------------------------------------------------------------------


@check
async def inbound_media_parses_into_the_real_models():
    load_plugins()
    async with FakeNapCat() as napcat:
        async with core(Recorder()) as manager:
            channel = await add_channel(manager, napcat, "it-parse")
            event = message_event("group", "", message_id=40, at_bot=True)
            event["message"] = [
                {"type": "reply", "data": {"id": "39"}},
                {"type": "at", "data": {"qq": "10000"}},
                {"type": "image", "data": {"url": "https://example.com/a.png", "summary": "图"}},
                {"type": "image", "data": {"file": "base64://aGVsbG8="}},
                {"type": "record", "data": {"url": "https://example.com/a.silk"}},
                {"type": "video", "data": {"url": "https://example.com/a.mp4"}},
                {"type": "file", "data": {"url": "https://example.com/a.pdf", "name": "a.pdf"}},
                {"type": "face", "data": {"id": "1"}},
                {"type": "text", "data": {"text": "完毕"}},
            ]
            message = channel.parse_inbound(event)
            assert isinstance(message, InboundMessage)
            kinds = {type(part) for part in message.content}
            assert {ImageContent, AudioContent, VideoContent, FileContent} <= kinds, kinds
            assert message.has_media
            assert message.metadata["bot_mentioned"] is True and message.metadata["reply_to"] == "39"
            assert message.channel_subject.subject_id == "group:20002"
            assert "完毕" in message.text


# ---------------------------------------------------------------------------
# R11: images and voice notes through the real pipeline
# ---------------------------------------------------------------------------

PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c6360f8cfc000000301010018dd8db40000000049454e44ae426082"
)
MP3 = b"ID3\x03\x00\x00\x00\x00\x00\x00" + b"\x01" * 96


@contextlib.asynccontextmanager
async def http_server(routes):
    """A throwaway aiohttp server on loopback (stands in for QQ's CDN or an ASR service)."""
    from aiohttp import web

    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield site._server.sockets[0].getsockname()[1]
    finally:
        await runner.cleanup()


def with_segments(event: dict, *segments: dict) -> dict:
    event["message"] = [*event["message"], *segments]
    return event


def voice_event(message_type: str, *, message_id: int, file: str = "v.amr", user_id: int = 10001) -> dict:
    event = message_event(message_type, "", message_id=message_id, user_id=user_id)
    event["message"] = [{"type": "record", "data": {"file": file}}]
    return event


def content_images(message) -> list:
    return [part for part in message.content if isinstance(part, ImageContent)]


@check
async def images_are_fetched_and_stored_for_the_model():
    from aiohttp import web

    load_plugins()
    agents: list[str] = []

    async def picture(request):
        agents.append(request.headers.get("User-Agent", ""))
        return web.Response(body=PNG, content_type="image/png")

    async with http_server([web.get("/a.png", picture)]) as port, FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-img")
            event = with_segments(
                message_event("group", "看看这个", message_id=70, at_bot=True),
                {"type": "image", "data": {"file": "A1.png", "url": f"http://127.0.0.1:{port}/a.png", "summary": ""}},
            )
            await napcat.emit(event)
            await wait_for(lambda: recorder.messages, what="image turn")
            images = content_images(recorder.messages[0])
            assert len(images) == 1, recorder.messages[0].content
            image = images[0]
            assert image.mime_type == "image/png" and image.size == len(PNG)
            assert image.local_path, "the picture must live in the media backend, not only behind a URL"
            raw, mime = await channel.load_media_bytes(image)
            assert raw == PNG and mime == "image/png"
            assert agents and agents[0].startswith("Mozilla/5.0"), agents
            assert len(agents) == 1, "one download: the core must not fetch what the plugin already stored"
            assert channel.stats()["media_images_fetched"] == 1


@check
async def images_fall_back_to_napcat_when_the_cdn_refuses():
    import base64

    from aiohttp import web

    load_plugins()

    async def gone(request):
        return web.Response(status=404)

    replies = {"get_image": lambda frame: {"file": "/napcat/cache/A1.png", "url": "", "base64": base64.b64encode(PNG).decode()}}
    async with http_server([web.get("/a.png", gone)]) as port, FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-img-fallback")
            await napcat.emit(
                with_segments(
                    message_event("group", "再看这个", message_id=71, at_bot=True),
                    {"type": "image", "data": {"file": "A1.png", "url": f"http://127.0.0.1:{port}/a.png"}},
                    {"type": "image", "data": {"file": "B2.png"}},
                )
            )
            await wait_for(lambda: recorder.messages, what="fallback turn")
            images = content_images(recorder.messages[0])
            assert len(images) == 2, "an image with no URL at all is recovered through get_image too"
            for image in images:
                raw, _ = await channel.load_media_bytes(image)
                assert raw == PNG
            asked = sorted(frame["params"]["file"] for frame in napcat.called("get_image"))
            assert asked == ["A1.png", "B2.png"], asked
            assert channel.stats()["media_images_fetched"] == 2 and channel.stats()["media_images_failed"] == 0


@check
async def untrusted_image_hosts_are_not_fetched_by_the_plugin():
    from aiohttp import web

    load_plugins()

    async def picture(request):
        return web.Response(body=PNG, content_type="image/png")

    async with http_server([web.get("/a.png", picture)]) as port, FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-img-untrusted")
            # ``localhost`` is neither a QQ domain nor NapCat's own host (127.0.0.1).
            await napcat.emit(
                with_segments(
                    message_event("group", "外链图", message_id=72, at_bot=True),
                    {"type": "image", "data": {"file": "C3.png", "url": f"http://localhost:{port}/a.png"}},
                )
            )
            await wait_for(lambda: recorder.messages, what="untrusted turn")
            stats = channel.stats()
            assert stats["media_images_fetched"] == 0 and stats["media_images_failed"] == 1, stats
            assert napcat.called("get_image"), "the NapCat fallback is still tried"


@check
async def passive_group_chatter_does_not_download_pictures():
    from aiohttp import web

    load_plugins()
    hits: list[str] = []

    async def picture(request):
        hits.append(request.path)
        return web.Response(body=PNG, content_type="image/png")

    async with http_server([web.get("/a.png", picture)]) as port, FakeNapCat() as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            # Default mention mode, no group history: a picture nobody addressed is never fetched.
            channel = await add_channel(manager, napcat, "it-img-passive", group_history_limit="0")
            await napcat.emit(
                with_segments(
                    message_event("group", "随手一发", message_id=73),
                    {"type": "image", "data": {"file": "D4.png", "url": f"http://127.0.0.1:{port}/a.png"}},
                )
            )
            await asyncio.sleep(0.5)
            assert not hits and not recorder.messages, (hits, recorder.messages)
            assert channel.stats()["media_images_fetched"] == 0


@check
async def voice_notes_are_transcribed_for_the_agent():
    import base64

    from aiohttp import web

    load_plugins()
    calls: list[dict] = []

    async def transcribe(request):
        form = await request.post()
        upload = form["file"]
        calls.append(
            {
                "auth": request.headers.get("Authorization"),
                "model": form.get("model"),
                "language": form.get("language"),
                "filename": upload.filename,
                "size": len(upload.file.read()),
            }
        )
        return web.json_response({"text": "你好，帮我查一下订单"})

    replies = {"get_record": lambda frame: {"file": "/napcat/cache/v.mp3", "base64": base64.b64encode(MP3).decode()}}
    async with http_server([web.post("/v1/audio/transcriptions", transcribe)]) as port, FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(
                manager,
                napcat,
                "it-voice",
                asr_base_url=f"http://127.0.0.1:{port}/v1",
                asr_api_key="sk-test",
                asr_model="whisper-x",
            )
            await napcat.emit(voice_event("private", message_id=80))
            await wait_for(lambda: recorder.messages, what="voice turn")
            text = recorder.messages[0].text
            assert "自动转写" in text and "帮我查一下订单" in text, text
            assert calls == [
                {"auth": "Bearer sk-test", "model": "whisper-x", "language": "zh", "filename": "voice.mp3", "size": len(MP3)}
            ], calls
            frames = napcat.called("get_record")
            assert [frame["params"] for frame in frames] == [{"file": "v.amr", "out_format": "mp3"}], frames
            stored = channel.history_store().query(channel_id="it-voice", chat_type="private", chat_id="10001")
            assert stored and "语音转文字：你好，帮我查一下订单" in stored[0].text, stored
            assert channel.stats()["voice_transcribed"] == 1 and channel.stats()["asr_configured"] is True


@check
async def voice_conversion_falls_back_and_failures_keep_the_placeholder():
    import base64

    from aiohttp import web

    load_plugins()
    asked: list = []

    def get_record(frame):
        fmt = frame["params"].get("out_format")
        asked.append(fmt)
        if fmt in ("mp3", "wav"):
            return RuntimeError("ffmpeg unavailable")
        return {"file": "/napcat/cache/v.amr", "base64": base64.b64encode(b"#!AMR\n" + b"\x01" * 40).decode()}

    async def broken(request):
        return web.Response(status=500, text='{"error": {"message": "model overloaded"}}')

    async with http_server([web.post("/v1/audio/transcriptions", broken)]) as port, FakeNapCat(replies={"get_record": get_record}) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-voice-fail", asr_base_url=f"http://127.0.0.1:{port}/v1")
            await napcat.emit(voice_event("private", message_id=81))
            await wait_for(lambda: recorder.messages, what="failed voice turn")
            assert asked == ["mp3", "wav", None], asked
            assert "[语音]" in recorder.messages[0].text, recorder.messages[0].text
            assert "自动转写" not in recorder.messages[0].text
            await wait_for(lambda: napcat.sent("send_private_msg"), what="the agent still answers")
            assert channel.stats()["voice_failed"] == 1 and channel.stats()["voice_transcribed"] == 0


@check
async def voice_is_left_alone_without_asr_and_in_passive_groups():
    import base64

    from aiohttp import web

    load_plugins()
    asr_hits: list = []

    async def transcribe(request):
        asr_hits.append(request.path)
        return web.json_response({"text": "不该被调用"})

    replies = {"get_record": lambda frame: {"file": "x", "base64": base64.b64encode(MP3).decode()}}
    async with http_server([web.post("/v1/audio/transcriptions", transcribe)]) as port, FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            # (1) nothing to transcribe with (no service, NapCat's own ASR switched off):
            #     no round trip at all, the placeholder stays.
            await add_channel(manager, napcat, "it-voice-off", asr_napcat_fallback="false")
            await napcat.emit(voice_event("private", message_id=82))
            await wait_for(lambda: recorder.messages, what="no-asr turn")
            assert "[语音]" in recorder.messages[0].text
            assert not napcat.called("get_record") and not napcat.called("fetch_ptt_text")

            # (2) ASR configured, but a group voice nobody addressed never reaches the agent -> no cost.
            recorder.messages.clear()
            await add_channel(manager, napcat, "it-voice-passive", asr_base_url=f"http://127.0.0.1:{port}/v1")
            await napcat.emit(voice_event("group", message_id=83))
            await asyncio.sleep(0.5)
            assert not asr_hits and not napcat.called("get_record"), "passive group voice must not be transcribed"
            assert not napcat.called("fetch_ptt_text"), "... nor sent to NapCat's own ASR"

            # (3) always-mode group: every message reaches the agent, so it is transcribed.
            await add_channel(
                manager, napcat, "it-voice-always", asr_base_url=f"http://127.0.0.1:{port}/v1", group_reply_mode="always"
            )
            await napcat.emit(voice_event("group", message_id=84, user_id=10003))
            await wait_for(lambda: asr_hits, what="always-mode transcription")


@check
async def napcat_builtin_asr_transcribes_when_no_service_is_configured():
    load_plugins()
    replies = {"fetch_ptt_text": lambda frame: {"text": "  你好，\n帮我   查一下订单 "}}
    async with FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-voice-napcat")
            await napcat.emit(voice_event("private", message_id=85))
            await wait_for(lambda: recorder.messages, what="napcat asr turn")
            text = recorder.messages[0].text
            assert "自动转写" in text and "你好， 帮我 查一下订单" in text, text
            assert "[语音]" not in text, "the placeholder is replaced by the transcript"
            assert [frame["params"] for frame in napcat.called("fetch_ptt_text")] == [{"message_id": 85}]
            assert not napcat.called("get_record"), "NapCat's own ASR needs no audio download"
            stats = channel.stats()
            assert stats["voice_transcribed"] == 1 and stats["voice_transcribed_napcat"] == 1, stats
            assert stats["voice_failed"] == 0 and stats["asr_configured"] is False and stats["asr_napcat_fallback"] is True
            stored = channel.history_store().query(channel_id="it-voice-napcat", chat_type="private", chat_id="10001")
            assert stored and "语音转文字：你好， 帮我 查一下订单" in stored[0].text, stored


@check
async def napcat_asr_is_the_second_chance_when_the_external_service_fails():
    import base64

    from aiohttp import web

    load_plugins()

    async def broken(request):
        return web.Response(status=503, text="down for maintenance")

    replies = {
        "get_record": lambda frame: {"file": "x", "base64": base64.b64encode(MP3).decode()},
        "fetch_ptt_text": lambda frame: {"text": "改用 QQ 自带转写"},
    }
    async with http_server([web.post("/v1/audio/transcriptions", broken)]) as port, FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-voice-second", asr_base_url=f"http://127.0.0.1:{port}/v1")
            await napcat.emit(voice_event("private", message_id=86))
            await wait_for(lambda: recorder.messages, what="second-chance turn")
            assert "改用 QQ 自带转写" in recorder.messages[0].text, recorder.messages[0].text
            assert napcat.called("get_record"), "the external service was tried first"
            stats = channel.stats()
            assert stats["voice_transcribed"] == 1 and stats["voice_transcribed_napcat"] == 1 and stats["voice_failed"] == 0, stats


@check
async def napcat_asr_failures_keep_the_placeholder():
    load_plugins()
    # NapCat refuses (message gone from its cache / no text): fetch_ptt_text fails with retcode 1200.
    replies = {"fetch_ptt_text": lambda frame: RuntimeError("消息不存在")}
    async with FakeNapCat(replies=replies) as napcat:
        recorder = Recorder()
        async with core(recorder) as manager:
            channel = await add_channel(manager, napcat, "it-voice-napcat-fail")
            await napcat.emit(voice_event("private", message_id=87))
            await wait_for(lambda: recorder.messages, what="failed napcat asr turn")
            assert "[语音]" in recorder.messages[0].text and "自动转写" not in recorder.messages[0].text
            await wait_for(lambda: napcat.sent("send_private_msg"), what="the agent still answers")
            stats = channel.stats()
            assert stats["voice_failed"] == 1 and stats["voice_transcribed"] == 0, stats


@check
async def send_voice_works_as_a_real_structured_tool():
    load_plugins()
    tools = {tool.name: tool for tool in build_plugin_tools()}
    assert "onebot11_send_voice" in tools, sorted(tools)
    async with FakeNapCat() as napcat:
        async with core(Recorder()) as manager:
            await add_channel(manager, napcat, "it-voice-tool")
            config = {"configurable": {"plugin_tool_configs": {"onebot11_send_voice": {"channel_id": "it-voice-tool"}}}}
            result = await tools["onebot11_send_voice"].ainvoke(
                {"voice": "https://example.com/hello.mp3", "group_id": "20002"}, config=config
            )
            assert result.startswith("已发送语音到群 20002"), result
            await wait_for(lambda: napcat.sent("send_group_msg"), what="voice send")
            message = napcat.sent("send_group_msg")[0]["params"]["message"]
            assert message == [{"type": "record", "data": {"file": "https://example.com/hello.mp3"}}], message
            refused = await tools["onebot11_send_voice"].ainvoke(
                {"voice": "https://example.com/hello.mp3"}, config=config
            )
            assert refused.startswith("发送失败") and "必须且只能提供一个" in refused, refused


# ---------------------------------------------------------------------------
# Agent tools as real StructuredTools with the real run-config plumbing
# ---------------------------------------------------------------------------


def run_config(channel_id: str | None) -> dict:
    configs = {SEND_GROUP_TOOL: {"channel_id": channel_id}} if channel_id else {}
    return {"configurable": {"plugin_tool_configs": configs}}


@check
async def tools_honour_the_per_agent_channel_binding():
    load_plugins()
    tools = {tool.name: tool for tool in build_plugin_tools()}
    assert SEND_GROUP_TOOL in tools, sorted(tools)
    send = tools[SEND_GROUP_TOOL]
    args = {"group_id": "20002", "text": "来自工具"}

    async with FakeNapCat() as napcat_a, FakeNapCat() as napcat_b:
        async with core(Recorder()) as manager:
            await add_channel(manager, napcat_a, "it-tool-a")
            await add_channel(manager, napcat_b, "it-tool-b")

            ambiguous = await send.ainvoke(args, config=run_config(None))
            assert ambiguous.startswith("发送失败") and "无法自动选择" in ambiguous, ambiguous
            assert not napcat_a.sent() and not napcat_b.sent()

            bound = await send.ainvoke(args, config=run_config("it-tool-b"))
            assert bound.startswith("已发送到群 20002"), bound
            await wait_for(lambda: napcat_b.sent("send_group_msg"), what="bound send")
            assert "来自工具" in json.dumps(napcat_b.sent("send_group_msg")[0], ensure_ascii=False)
            assert not napcat_a.sent(), "binding must keep the message off the other channel"

            hijack = await send.ainvoke({**args, "channel_id": "it-tool-a"}, config=run_config("it-tool-b"))
            assert hijack.startswith("发送失败") and "绑定" in hijack, hijack
            assert not napcat_a.sent()


@check
async def history_tools_work_as_real_structured_tools():
    """R10 through the real stack: ChannelManager stores the chat, StructuredTool reads it back."""
    load_plugins()
    tools = {tool.name: tool for tool in build_plugin_tools()}
    assert "onebot11_query_history" in tools and "onebot11_history_overview" in tools, sorted(tools)
    query, overview = tools["onebot11_query_history"], tools["onebot11_history_overview"]
    assert "group_id" in query.args and "keyword" in query.args and "before_message_id" in query.args, query.args

    def group_run(session_key: str, **tool_configs) -> dict:
        return {"configurable": {"session_key": session_key, "plugin_tool_configs": tool_configs}}

    async with FakeNapCat() as napcat_a, FakeNapCat() as napcat_b:
        async with core(Recorder(reply="收到啦")) as manager:
            channel_a = await add_channel(manager, napcat_a, "it-hist-a", group_reply_mode="always")
            await add_channel(manager, napcat_b, "it-hist-b", group_reply_mode="always")

            await napcat_a.emit(message_event("group", "周五发布新版本", message_id=11, user_id=10001, group_id=20002))
            await napcat_a.emit(message_event("group", "另一个群的话", message_id=12, user_id=10009, group_id=30003))
            await wait_for(lambda: len(napcat_a.sent("send_group_msg")) >= 2, what="bot replies")

            bound = {"onebot11_query_history": {"channel_id": "it-hist-a"}}
            here = group_run("agent1:onebot11:group:20002:group", **bound)
            out = await query.ainvoke({"keyword": "发布"}, config=here)
            assert "群 20002 的聊天记录" in out and "U10001(10001)" in out, out
            assert "周五发布新版本" in out and "另一个群" not in out, out
            # The bot's reply is part of the history with the id NapCat returned.
            both = await query.ainvoke({}, config=here)
            assert "我(机器人): 收到啦" in both, both
            assert re.search(r"收到啦 \(#700\d\)", both), both

            # Another group is off limits from inside group 20002, and the model cannot opt itself in.
            denied = await query.ainvoke({"group_id": "30003"}, config=here)
            assert denied.startswith("查询失败") and "只能查询本会话" in denied and "另一个群" not in denied, denied
            # Channel B has its own database: nothing leaks across channels.
            other = await query.ainvoke(
                {"group_id": "20002"},
                config=group_run("agent1:onebot11:group:20002:group", onebot11_query_history={"channel_id": "it-hist-b"}),
            )
            assert "没有找到" in other or "没有任何记录" in other, other

            listing = await overview.ainvoke(
                {"list_chats": True},
                config=group_run("agent1:dashboard:1:dm", onebot11_history_overview={"channel_id": "it-hist-a"}),
            )
            assert "20002" in listing and "30003" in listing, listing

            # The same store sits on disk (owner-only) under the isolated home directory.
            path = channel_a.history_path()
            assert path.startswith(str(Path(os.environ["HOME"]))) and os.path.isfile(path), path


# ---------------------------------------------------------------------------
# Tool matrix through a real agent graph (R12)
# ---------------------------------------------------------------------------


def tool_graph(tools_by_name: dict, calls: list):
    """A real LangGraph ``StateGraph`` whose model "decides" to call the given tools, run by a real ``ToolNode``."""
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    async def model(state):
        return {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": name, "args": args, "id": f"call-{index}"}
                        for index, (name, args) in enumerate(calls)
                    ],
                )
            ]
        }

    builder = StateGraph(MessagesState)
    builder.add_node("model", model)
    builder.add_node("tools", ToolNode(list(tools_by_name.values())))
    builder.add_edge(START, "model")
    builder.add_edge("model", "tools")
    builder.add_edge("tools", END)
    return builder.compile()


@check
async def admin_actions_follow_the_sender_through_a_real_agent_graph():
    """The whole chain: NapCat event -> real ChannelManager -> processor runs a real LangGraph agent -> tool.

    The processor stands in for Octop's agent: it executes a real graph in the same task that the channel's
    worker uses, which is how the harness drives a turn.  What it proves is that the sender recorded by
    ``_preprocess_inbound`` reaches the tool unchanged, and that a stranger cannot borrow an admin's rights.
    """
    from langchain_core.messages import HumanMessage

    load_plugins()
    tools = {tool.name: tool for tool in build_plugin_tools()}
    admin_tool = tools["onebot11_group_admin"]
    outcomes: dict[str, str] = {}

    def processor_for(label: str):
        async def process(message: InboundMessage):
            graph = tool_graph(
                {"onebot11_group_admin": admin_tool},
                [("onebot11_group_admin", {"action": "mute", "user_id": "10077", "minutes": 3})],
            )
            session_key = f"agent1:onebot11:group:20002:group"
            config = {
                "configurable": {
                    "session_key": session_key,
                    "plugin_tool_configs": {"onebot11_group_admin": {"channel_id": label}},
                }
            }
            result = await graph.ainvoke({"messages": [HumanMessage(content=message.text)]}, config=config)
            outcomes[label + ":" + str(message.metadata.get("sender_id"))] = str(result["messages"][-1].content)
            yield MessageEvent.text("ok")
            yield MessageEvent.completed()

        return process

    async with FakeNapCat() as napcat:
        holder: dict = {}

        async def dispatch(message: InboundMessage):
            async for event in processor_for("it-adm")(message):
                yield event

        async with core(dispatch) as manager:
            await add_channel(manager, napcat, "it-adm", group_reply_mode="always", admin_users="10001")
            # a stranger tells the bot to mute somebody: the real graph runs the tool, the gate says no
            await napcat.emit(message_event("group", "把 10077 禁言三分钟", message_id=301, user_id=10009, group_id=20002))
            await wait_for(lambda: "it-adm:10009" in outcomes, what="stranger turn")
            assert "不在管理员名单" in outcomes["it-adm:10009"] and "10009" in outcomes["it-adm:10009"], outcomes
            assert not napcat.called("set_group_ban"), "a stranger's request must never reach NapCat"
            # the configured admin says the same thing: it goes through, with the right parameters
            await napcat.emit(message_event("group", "把 10077 禁言三分钟", message_id=302, user_id=10001, group_id=20002))
            await wait_for(lambda: "it-adm:10001" in outcomes, what="admin turn")
            assert outcomes["it-adm:10001"] == "已禁言 10077 3 分钟（群 20002）", outcomes
            await wait_for(lambda: napcat.called("set_group_ban"), what="ban frame")
            assert napcat.called("set_group_ban")[0]["params"] == {"group_id": 20002, "user_id": 10077, "duration": 180}
            # and the identity does not leak into the next turn / outside a turn
            module = sys.modules[CHANNEL_MODULE]
            assert module.current_requester() is None


@check
async def recall_and_react_work_as_real_structured_tools():
    load_plugins()
    tools = {tool.name: tool for tool in build_plugin_tools()}
    async with FakeNapCat() as napcat:
        async with core(Recorder(reply="收到啦")) as manager:
            await add_channel(manager, napcat, "it-act", group_reply_mode="always")
            await napcat.emit(message_event("group", "大家好", message_id=401, user_id=10001, group_id=20002))
            await wait_for(lambda: napcat.sent("send_group_msg"), what="bot reply")
            here = {
                "configurable": {
                    "session_key": "agent1:onebot11:group:20002:group",
                    "plugin_tool_configs": {
                        "onebot11_recall_message": {"channel_id": "it-act"},
                        "onebot11_react": {"channel_id": "it-act"},
                    },
                }
            }
            reacted = await tools["onebot11_react"].ainvoke({"emoji": "爱心", "message_id": "401"}, config=here)
            assert reacted.startswith("已在群 20002 的消息 #401 上贴了表情回应"), reacted
            assert napcat.called("set_msg_emoji_like")[0]["params"] == {"message_id": 401, "emoji_id": "66", "set": True}
            recalled = await tools["onebot11_recall_message"].ainvoke({}, config=here)
            assert recalled.startswith("已撤回机器人在群 20002 发出的消息 #7001"), recalled
            assert napcat.called("delete_msg")[0]["params"] == {"message_id": 7001}
            stranger = await tools["onebot11_recall_message"].ainvoke({"message_id": "401"}, config=here)
            assert stranger.startswith("操作失败") and "找不到机器人自己发出的消息" in stranger, stranger
            assert len(napcat.called("delete_msg")) == 1


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


async def run_all(selected: list[str]) -> int:
    failures = 0
    for fn in CHECKS:
        if selected and fn.__name__ not in selected:
            continue
        started = time.monotonic()
        try:
            await asyncio.wait_for(fn(), timeout=60)
        except BaseException as exc:  # noqa: BLE001 - report every failing check
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            failures += 1
            print(f"FAIL {fn.__name__}: {exc.__class__.__name__}: {exc}")
            traceback.print_exc(limit=6)
        else:
            print(f"ok   {fn.__name__} ({time.monotonic() - started:.1f}s)")
    if _loaded:
        with contextlib.suppress(Exception):
            unregister_channel_kind("onebot11", plugin_id=CHANNEL_PLUGIN_ID)
        for plugin_id in (TOOLS_PLUGIN_ID, CHANNEL_PLUGIN_ID):
            with contextlib.suppress(Exception):
                unload_plugin(plugin_id)
    return failures


if __name__ == "__main__":
    failed = asyncio.run(run_all(sys.argv[1:]))
    if failed:
        print(f"{failed} core integration check(s) failed")
        raise SystemExit(1)
    print("core integration checks passed")
