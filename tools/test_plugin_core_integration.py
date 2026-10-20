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
import contextlib
import dataclasses
import json
import os
import socket
import sys
import tempfile
import time
import traceback
from pathlib import Path

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

    def __init__(self, self_id: int = 10000):
        self.self_id = self_id
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
                await ws.send(
                    json.dumps(
                        {"status": "ok", "retcode": 0, "data": data, "echo": frame.get("echo")}
                    )
                )
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
        ["onebot11_send_forward_group", "onebot11_send_group", "onebot11_send_private"]
    ), tool_names
    for tool in tools_plugin.tools:
        assert [field["name"] for field in tool.config_fields] == ["channel_id"], tool.name


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
            assert "echo:你好" in json.dumps(reply, ensure_ascii=False)
            assert len(recorder.messages) == 1
            message = recorder.messages[0]
            assert isinstance(message, InboundMessage)
            assert message.channel_subject.subject_id == "user:10001"
            assert message.metadata["sender_id"] == "10001"
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
