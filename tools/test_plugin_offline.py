"""Offline unit checks for the OneBot 11 plugin.

The real Octop runtime is not required: lightweight stand-ins for the
``octop_gateway`` contracts are injected before importing ``main.py``.  This
keeps the wire-format logic (event parsing, CQ fallback, chunking, routing)
testable on a workstation without Octop or a running NapCat.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Every temp file the checks create lives under one directory that is removed
# on exit, so a run leaves nothing behind (also on shared hosts).
_SCRATCH = tempfile.mkdtemp(prefix="ob11-offline-")
tempfile.tempdir = _SCRATCH
atexit.register(shutil.rmtree, _SCRATCH, ignore_errors=True)

# ---------------------------------------------------------------------------
# Stub octop_gateway
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

    @classmethod
    def from_dict(cls, data):
        from dataclasses import fields as _fields

        known = {f.name for f in _fields(cls)}
        out = {k: (v.strip() if isinstance(v, str) else v) for k, v in data.items() if k in known}
        for alias, canonical in getattr(cls, "field_aliases", {}).items():
            if canonical in known and not out.get(canonical) and data.get(alias):
                out[canonical] = data[alias]
        return cls(**out)


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
        self._config = config
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

# Stub websockets (import-time only)
ws_mod = types.ModuleType("websockets")
ws_mod.connect = lambda *a, **k: None
ws_exc = types.ModuleType("websockets.exceptions")


class ConnectionClosed(Exception):
    pass


class InvalidStatus(Exception):
    pass


ws_exc.ConnectionClosed = ConnectionClosed
ws_exc.InvalidStatus = InvalidStatus
ws_mod.exceptions = ws_exc
sys.modules["websockets"] = ws_mod
sys.modules["websockets.exceptions"] = ws_exc


def _load_plugin():
    path = Path(__file__).resolve().parents[1] / "plugins" / "octop-onebot11" / "main.py"
    spec = importlib.util.spec_from_file_location("plugin_main", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["plugin_main"] = module
    spec.loader.exec_module(module)
    return module


def main() -> None:
    plugin = _load_plugin()
    cfg = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "access_token": "dev-token",
            "group_reply_mode": "mention",
            "reply_with_quote": "false",
        },
    )
    assert cfg.token == "dev-token", "access_token alias must map to token"
    assert cfg.reply_with_quote is False
    channel = plugin.OneBot11Channel(lambda _msg: None, config=cfg, channel_id="ch-1", tenant_id="agent-1")

    group_event = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 9001,
        "group_id": 20002,
        "user_id": 10001,
        "self_id": 10000,
        "time": 1700000000,
        "message": [
            {"type": "at", "data": {"qq": "10000"}},
            {"type": "text", "data": {"text": " 你好"}},
        ],
        "sender": {"user_id": 10001, "nickname": "测试用户", "card": "测试用户"},
    }
    msg = channel.parse_inbound(group_event)
    assert msg.channel_type == "onebot11"
    assert msg.channel_subject.subject_id == "group:20002"
    assert msg.channel_subject.chat_type == "group"
    assert msg.metadata["bot_mentioned"] is True
    assert "你好" in msg.text
    assert msg.channel_session_id == "onebot11:10000:group:20002"

    private_event = {
        "post_type": "message",
        "message_type": "private",
        "message_id": 9002,
        "user_id": 10001,
        "self_id": 10000,
        "message": "[CQ:reply,id=88][CQ:text,text=hi]",
        "sender": {"user_id": 10001, "nickname": "A"},
    }
    msg2 = channel.parse_inbound(private_event)
    assert msg2.channel_subject.subject_id == "user:10001"
    assert msg2.channel_subject.chat_type == "direct"
    assert msg2.metadata["reply_to"] == "88"

    assert channel._split_text("a" * 3200)
    assert all(len(chunk) <= 1500 for chunk in channel._split_text("a" * 3200))

    action, params = channel._build_send_action(msg.channel_subject, [{"type": "text", "data": {"text": "x"}}])
    assert action == "send_group_msg"
    assert params["group_id"] == 20002

    action, params = channel._build_send_action(msg2.channel_subject, [{"type": "text", "data": {"text": "x"}}])
    assert action == "send_private_msg"
    assert params["user_id"] == 10001

    # Proactive group routing: persisted sessions may carry chat_type="dm"
    # while group_id / subject_id still identify a group.
    stale_group = plugin.ChannelSubject(
        subject_id="group:20002",
        chat_type="dm",
        metadata={"group_id": "20002", "message_type": "group", "user_id": "10001"},
    )
    action, params = channel._build_send_action(stale_group, [{"type": "text", "data": {"text": "x"}}])
    assert action == "send_group_msg", (action, params)
    assert params["group_id"] == 20002

    sparse_group = plugin.ChannelSubject(subject_id="group:20003", chat_type="", metadata={})
    action, params = channel._build_send_action(sparse_group, [{"type": "text", "data": {"text": "x"}}])
    assert action == "send_group_msg", (action, params)
    assert params["group_id"] == 20003
    enriched = channel._enrich_push_metadata(sparse_group, {})
    assert enriched["group_id"] == "20003"
    assert enriched["chat_type"] == "group"

    assert msg.metadata["chat_type"] == "group"
    assert msg.metadata["is_group"] is True
    assert msg2.metadata["chat_type"] == "dm"

    # Keyword trigger: group message without @ still wakes the agent.
    cfg_kw = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "trigger_keywords": "日报, 提醒",
            "group_history_limit": "5",
            "private_reply_mode": "keyword",
        },
    )
    channel_kw = plugin.OneBot11Channel(lambda _msg: None, config=cfg_kw, channel_id="ch-kw", tenant_id="agent-1")
    keyword_event = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 9101,
        "group_id": 20002,
        "user_id": 10001,
        "self_id": 10000,
        "message": [{"type": "text", "data": {"text": "帮我生成今天的日报"}}],
        "sender": {"user_id": 10001, "nickname": "测试用户"},
    }
    kw_msg = channel_kw.parse_inbound(keyword_event)
    assert kw_msg.metadata["bot_mentioned"] is True
    assert kw_msg.metadata["keyword_triggered"] is True
    assert kw_msg.metadata["trigger_reason"].startswith("keyword:")
    assert cfg_kw.group_context.history_limit == 5

    plain_event = dict(keyword_event, message_id=9102, message=[{"type": "text", "data": {"text": "随便聊聊"}}])
    plain_msg = channel_kw.parse_inbound(plain_event)
    assert plain_msg.metadata["bot_mentioned"] is False

    before = len(channel_kw.enqueued)
    asyncio.run(
        channel_kw._handle_message_event(
            {
                "post_type": "message",
                "message_type": "private",
                "message_id": 9103,
                "user_id": 10001,
                "self_id": 10000,
                "message": [{"type": "text", "data": {"text": "普通私聊"}}],
                "sender": {"user_id": 10001, "nickname": "A"},
            },
        )
    )
    assert len(channel_kw.enqueued) == before, "private keyword mode must drop non-keyword messages"
    asyncio.run(
        channel_kw._handle_message_event(
            {
                "post_type": "message",
                "message_type": "private",
                "message_id": 9104,
                "user_id": 10001,
                "self_id": 10000,
                "message": [{"type": "text", "data": {"text": "提醒我开会"}}],
                "sender": {"user_id": 10001, "nickname": "A"},
            },
        )
    )
    assert len(channel_kw.enqueued) == before + 1, "keyword private message must be enqueued"

    # Proactive send tool: resolves the live channel and routes to a group.
    sys.modules["harness_plugin_octop_onebot11"] = plugin
    tools_path = Path(__file__).resolve().parents[1] / "plugins" / "octop-onebot11-tools" / "main.py"
    tools_spec = importlib.util.spec_from_file_location("plugin_tools_test", tools_path)
    tools_mod = importlib.util.module_from_spec(tools_spec)
    sys.modules["plugin_tools_test"] = tools_mod
    assert tools_spec.loader is not None
    tools_spec.loader.exec_module(tools_mod)

    channel._connected_transport = True
    channel._login_confirmed = True
    # Earlier checks left other channel instances in the process-wide registry;
    # auto-selection is only allowed when exactly one channel is live (the
    # multi-channel refusal is covered in test_plugin_regression.py).
    for stale_id in [cid for cid, other in plugin._LIVE_CHANNELS.items() if other is not channel]:
        plugin._LIVE_CHANNELS.pop(stale_id)
    assert plugin.list_live_channels() == [channel]
    pushed = {}

    async def fake_push_segments(subject, segments):
        pushed["subject"] = subject
        pushed["segments"] = segments

    channel.push_segments = fake_push_segments
    result = asyncio.run(tools_mod.onebot11_send_group("20002", "群通知"))
    assert result.startswith("已发送到群"), result
    assert pushed["subject"].chat_type == "group"
    assert pushed["subject"].metadata["group_id"] == "20002"
    assert pushed["segments"][-1] == {"type": "text", "data": {"text": "群通知"}}

    # Rich segments: @ + image ride along with the text.
    result = asyncio.run(
        tools_mod.onebot11_send_group(
            "20002", "看图", image_url="https://example.com/a.png", at_user_id="10001"
        )
    )
    assert result.startswith("已发送到群"), result
    segs = pushed["segments"]
    assert segs[0] == {"type": "at", "data": {"qq": "10001"}}
    assert segs[1] == {"type": "image", "data": {"file": "https://example.com/a.png"}}
    assert segs[2] == {"type": "text", "data": {"text": "看图"}}

    # Argument validation: group id / at least one payload are mandatory.
    assert asyncio.run(tools_mod.onebot11_send_group("20002", "")).startswith("发送失败")
    assert asyncio.run(tools_mod.onebot11_send_group("", "x")).startswith("发送失败")
    assert asyncio.run(tools_mod.onebot11_send_private("10001", "")).startswith("发送失败")

    # While disconnected the tool reports the queued state instead of failing.
    channel._connected_transport = False
    result = asyncio.run(tools_mod.onebot11_send_group("20002", "x"))
    assert result.startswith("已发送到群"), result
    assert "待发队列" in result, result
    channel._connected_transport = True

    # Reply rides first; forward tool builds proper node payloads.
    result = asyncio.run(tools_mod.onebot11_send_group("20002", "引用回复", reply_to_message_id="9001"))
    assert result.startswith("已发送到群"), result
    assert pushed["segments"][0] == {"type": "reply", "data": {"id": "9001"}}

    async def fake_push_forward(subject, nodes):
        pushed["forward"] = (subject, nodes)

    channel.push_forward = fake_push_forward
    result = asyncio.run(
        tools_mod.onebot11_send_forward_group(
            "20002", '[{"name": "早报", "text": "第一条"}, {"name": "早报", "text": "第二条"}]'
        )
    )
    assert result.startswith("已发送合并转发"), result
    fwd_nodes = pushed["forward"][1]
    assert fwd_nodes[0]["type"] == "node"
    assert fwd_nodes[0]["data"]["content"][0]["data"]["text"] == "第一条"
    assert fwd_nodes[1]["data"]["name"] == "早报"
    assert asyncio.run(tools_mod.onebot11_send_forward_group("20002", "[]")).startswith("发送失败")
    assert asyncio.run(tools_mod.onebot11_send_forward_group("20002", "not-json")).startswith("发送失败")

    # Reverse mode: config normalization, credential policy, token checks.
    cfg_rev = plugin.OneBot11Config.from_dict(
        {
            "connection_mode": "reverse",
            "listen_port": "3002",
            "listen_path": "onebot/v11/ws",
            "token": "s3cret",
        },
    )
    assert cfg_rev.is_reverse is True
    assert cfg_rev.listen_port == 3002
    assert cfg_rev.listen_path == "/onebot/v11/ws"
    assert cfg_rev.missing_credentials() == [], "reverse mode must not require ws_url"

    forward_cfg = plugin.OneBot11Config.from_dict({"connection_mode": "forward"})
    assert forward_cfg.normalized_mode() == "forward"
    assert forward_cfg.missing_credentials() == ["ws_url"]

    class _FakeRequest:
        def __init__(self, path, headers):
            self.path = path
            self.headers = headers

    class _FakeWS:
        def __init__(self, path="/onebot/v11/ws", headers=None):
            self.request = _FakeRequest(path, headers or {})

    rev_channel = plugin.OneBot11Channel(
        lambda _m: None, config=cfg_rev, channel_id="ch-rev", tenant_id="agent-1"
    )
    assert rev_channel._reverse_authorized(
        _FakeWS(headers={"Authorization": "Bearer s3cret"}), "/onebot/v11/ws"
    )
    assert rev_channel._reverse_authorized(_FakeWS(), "/onebot/v11/ws?access_token=s3cret")
    assert not rev_channel._reverse_authorized(
        _FakeWS(headers={"Authorization": "Bearer wrong"}), "/onebot/v11/ws"
    )
    assert not rev_channel._reverse_authorized(_FakeWS(), "/onebot/v11/ws")
    assert (
        rev_channel._request_target(_FakeWS(path="/ignored"), "/onebot/v11/ws?access_token=x")
        == "/onebot/v11/ws?access_token=x"
    )

    open_rev = plugin.OneBot11Config(connection_mode="reverse", listen_port=3002)
    open_channel = plugin.OneBot11Channel(
        lambda _m: None, config=open_rev, channel_id="ch-rev-open", tenant_id="agent-1"
    )
    assert open_channel._reverse_authorized(_FakeWS(), "/onebot/v11/ws"), "empty token accepts any client"

    # start() validates the mode-specific required fields before spawning tasks.
    try:
        asyncio.run(
            plugin.OneBot11Channel(
                lambda _m: None,
                config=plugin.OneBot11Config(ws_url=""),
                channel_id="ch-bad-forward",
                tenant_id="agent-1",
            ).start()
        )
    except ValueError as exc:
        assert "ws_url" in str(exc), exc
    else:
        raise AssertionError("forward mode without ws_url must fail fast")

    try:
        asyncio.run(
            plugin.OneBot11Channel(
                lambda _m: None,
                config=plugin.OneBot11Config(connection_mode="reverse", listen_port=0),
                channel_id="ch-bad-reverse",
                tenant_id="agent-1",
            ).start()
        )
    except ValueError as exc:
        assert "listen_port" in str(exc), exc
    else:
        raise AssertionError("reverse mode with invalid port must fail fast")

    # ------------------------------------------------------------------
    # v0.3.0: outbox, whitelists, configurable rate limit, rich push.
    # ------------------------------------------------------------------

    state_dir = tempfile.mkdtemp()

    cfg_v3 = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "allowed_groups": "111, 222",
            "allowed_users": "10001",
            "outbox_enabled": "true",
            "outbox_max_items": "2",
            "outbox_ttl_seconds": "3600",
            "outbox_max_attempts": "2",
            "send_rate_limit_count": "5",
            "send_rate_limit_window": "30",
            "outbox_persist_dir": state_dir,
        }
    )
    assert cfg_v3.outbox_enabled is True
    assert cfg_v3.outbox_max_items == 2
    assert cfg_v3.outbox_max_attempts == 2
    assert cfg_v3.send_rate_limit_count == 5
    assert cfg_v3.send_rate_limit_window == 30.0
    ch_v3 = plugin.OneBot11Channel(lambda _m: None, config=cfg_v3, channel_id="ch-v3", tenant_id="agent-1")
    assert ch_v3._allowed_groups == frozenset({"111", "222"})
    assert ch_v3._allowed_users == frozenset({"10001"})
    assert ch_v3._default_constraints().send_rate_limit == (5, 30.0)

    # send_rate_limit_count=0 disables the constraint entirely.
    cfg_nolimit = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "send_rate_limit_count": "0"}
    )
    ch_nolimit = plugin.OneBot11Channel(lambda _m: None, config=cfg_nolimit, channel_id="ch-nolimit", tenant_id="a")
    assert ch_nolimit._default_constraints().send_rate_limit is None

    # Whitelists: strict push denied, passive send silently dropped.
    assert ch_v3._send_denial(plugin.ChannelSubject(subject_id="group:333", chat_type="group")) is not None
    assert ch_v3._send_denial(plugin.ChannelSubject(subject_id="group:111", chat_type="group")) is None
    assert ch_v3._send_denial(plugin.ChannelSubject(subject_id="user:99999", chat_type="dm")) is not None
    assert ch_v3._send_denial(plugin.ChannelSubject(subject_id="user:10001", chat_type="dm")) is None

    async def _run_strict_denied():
        await ch_v3.push_segments(
            plugin.ChannelSubject(subject_id="group:333", chat_type="group", metadata={"group_id": "333"}),
            [{"type": "text", "data": {"text": "hi"}}],
        )

    try:
        asyncio.run(_run_strict_denied())
    except PermissionError as exc:
        assert "allowed_groups" in str(exc), exc
    else:
        raise AssertionError("group outside whitelist must be denied")

    ch_v3._connected_transport = True
    ch_v3._login_confirmed = True
    passive_calls = []

    async def fake_call_api(action, params, *, timeout=None, account=""):
        passive_calls.append((action, params))
        return {"message_id": 1}

    ch_v3._call_api = fake_call_api
    asyncio.run(
        ch_v3._send_segments(
            plugin.ChannelSubject(subject_id="group:333", chat_type="group", metadata={"group_id": "333"}),
            [{"type": "text", "data": {"text": "nope"}}],
            allow_quote=False,
        )
    )
    assert passive_calls == [], "passive send to a non-whitelisted group must be dropped"
    asyncio.run(
        ch_v3._send_segments(
            plugin.ChannelSubject(subject_id="group:111", chat_type="group", metadata={"group_id": "111"}),
            [{"type": "text", "data": {"text": "ok"}}],
            allow_quote=False,
        )
    )
    assert len(passive_calls) == 1 and passive_calls[0][0] == "send_group_msg"
    assert passive_calls[0][1]["group_id"] == 111

    # Outbox: strict push while disconnected parks the message instead of raising.
    ch_v3._connected_transport = False
    ch_v3._login_confirmed = False
    ch_v3._call_api = fake_call_api  # would fail if called while disconnected
    asyncio.run(
        ch_v3.push_text(plugin.ChannelSubject(subject_id="group:111", chat_type="group",
                                              metadata={"group_id": "111", "strict_push": True}), "排队消息")
    )
    assert len(ch_v3._outbox) == 1, "strict push while disconnected must be queued"
    assert ch_v3._outbox[0].action == "send_group_msg"

    # Outbox capacity: exceeding outbox_max_items rejects the whole batch.
    assert ch_v3._enqueue_outbox([("send_group_msg", {"group_id": 111, "message": []}, "")]) is True
    assert len(ch_v3._outbox) == 2
    assert ch_v3._enqueue_outbox([("send_group_msg", {"group_id": 111, "message": []}, "")]) is False
    assert len(ch_v3._outbox) == 2

    # Flush delivers everything once connected, in order.
    ch_v3._connected_transport = True
    ch_v3._login_confirmed = True
    delivered = []

    async def flush_call_api(action, params, *, timeout=None, account=""):
        delivered.append((action, params))
        return {"message_id": 1}

    ch_v3._call_api = flush_call_api
    asyncio.run(ch_v3._flush_outbox())
    assert len(delivered) == 2, delivered
    assert len(ch_v3._outbox) == 0

    # TTL expiry drops stale items without calling the API.
    stale = plugin._OutboxItem(
        action="send_group_msg", params={"group_id": 111}, queued_at=time.monotonic() - 99999.0
    )
    ch_v3._outbox.append(stale)
    delivered.clear()
    asyncio.run(ch_v3._flush_outbox())
    assert delivered == [] and len(ch_v3._outbox) == 0

    # Exhausted retries are dropped; API errors back off and retry later.
    exhausted = plugin._OutboxItem(action="send_group_msg", params={"group_id": 111},
                                   queued_at=time.monotonic(), attempts=cfg_v3.outbox_max_attempts)
    ch_v3._outbox.append(exhausted)
    asyncio.run(ch_v3._flush_outbox())
    assert len(ch_v3._outbox) == 0, "item at max attempts must be dropped"

    async def failing_call_api(action, params, *, timeout=None, account=""):
        raise plugin.OneBotAPIError(action, 1200, "mock failure")

    ch_v3._call_api = failing_call_api
    failing = plugin._OutboxItem(action="send_group_msg", params={"group_id": 111}, queued_at=time.monotonic())
    ch_v3._outbox.append(failing)
    asyncio.run(ch_v3._flush_outbox())
    assert len(ch_v3._outbox) == 1 and ch_v3._outbox[0].attempts == 1
    assert ch_v3._outbox[0].next_try > time.monotonic()
    ch_v3._outbox[0].next_try = 0.0  # force due
    asyncio.run(ch_v3._flush_outbox())
    assert ch_v3._outbox[0].attempts == 2  # == max: next flush drops it
    ch_v3._outbox[0].next_try = 0.0
    asyncio.run(ch_v3._flush_outbox())
    assert len(ch_v3._outbox) == 0

    # Disabled outbox keeps the old behaviour: strict push raises when offline.
    cfg_nooutbox = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "outbox_enabled": "false"}
    )
    ch_nooutbox = plugin.OneBot11Channel(lambda _m: None, config=cfg_nooutbox, channel_id="ch-noout", tenant_id="a")

    async def _run_strict_offline():
        await ch_nooutbox.push_text(
            plugin.ChannelSubject(subject_id="group:111", chat_type="group",
                                  metadata={"group_id": "111", "strict_push": True}), "x"
        )

    try:
        asyncio.run(_run_strict_offline())
    except ConnectionError:
        pass
    else:
        raise AssertionError("disabled outbox + offline must raise ConnectionError")

    # Chunk composition: media rides on the first chunk, long text splits.
    chunks = ch_v3._compose_push_chunks(
        [
            {"type": "at", "data": {"qq": "10001"}},
            {"type": "image", "data": {"file": "https://example.com/a.png"}},
            {"type": "text", "data": {"text": "a" * 2000}},
        ]
    )
    assert len(chunks) >= 2, chunks
    assert [seg["type"] for seg in chunks[0]] == ["at", "image", "text"]
    assert len(chunks[0][2]["data"]["text"]) <= 1500
    assert all(seg["type"] == "text" for seg in chunks[1])
    media_only = ch_v3._compose_push_chunks([{"type": "image", "data": {"file": "https://x/b.png"}}])
    assert len(media_only) == 1 and media_only[0][0]["type"] == "image"

    # Local media paths: rejected by default; inlined as base64 only below
    # media_allowed_dirs.  URLs and base64 payloads pass through untouched.
    media_dir = tempfile.mkdtemp()
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"pngbytes"
    local_path = os.path.join(media_dir, "shot.png")
    with open(local_path, "wb") as handle:
        handle.write(png_bytes)
    local_seg = {"type": "image", "data": {"file": local_path}}
    try:
        ch_v3._materialize_media_segment(local_seg)
    except PermissionError:
        pass
    else:
        raise AssertionError("local files must be rejected unless media_allowed_dirs is set")

    cfg_media = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "media_allowed_dirs": media_dir}
    )
    ch_media = plugin.OneBot11Channel(lambda _m: None, config=cfg_media, channel_id="ch-media", tenant_id="a")
    materialized = ch_media._materialize_media_segment(local_seg)
    assert materialized["data"]["file"].startswith("base64://")
    assert base64.b64decode(materialized["data"]["file"][len("base64://"):]) == png_bytes
    via_file_url = ch_media._materialize_media_segment(
        {"type": "image", "data": {"file": "file://" + local_path}}
    )
    assert via_file_url["data"]["file"] == materialized["data"]["file"]

    def _must_raise(exc_type, segment, why):
        try:
            ch_media._materialize_media_segment(segment)
        except exc_type:
            return
        raise AssertionError(why)

    secret_path = os.path.join(tempfile.mkdtemp(), "secret.png")
    with open(secret_path, "wb") as handle:
        handle.write(png_bytes)
    _must_raise(PermissionError, {"type": "image", "data": {"file": secret_path}}, "outside allowed dirs")
    link_path = os.path.join(media_dir, "escape.png")
    os.symlink(secret_path, link_path)
    _must_raise(PermissionError, {"type": "image", "data": {"file": link_path}}, "symlink must not escape")
    traversal = os.path.join(media_dir, "..", os.path.basename(os.path.dirname(secret_path)), "secret.png")
    _must_raise(PermissionError, {"type": "image", "data": {"file": traversal}}, "../ traversal")
    notes_path = os.path.join(media_dir, "notes.txt")
    with open(notes_path, "wb") as handle:
        handle.write(b"not an image")
    _must_raise(ValueError, {"type": "image", "data": {"file": notes_path}}, "non-image bytes as image")
    assert ch_media._materialize_media_segment({"type": "file", "data": {"file": notes_path}})["data"][
        "file"
    ].startswith("base64://"), "non-image segment types skip the image magic check"
    _must_raise(ValueError, {"type": "image", "data": {"file": "relative/shot.png"}}, "relative path")
    _must_raise(ValueError, {"type": "image", "data": {"file": "ftp://host/a.png"}}, "unknown scheme")
    _must_raise(PermissionError, {"type": "image", "data": {"file": "file://evil-host/x.png"}}, "remote file://")
    _must_raise(
        ValueError,
        {"type": "image", "data": {"file": os.path.join(media_dir, "missing.png")}},
        "missing file",
    )
    url_seg = {"type": "image", "data": {"file": "https://example.com/a.png"}}
    assert ch_v3._materialize_media_segment(url_seg) is url_seg
    b64_seg = {"type": "image", "data": {"file": "base64://AAAA"}}
    assert ch_v3._materialize_media_segment(b64_seg) is b64_seg
    text_seg = {"type": "text", "data": {"text": "keep"}}
    assert ch_v3._materialize_media_segment(text_seg) is text_seg

    # ------------------------------------------------------------------
    # v0.4.0: sender policy, notices, mentions, reply/forward, HTTP, monitor
    # ------------------------------------------------------------------

    cfg_v4 = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "blocked_senders": "666",
            "allowed_senders": "111,222",
        }
    )
    ch_v4 = plugin.OneBot11Channel(lambda _m: None, config=cfg_v4, channel_id="ch-v4", tenant_id="a")

    def _sender_event(uid, mid):
        return {
            "post_type": "message",
            "message_type": "group",
            "message_id": mid,
            "group_id": 20002,
            "user_id": uid,
            "self_id": 10000,
            "message": [{"type": "text", "data": {"text": "hello"}}],
            "sender": {"user_id": uid, "nickname": "u"},
        }

    before = len(ch_v4.enqueued)
    asyncio.run(ch_v4._handle_message_event(_sender_event(666, 30001)))
    asyncio.run(ch_v4._handle_message_event(_sender_event(999, 30002)))
    assert len(ch_v4.enqueued) == before, "blocked / non-whitelisted senders must be dropped"
    asyncio.run(ch_v4._handle_message_event(_sender_event(111, 30003)))
    assert len(ch_v4.enqueued) == before + 1, "whitelisted sender must pass"

    mention_event = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 30010,
        "group_id": 20002,
        "user_id": 111,
        "self_id": 10000,
        "message": [
            {"type": "at", "data": {"qq": "30001", "name": "小明"}},
            {"type": "text", "data": {"text": " 看看"}},
        ],
        "sender": {"user_id": 111, "nickname": "u"},
    }
    m_msg = ch_v4.parse_inbound(mention_event)
    assert m_msg.metadata["mentions"] == [{"qq": "30001", "name": "小明"}]
    assert m_msg.metadata["bot_mentioned"] is False

    # Poke: enabled synthesizes a trigger, disabled stays quiet.
    cfg_poke = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "poke_triggers_reply": "true"}
    )
    ch_poke = plugin.OneBot11Channel(lambda _m: None, config=cfg_poke, channel_id="ch-poke", tenant_id="a")
    asyncio.run(
        ch_poke._handle_notice_event(
            {
                "post_type": "notice", "notice_type": "notify", "sub_type": "poke",
                "group_id": 20002, "user_id": 30001, "target_id": 10000, "self_id": 10000,
            }
        )
    )
    assert len(ch_poke.enqueued) == 1
    poke_msg = ch_poke.parse_inbound(ch_poke.enqueued[0])
    assert poke_msg.metadata["trigger_reason"] == "poke"
    assert poke_msg.metadata["bot_mentioned"] is True
    assert poke_msg.metadata["poke"] is True
    asyncio.run(
        ch_poke._handle_notice_event(
            {
                "post_type": "notice", "notice_type": "notify", "sub_type": "poke",
                "group_id": 20002, "user_id": 30001, "target_id": 30002, "self_id": 10000,
            }
        )
    )
    assert len(ch_poke.enqueued) == 1, "poke at someone else must not trigger"

    cfg_poke_off = plugin.OneBot11Config.from_dict({"ws_url": "ws://127.0.0.1:3001"})
    ch_poke_off = plugin.OneBot11Channel(lambda _m: None, config=cfg_poke_off, channel_id="ch-poke-off", tenant_id="a")
    asyncio.run(
        ch_poke_off._handle_notice_event(
            {
                "post_type": "notice", "notice_type": "notify", "sub_type": "poke",
                "group_id": 20002, "user_id": 30001, "target_id": 10000, "self_id": 10000,
            }
        )
    )
    assert ch_poke_off.enqueued == [], "poke trigger must be opt-in"

    # Friend / group requests: auto-accept issues the approval action.
    api_calls = []

    async def recording_api(action, params, *, timeout=None, account=""):
        api_calls.append((action, params))
        return {}

    ch_req = plugin.OneBot11Channel(lambda _m: None, config=cfg_poke_off, channel_id="ch-req", tenant_id="a")
    ch_req._call_api = recording_api
    asyncio.run(
        ch_req._handle_notice_event(
            {"post_type": "request", "request_type": "friend", "flag": "f-1", "self_id": 10000}
        )
    )
    assert api_calls == [], "auto-accept must be disabled by default"

    cfg_fr = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "auto_accept_friend_request": "true",
            "auto_accept_group_invite": "true",
        }
    )
    ch_fr = plugin.OneBot11Channel(lambda _m: None, config=cfg_fr, channel_id="ch-fr", tenant_id="a")
    ch_fr._call_api = recording_api
    asyncio.run(
        ch_fr._handle_notice_event(
            {"post_type": "request", "request_type": "friend", "flag": "f-2", "self_id": 10000}
        )
    )
    asyncio.run(
        ch_fr._handle_notice_event(
            {
                "post_type": "request", "request_type": "group", "sub_type": "invite",
                "flag": "f-3", "self_id": 10000,
            }
        )
    )
    assert api_calls[0][0] == "set_friend_add_request" and api_calls[0][1]["approve"] is True
    assert api_calls[1][0] == "set_group_add_request" and api_calls[1][1]["sub_type"] == "invite"
    asyncio.run(  # recall events must not crash
        ch_fr._handle_notice_event(
            {"post_type": "notice", "notice_type": "group_recall", "message_id": 5, "self_id": 10000}
        )
    )

    # Forward resolution enriches the payload; parse_inbound renders the content.
    cfg_fwd = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "resolve_forward": "true"}
    )
    ch_fwd = plugin.OneBot11Channel(lambda _m: None, config=cfg_fwd, channel_id="ch-fwd", tenant_id="a")

    async def forward_api(action, params, *, timeout=None, account=""):
        if action == "get_forward_msg":
            return [
                {
                    "sender": {"user_id": 10001, "nickname": "转发用户A"},
                    "message": [{"type": "text", "data": {"text": "转发内容第一条"}}],
                },
                {
                    "sender": {"user_id": 10002, "nickname": "转发用户B"},
                    "message": [{"type": "text", "data": {"text": "转发内容第二条"}}],
                },
            ]
        return {}

    ch_fwd._call_api = forward_api
    asyncio.run(
        ch_fwd._handle_message_event(
            {
                "post_type": "message", "message_type": "group", "message_id": 30020,
                "group_id": 20002, "user_id": 111, "self_id": 10000,
                "message": [{"type": "forward", "data": {"id": "res-1"}}],
                "sender": {"user_id": 111, "nickname": "u"},
            }
        )
    )
    fwd_payload = ch_fwd.enqueued[-1]
    assert isinstance(fwd_payload.get("_forward_messages"), list)
    fwd_msg = ch_fwd.parse_inbound(fwd_payload)
    assert "转发内容第一条" in fwd_msg.text and "转发用户B" in fwd_msg.text

    # Reply segments lead the message; forward push targets groups only.
    ch_v4._connected_transport = True
    ch_v4._login_confirmed = True
    sent_batches = []

    async def capture_api(action, params, *, timeout=None, account=""):
        sent_batches.append((action, params))
        return {"message_id": 1}

    ch_v4._call_api = capture_api
    asyncio.run(
        ch_v4.push_segments(
            plugin.build_group_subject("20002"),
            [
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "reply", "data": {"id": "9001"}},
                {"type": "text", "data": {"text": "带引用的主动消息"}},
            ],
        )
    )
    assert sent_batches[0][0] == "send_group_msg"
    first_message = sent_batches[0][1]["message"]
    assert first_message[0]["type"] == "reply" and first_message[0]["data"]["id"] == "9001"
    assert first_message[1]["type"] == "at"

    asyncio.run(
        ch_v4.push_forward(
            plugin.build_group_subject("20002"),
            [
                {
                    "type": "node",
                    "data": {
                        "name": "n",
                        "uin": "10000",
                        "content": [{"type": "text", "data": {"text": "x"}}],
                    },
                }
            ],
        )
    )
    assert sent_batches[-1][0] == "send_group_forward_msg"
    try:
        asyncio.run(ch_v4.push_forward(plugin.build_private_subject("10001"), []))
    except ValueError:
        pass
    else:
        raise AssertionError("push_forward must reject empty nodes / private subjects")

    # HTTP fallback: outbound actions succeed while the WS transport is down.
    received_http = []

    class _HTTPHandler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            received_http.append((self.path, json.loads(body or b"{}")))
            payload = json.dumps(
                {"status": "ok", "retcode": 0, "data": {"message_id": 1}}
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _HTTPHandler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        cfg_http = plugin.OneBot11Config.from_dict(
            {
                "ws_url": "ws://127.0.0.1:1",
                "http_api_url": f"http://127.0.0.1:{httpd.server_address[1]}",
            }
        )
        ch_http = plugin.OneBot11Channel(lambda _m: None, config=cfg_http, channel_id="ch-http", tenant_id="a")
        asyncio.run(ch_http.push_text(plugin.build_group_subject("20002"), "HTTP 备用消息"))
        assert received_http and received_http[0][0] == "/send_group_msg", received_http
        assert received_http[0][1]["group_id"] == 20002
        assert received_http[0][1]["message"][0]["data"]["text"] == "HTTP 备用消息"
        assert not ch_http._outbox, "successful HTTP fallback must not queue"
    finally:
        httpd.shutdown()

    # Monitor: buffer → digest trigger, with gap / keyword / cap throttles.
    cfg_mon = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "monitor_enabled": "true",
            "monitor_interval_seconds": "60",
            "monitor_min_messages": "3",
            "monitor_min_trigger_gap_seconds": "0",
            "monitor_max_per_hour": "10",
        }
    )
    ch_mon = plugin.OneBot11Channel(lambda _m: None, config=cfg_mon, channel_id="ch-mon", tenant_id="a")
    for i in range(4):
        asyncio.run(
            ch_mon._handle_message_event(
                {
                    "post_type": "message", "message_type": "group", "message_id": 31000 + i,
                    "group_id": 20002, "user_id": 31000 + i, "self_id": 10000,
                    "message": [{"type": "text", "data": {"text": f"闲聊{i}"}}],
                    "sender": {"user_id": 31000 + i, "nickname": f"群友{i}"},
                }
            )
        )
    assert ch_mon._monitor_buffer.get("20002") and len(ch_mon._monitor_buffer["20002"]) == 4
    real_enqueued = len(ch_mon.enqueued)  # mention mode also enqueues the raw events
    ch_mon._monitor_sweep()
    assert len(ch_mon.enqueued) == real_enqueued + 1, "monitor sweep must synthesize one trigger"
    mon_msg = ch_mon.parse_inbound(ch_mon.enqueued[-1])
    assert mon_msg.metadata["trigger_reason"] == "monitor"
    assert mon_msg.metadata["bot_mentioned"] is True
    assert mon_msg.metadata["monitor"] is True
    assert "闲聊0" in mon_msg.text and "〔巡检跳过〕" in mon_msg.text
    ch_mon._monitor_sweep()
    assert len(ch_mon.enqueued) == real_enqueued + 1, "empty buffer must not re-trigger"

    cfg_gap = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "monitor_enabled": "true",
            "monitor_min_messages": "1",
            "monitor_min_trigger_gap_seconds": "3600",
        }
    )
    ch_gap = plugin.OneBot11Channel(lambda _m: None, config=cfg_gap, channel_id="ch-gap", tenant_id="a")
    asyncio.run(
        ch_gap._handle_message_event(
            {
                "post_type": "message", "message_type": "group", "message_id": 32000,
                "group_id": 20002, "user_id": 32001, "self_id": 10000,
                "message": [{"type": "text", "data": {"text": "x"}}],
                "sender": {"user_id": 32001, "nickname": "g"},
            }
        )
    )
    ch_gap._monitor_last["20002"] = time.monotonic()  # pretend it just fired
    gap_base = len(ch_gap.enqueued)
    ch_gap._monitor_sweep()
    assert len(ch_gap.enqueued) == gap_base, "gap throttle must suppress a fresh group"

    cfg_mkw = plugin.OneBot11Config.from_dict(
        {
            "ws_url": "ws://127.0.0.1:3001",
            "monitor_enabled": "true",
            "monitor_min_messages": "1",
            "monitor_keywords": "求助",
            "monitor_min_trigger_gap_seconds": "0",
        }
    )
    ch_mkw = plugin.OneBot11Channel(lambda _m: None, config=cfg_mkw, channel_id="ch-mkw", tenant_id="a")
    asyncio.run(
        ch_mkw._handle_message_event(
            {
                "post_type": "message", "message_type": "group", "message_id": 33000,
                "group_id": 20002, "user_id": 33001, "self_id": 10000,
                "message": [{"type": "text", "data": {"text": "今天天气不错"}}],
                "sender": {"user_id": 33001, "nickname": "g"},
            }
        )
    )
    ch_mkw._monitor_sweep()
    mkw_base = len(ch_mkw.enqueued)
    assert mkw_base >= 1, "raw event itself is enqueued"
    assert not any(
        ch_mkw.parse_inbound(item).metadata.get("monitor") for item in ch_mkw.enqueued
    ), "keyword filter must suppress a non-matching digest"
    asyncio.run(
        ch_mkw._handle_message_event(
            {
                "post_type": "message", "message_type": "group", "message_id": 33001,
                "group_id": 20002, "user_id": 33001, "self_id": 10000,
                "message": [{"type": "text", "data": {"text": "有人能帮忙吗，求助"}}],
                "sender": {"user_id": 33001, "nickname": "g"},
            }
        )
    )
    after_raw = len(ch_mkw.enqueued)
    assert after_raw == mkw_base + 1, "raw keyword event is enqueued too"
    ch_mkw._monitor_sweep()
    assert len(ch_mkw.enqueued) == after_raw + 1, "matching keyword must trigger the digest"
    assert ch_mkw.parse_inbound(ch_mkw.enqueued[-1]).metadata.get("monitor") is True

    # Sentinel: monitor skip replies are dropped instead of being sent.
    sent_batches.clear()
    ch_v4._call_api = capture_api
    mon_subject = plugin.ChannelSubject(
        subject_id="group:20002",
        chat_type="group",
        metadata={"group_id": "20002", "chat_type": "group", "monitor": True},
    )
    asyncio.run(ch_v4._send_text(mon_subject, "〔巡检跳过〕"))
    assert sent_batches == [], "monitor skip sentinel must be dropped"
    asyncio.run(ch_v4._send_text(mon_subject, "有一条值得注意：xxx"))
    assert len(sent_batches) == 1, "normal monitor replies must be delivered"

    # ------------------------------------------------------------------
    # v0.5.0: outbox persistence, reverse multi-account, patch API mode
    # ------------------------------------------------------------------

    # Persistence: queued messages survive a channel restart.
    cfg_persist = plugin.OneBot11Config.from_dict(
        {"ws_url": "ws://127.0.0.1:3001", "outbox_persist_dir": state_dir}
    )
    ch_p1 = plugin.OneBot11Channel(lambda _m: None, config=cfg_persist, channel_id="ch-persist", tenant_id="a")
    ch_p1._connected_transport = False
    asyncio.run(
        ch_p1.push_text(
            plugin.ChannelSubject(
                subject_id="group:555", chat_type="group",
                metadata={"group_id": "555", "chat_type": "group", "strict_push": True},
            ),
            "重启后补发",
        )
    )
    assert len(ch_p1._outbox) == 1
    state_file = os.path.join(state_dir, "ch-persist.json")
    assert os.path.isfile(state_file), "outbox state must be written on enqueue"

    ch_p2 = plugin.OneBot11Channel(lambda _m: None, config=cfg_persist, channel_id="ch-persist", tenant_id="a")
    assert len(ch_p2._outbox) == 1, "state must be restored on construction"
    assert ch_p2._outbox[0].params.get("group_id") == 555
    delivered2 = []

    async def deliver_api(action, params, *, timeout=None, account=""):
        delivered2.append((action, params, account))
        return {"message_id": 1}

    ch_p2._call_api = deliver_api
    ch_p2._connected_transport = True
    ch_p2._login_confirmed = True
    asyncio.run(ch_p2._flush_outbox())
    assert delivered2 and delivered2[0][0] == "send_group_msg"
    assert not ch_p2._outbox
    with open(state_file, encoding="utf-8") as handle:
        assert json.load(handle)["items"] == [], "drained state must be persisted as empty"

    # TTL survives restarts: an old queued_at epoch is treated as expired.
    with open(state_file, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "version": 1,
                "items": [
                    {
                        "action": "send_group_msg",
                        "params": {"group_id": 555, "message": []},
                        "queued_at": time.time() - 99999.0,
                        "attempts": 0,
                        "account": "",
                    }
                ],
            },
            handle,
        )
    ch_p3 = plugin.OneBot11Channel(lambda _m: None, config=cfg_persist, channel_id="ch-persist", tenant_id="a")
    assert len(ch_p3._outbox) == 1
    ch_p3._call_api = deliver_api
    ch_p3._connected_transport = True
    ch_p3._login_confirmed = True
    delivered2.clear()
    asyncio.run(ch_p3._flush_outbox())
    assert delivered2 == [] and not ch_p3._outbox, "expired item must be dropped after restore"

    # Corrupt state files are ignored, probe channels never persist.
    with open(state_file, "w", encoding="utf-8") as handle:
        handle.write("{not json")
    ch_p4 = plugin.OneBot11Channel(lambda _m: None, config=cfg_persist, channel_id="ch-persist", tenant_id="a")
    assert not ch_p4._outbox
    probe_channel = plugin.OneBot11Channel(lambda _m: None, config=cfg_persist, channel_id="__probe__", tenant_id="a")
    assert probe_channel._persist_path() == ""

    # Multi-account reverse: sessions coexist and route by self_id.
    cfg_multi = plugin.OneBot11Config.from_dict(
        {"connection_mode": "reverse", "listen_port": 3002, "multi_account": "true"}
    )
    ch_multi = plugin.OneBot11Channel(lambda _m: None, config=cfg_multi, channel_id="ch-multi", tenant_id="a")
    ch_multi._start_probe = lambda session: None  # keep offline tests task-free

    class _FakeWS:
        def __init__(self):
            self.closed = []

        async def close(self, code=None, reason=None):
            self.closed.append(code)

    ws_a, ws_b, ws_c = _FakeWS(), _FakeWS(), _FakeWS()
    s_a = asyncio.run(ch_multi._add_session(ws_a, "ra", self_id_hint="10000"))
    s_b = asyncio.run(ch_multi._add_session(ws_b, "rb", self_id_hint="20000"))
    assert len(ch_multi._sessions) == 2
    assert ch_multi._select_session("10000") is s_a
    assert ch_multi._select_session("20000") is s_b
    assert ch_multi._select_session() is s_a, "primary = first attached session"
    assert ch_multi._connected_transport is True
    assert ch_multi.is_connected is False
    s_b.login_confirmed = True
    ch_multi._refresh_state()
    assert ch_multi.is_connected is True
    assert ch_multi._ws is ws_a, "aggregate view follows the primary session"

    s_c = asyncio.run(ch_multi._add_session(ws_c, "rc", self_id_hint="10000"))
    assert ws_a.closed, "same-account redial must retire the older connection"
    assert ch_multi._select_session("10000") is s_c
    assert len(ch_multi._sessions) == 2

    routed = {}

    async def route_api(action, params, *, timeout=None, account=""):
        routed["account"] = account
        return {}

    ch_multi._call_api = route_api
    asyncio.run(
        ch_multi.push_segments(
            plugin.build_group_subject("20002", "20000"),
            [{"type": "text", "data": {"text": "hi"}}],
        )
    )
    assert routed["account"] == "20000", "push must carry the account hint"
    try:
        asyncio.run(
            ch_multi.push_segments(
                plugin.build_group_subject("20002", "99999"),
                [{"type": "text", "data": {"text": "hi"}}],
            )
        )
    except ConnectionError as exc:
        assert "99999" in str(exc), exc
    else:
        raise AssertionError("push to an unknown account must be rejected")

    passive = {}

    async def passive_api(action, params, *, timeout=None, account=""):
        passive["account"] = account
        return {}

    ch_multi._call_api = passive_api
    gone = plugin.ChannelSubject(
        subject_id="group:20002",
        chat_type="group",
        metadata={"group_id": "20002", "self_id": "77777"},
    )
    asyncio.run(ch_multi._send_segments(gone, [{"type": "text", "data": {"text": "x"}}], allow_quote=False))
    assert passive["account"] == "", "passive replies fall back to the primary session"

    cfg_single_rev = plugin.OneBot11Config.from_dict(
        {"connection_mode": "reverse", "listen_port": 3003}
    )
    ch_single_rev = plugin.OneBot11Channel(lambda _m: None, config=cfg_single_rev, channel_id="ch-single-rev", tenant_id="a")
    ch_single_rev._start_probe = lambda session: None
    ws_x, ws_y = _FakeWS(), _FakeWS()
    asyncio.run(ch_single_rev._add_session(ws_x, "rx", self_id_hint="10000"))
    asyncio.run(ch_single_rev._add_session(ws_y, "ry", self_id_hint="20000"))
    assert len(ch_single_rev._sessions) == 1 and ws_x.closed, "single mode must keep replace semantics"

    # patch_channel.py --api-url: the write goes through the hot-effect PATCH.
    api_seen = []

    class _PatchHandler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_PATCH(self):  # noqa: N802 - http.server naming
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            api_seen.append((self.path, self.headers.get("Authorization"), json.loads(body or b"{}")))
            payload = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    apid = ThreadingHTTPServer(("127.0.0.1", 0), _PatchHandler)
    threading.Thread(target=apid.serve_forever, daemon=True).start()
    base_file = Path(state_dir) / "base.json"
    base_file.write_text(
        json.dumps({"ws_url": "ws://x", "token": "real", "group_reply_mode": "mention"}),
        encoding="utf-8",
    )
    script = Path(__file__).resolve().parents[1] / "scripts" / "patch_channel.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--api-url",
            f"http://127.0.0.1:{apid.server_address[1]}",
            "--api-token",
            "jwt-tok",
            "--agent",
            "ag",
            "--channel",
            "ch",
            "--base-file",
            str(base_file),
            "--set",
            "group_reply_mode=always",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    apid.shutdown()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    api_path, api_auth, api_body = api_seen[0]
    assert api_path == "/api/v1/agents/ag/channels/ch"
    assert api_auth == "Bearer jwt-tok"
    assert api_body["config"]["group_reply_mode"] == "always"
    assert api_body["config"]["token"] == "real", "merged config must keep base secrets"
    assert "applied" in proc.stdout, proc.stdout

    print("offline checks passed")


if __name__ == "__main__":
    main()
