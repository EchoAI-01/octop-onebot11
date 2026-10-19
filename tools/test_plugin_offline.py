"""Offline unit checks for the OneBot 11 plugin.

The real Octop runtime is not required: lightweight stand-ins for the
``octop_gateway`` contracts are injected before importing ``main.py``.  This
keeps the wire-format logic (event parsing, CQ fallback, chunking, routing)
testable on a workstation without Octop or a running NapCat.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path

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
    assert len(channel_kw.enqueued) == before, "private keyword mode must drop non-keyword messages"
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
    sent = {}

    async def fake_push(subject, text):
        sent["subject"] = subject
        sent["text"] = text

    channel.push_text = fake_push
    result = asyncio.run(tools_mod.onebot11_send_group("20002", "群通知"))
    assert result.startswith("已发送到群"), result
    assert sent["subject"].chat_type == "group"
    assert sent["subject"].metadata["group_id"] == "20002"
    assert sent["text"] == "群通知"

    channel._connected_transport = False
    result = asyncio.run(tools_mod.onebot11_send_group("20002", "x"))
    assert result.startswith("发送失败"), result
    channel._connected_transport = True

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

    print("offline checks passed")


if __name__ == "__main__":
    main()
