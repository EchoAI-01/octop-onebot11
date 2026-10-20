"""Octop channel plugin: OneBot 11 WebSocket (NapCat compatible).

Two transport modes share the same protocol and routing code:

* ``forward`` (default): Octop dials NapCat's WebSocket server
  (``ws_url``, e.g. ``ws://maim-bot-napcat:3001``).
* ``reverse``: Octop listens on ``listen_host:listen_port`` and NapCat dials
  in (OneBot 11 reverse-WS).  Useful when NapCat cannot expose a port or sits
  behind NAT.

Design goals
------------
* Octop must boot and persist this channel even when NapCat is offline or the
  QQ account is not logged in.  ``start()`` therefore spawns the connection
  supervisor and returns; connection failures only change ``is_connected`` /
  ``runtime_error``.
* One WebSocket connection carries both directions: inbound events arrive as
  JSON objects with ``post_type`` and outbound actions are JSON objects with
  ``action`` / ``params`` / ``echo``.  Responses are correlated by ``echo``.
* The channel is deliberately self-contained (single file, stdlib + the
  ``websockets`` package already shipped with Octop) so it can be installed as
  a regular ``kind: channel`` plugin without patching Octop core.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hmac
import json
import logging
import os
import random
import re
import time
import urllib.error
import urllib.request
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from octop_gateway.channel import BaseChannel, ChannelConfig, MessageProcessor
from octop_gateway.constraints import ChannelConstraints
from octop_gateway.group_context import GroupContextConfig
from octop_gateway.models import (
    AudioContent,
    ChannelSubject,
    ContentPart,
    FileContent,
    ImageContent,
    InboundMessage,
    TextContent,
    VideoContent,
)

logger = logging.getLogger(__name__)

# Live channel instances, keyed by Octop channel_id.  Used by the companion
# `octop-onebot11-tools` plugin so an agent can proactively target any group,
# not only groups that already have a session.
_LIVE_CHANNELS: dict[str, "OneBot11Channel"] = {}


def get_live_channel(channel_id: str = "") -> "OneBot11Channel | None":
    """Return a live channel by id, or the first connected one."""
    wanted = str(channel_id or "").strip()
    if wanted:
        return _LIVE_CHANNELS.get(wanted)
    for channel in list(_LIVE_CHANNELS.values()):
        if channel.is_connected:
            return channel
    return next(iter(_LIVE_CHANNELS.values()), None)


def build_group_subject(group_id: str, self_id: str = "") -> "ChannelSubject":
    """Build a proactive group subject from a bare group id."""
    gid = str(group_id or "").strip()
    account = str(self_id or "").strip()
    metadata = {
        "group_id": gid,
        "chat_type": "group",
        "message_type": "group",
        "strict_push": True,
    }
    if account:
        # Multi-account routing: send through this QQ's connection.
        metadata["self_id"] = account
    return ChannelSubject(subject_id=f"group:{gid}", chat_type="group", metadata=metadata)


def build_private_subject(user_id: str, self_id: str = "") -> "ChannelSubject":
    """Build a proactive private subject from a bare QQ user id."""
    uid = str(user_id or "").strip()
    account = str(self_id or "").strip()
    metadata = {
        "user_id": uid,
        "chat_type": "dm",
        "message_type": "private",
        "strict_push": True,
    }
    if account:
        metadata["self_id"] = account
    return ChannelSubject(subject_id=f"user:{uid}", chat_type="dm", metadata=metadata)

_CQ_RE = re.compile(r"\[CQ:([a-zA-Z0-9_]+)((?:,[^\]]*)?)\]")
_MAX_DEDUPE = 512
_MAX_MEDIA_BYTES = 10 * 1024 * 1024
_MONITOR_BUFFER_CAP = 50
_MONITOR_SKIP_TEXTS = frozenset({"〔巡检跳过〕", "巡检跳过"})


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("1", "true", "yes", "on", "y")


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _split_id_list(raw: Any) -> frozenset[str]:
    """Parse a comma-separated id list (or an iterable) into a set of strings."""
    if isinstance(raw, (list, tuple, set)):
        items = [str(item).strip() for item in raw]
    else:
        text = str(raw or "").replace("，", ",").replace("\n", ",")
        items = [item.strip() for item in text.split(",")]
    return frozenset(item for item in items if item)



def _normalize_path(value: Any) -> str:
    """Normalize a listen path; ``/`` means "accept any path"."""
    text = str(value or "").strip()
    if not text:
        return "/"
    if not text.startswith("/"):
        text = "/" + text
    return text


class OneBotAPIError(RuntimeError):
    """A OneBot action returned ``status=failed`` or a non-zero retcode."""

    def __init__(self, action: str, retcode: Any, message: str) -> None:
        self.action = action
        self.retcode = retcode
        self.message = message
        super().__init__(f"OneBot {action} failed (retcode={retcode}): {message}")


@dataclass
class _OutboxItem:
    """One queued proactive message waiting for a healthy connection."""

    action: str
    params: dict[str, Any]
    queued_at: float
    attempts: int = 0
    next_try: float = 0.0
    account: str = ""


@dataclass
class _ConnSession:
    """One live OneBot connection (reverse client or forward dial).

    Sessions exist only while their socket is attached; aggregate channel
    state (``_ws`` / ``_connected_transport`` / ``_login_confirmed`` /
    ``_self_id``) is derived from the session map so single-account
    behaviour stays identical to the pre-multi-account code.
    """

    key: str
    ws: Any
    remote: str = ""
    self_id: str = ""
    login_confirmed: bool = False
    probe_task: asyncio.Task[None] | None = None
    pending: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)


_OUTBOX_FLUSH_INTERVAL = 2.0


@dataclass
class OneBot11Config(ChannelConfig):
    """Configuration persisted in Octop's ``channels.config_json``.

    ``connection_mode`` selects the transport:

    * ``forward`` — ``ws_url`` is required; ``token`` is sent as
      ``Authorization: Bearer <token>`` to NapCat's WS server.
    * ``reverse`` — ``listen_host`` / ``listen_port`` / ``listen_path`` define
      the local WS endpoint; ``ws_url`` is ignored and NapCat connects in.
      ``token`` becomes the expected credential instead of the sent one.

    Every field is stored per channel, so two channels can run one QQ account
    in forward mode and another in reverse mode side by side.
    """

    connection_mode: str = "forward"  # forward | reverse
    ws_url: str = ""
    token: str = ""
    listen_host: str = "0.0.0.0"
    listen_port: int = 3002
    listen_path: str = "/onebot/v11/ws"
    self_id: str = ""
    group_reply_mode: str = "mention"  # mention | always | off
    private_reply_mode: str = "always"  # always | keyword | off
    trigger_keywords: str = ""
    keyword_case_sensitive: bool = False
    group_history_limit: int = 10
    group_history_ttl_seconds: float = 300.0
    reply_with_quote: bool = False
    ignore_self_message: bool = True
    connect_timeout_seconds: float = 8.0
    request_timeout_seconds: float = 15.0
    startup_wait_seconds: float = 2.0
    reconnect_min_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    max_text_chars: int = 1500
    # Outbound policy: empty whitelist entries mean "no restriction".
    allowed_groups: str = ""
    allowed_users: str = ""
    send_rate_limit_count: int = 20
    send_rate_limit_window: float = 60.0
    # In-memory outbox for proactive (strict) sends while disconnected.
    outbox_enabled: bool = True
    outbox_max_items: int = 100
    outbox_ttl_seconds: float = 3600.0
    outbox_max_attempts: int = 3
    outbox_retry_min_seconds: float = 2.0
    outbox_retry_max_seconds: float = 60.0
    # Outbox persistence: survive Octop restarts (file-based, per channel).
    outbox_persist: bool = True
    outbox_persist_dir: str = ""  # empty -> ~/.octop/onebot11/state
    # Reverse mode: accept multiple concurrent NapCat accounts (routing by self_id).
    multi_account: bool = False
    # Inbound sender policy: empty means no restriction; blacklist wins.
    allowed_senders: str = ""
    blocked_senders: str = ""
    # Event handling.
    poke_triggers_reply: bool = False
    auto_accept_friend_request: bool = False
    auto_accept_group_invite: bool = False
    resolve_forward: bool = False
    # OneBot HTTP API fallback for outbound actions while WS is down.
    http_api_url: str = ""
    http_api_token: str = ""
    http_api_timeout_seconds: float = 10.0
    # Group monitor: periodic digest of non-triggering group messages.
    monitor_enabled: bool = False
    monitor_interval_seconds: float = 600.0
    monitor_min_messages: int = 5
    monitor_keywords: str = ""
    monitor_min_trigger_gap_seconds: float = 1800.0
    monitor_max_per_hour: int = 4
    group_context: GroupContextConfig = field(
        default_factory=lambda: GroupContextConfig(
            enabled=True,
            activation="mention",
            history="recent",
            history_limit=10,
        )
    )

    # Used by ChannelManager before start(); must not contain login state.
    required_credentials = ("ws_url",)
    # Accept the older/documented spelling while persisting the canonical key.
    field_aliases = {"access_token": "token"}

    def __post_init__(self) -> None:
        # Dashboard form values may arrive as strings; normalize booleans once.
        self.reply_with_quote = _to_bool(self.reply_with_quote)
        self.ignore_self_message = _to_bool(self.ignore_self_message)
        self.keyword_case_sensitive = _to_bool(self.keyword_case_sensitive)
        self.outbox_enabled = _to_bool(self.outbox_enabled)
        self.outbox_max_items = max(0, _to_int(self.outbox_max_items, 100))
        self.outbox_ttl_seconds = max(0.0, _to_float(self.outbox_ttl_seconds, 3600.0))
        self.outbox_max_attempts = max(1, _to_int(self.outbox_max_attempts, 3))
        self.outbox_retry_min_seconds = max(0.5, _to_float(self.outbox_retry_min_seconds, 2.0))
        self.outbox_retry_max_seconds = max(
            self.outbox_retry_min_seconds, _to_float(self.outbox_retry_max_seconds, 60.0)
        )
        self.outbox_persist = _to_bool(self.outbox_persist)
        self.outbox_persist_dir = str(self.outbox_persist_dir or "").strip()
        self.multi_account = _to_bool(self.multi_account)
        self.allowed_senders = str(self.allowed_senders or "").strip()
        self.blocked_senders = str(self.blocked_senders or "").strip()
        self.poke_triggers_reply = _to_bool(self.poke_triggers_reply)
        self.auto_accept_friend_request = _to_bool(self.auto_accept_friend_request)
        self.auto_accept_group_invite = _to_bool(self.auto_accept_group_invite)
        self.resolve_forward = _to_bool(self.resolve_forward)
        self.http_api_url = str(self.http_api_url or "").strip()
        self.http_api_token = str(self.http_api_token or "").strip()
        self.http_api_timeout_seconds = max(1.0, _to_float(self.http_api_timeout_seconds, 10.0))
        self.monitor_enabled = _to_bool(self.monitor_enabled)
        self.monitor_interval_seconds = max(30.0, _to_float(self.monitor_interval_seconds, 600.0))
        self.monitor_min_messages = max(1, _to_int(self.monitor_min_messages, 5))
        self.monitor_min_trigger_gap_seconds = max(0.0, _to_float(self.monitor_min_trigger_gap_seconds, 1800.0))
        self.monitor_max_per_hour = max(1, _to_int(self.monitor_max_per_hour, 4))
        self.send_rate_limit_count = max(0, _to_int(self.send_rate_limit_count, 20))
        self.send_rate_limit_window = max(1.0, _to_float(self.send_rate_limit_window, 60.0))
        self.group_context.history_limit = max(0, int(self.group_history_limit))
        self.group_context.history_ttl_seconds = max(0.0, float(self.group_history_ttl_seconds))
        self.connection_mode = self.normalized_mode()
        self.listen_port = _to_int(self.listen_port, 3002)
        self.listen_path = _normalize_path(self.listen_path)
        self.listen_host = str(self.listen_host or "0.0.0.0").strip() or "0.0.0.0"

    def normalized_mode(self) -> str:
        """Return ``"forward"`` or ``"reverse"``; accept common spellings."""
        raw = str(self.connection_mode or "forward").strip().lower()
        if raw in ("reverse", "reverse_ws", "reverse-ws", "server", "listen", "listen_only"):
            return "reverse"
        return "forward"

    @property
    def is_reverse(self) -> bool:
        return self.normalized_mode() == "reverse"

    def missing_credentials(self) -> list[str]:
        """Reverse mode needs no outbound URL; forward mode still needs one."""
        if self.is_reverse:
            return [] if 0 < int(self.listen_port) < 65536 else ["listen_port"]
        return [name for name in self.required_credentials if not getattr(self, name, None)]


class OneBot11Channel(BaseChannel):
    """OneBot 11 channel (forward client or reverse server).

    In ``forward`` mode Octop dials NapCat.  In ``reverse`` mode Octop runs a
    WebSocket server and NapCat dials in; both modes use the same inbound
    dispatch, action/echo correlation and outbound routing.  The channel is
    considered "connected" only after the transport is open *and* a
    ``get_login_info`` / heartbeat confirms a logged-in QQ account.  Until
    then it stays registered, reports a degraded status, and keeps retrying.
    """

    channel_type = "onebot11"

    def __init__(
        self,
        processor: MessageProcessor,
        *,
        config: OneBot11Config,
        channel_id: str | None = None,
        tenant_id: str | None = None,
        debounce_seconds: float = 0.0,
        constraints: ChannelConstraints | None = None,
    ) -> None:
        # The real BaseChannel.__init__() resolves constraints (calling
        # self._default_constraints()) before returning, so _config must exist
        # before super().__init__(); assign again after in case it wraps it.
        self._config = config
        super().__init__(
            processor,
            channel_id=channel_id,
            tenant_id=tenant_id,
            debounce_seconds=debounce_seconds,
            constraints=constraints,
            config=config,
        )
        self._config = config
        self._keywords = self._parse_keywords(config.trigger_keywords)
        self._allowed_groups = _split_id_list(config.allowed_groups)
        self._allowed_users = _split_id_list(config.allowed_users)
        self._allowed_senders = _split_id_list(config.allowed_senders)
        self._blocked_senders = _split_id_list(config.blocked_senders)
        self._monitor_keywords = self._parse_keywords(config.monitor_keywords)
        self._ws: Any = None
        self._task: asyncio.Task[None] | None = None
        self._receiver_task: asyncio.Task[None] | None = None
        self._outbox_task: asyncio.Task[None] | None = None
        self._server: Any = None
        self._session_tasks: set[asyncio.Task[None]] = set()
        self._stopping = False
        self._connected_transport = False
        self._login_confirmed = False
        self._self_id: str = str(config.self_id or "").strip()
        self._runtime_error: str | None = "未启动"
        self._dedupe: OrderedDict[str, float] = OrderedDict()
        self._outbox: deque[_OutboxItem] = deque()
        self._sessions: dict[str, _ConnSession] = {}
        self._monitor_buffer: dict[str, deque[tuple[str, str, str, float]]] = {}
        self._monitor_last: dict[str, float] = {}
        self._monitor_triggers: deque[float] = deque()
        self._monitor_task: asyncio.Task[None] | None = None
        self._first_attempt: asyncio.Event | None = None
        self._backoff = max(0.5, float(config.reconnect_min_seconds))
        if self.channel_id != "__probe__":
            _LIVE_CHANNELS[self.channel_id] = self

        if config.group_reply_mode == "always":
            config.group_context.enabled = True
            config.group_context.activation = "always"
        elif config.group_reply_mode == "off":
            config.group_context.enabled = False
        self._load_outbox()

    # ------------------------------------------------------------------
    # Runtime status (read by Octop's Gateway.get_runtime_status)
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        """True only when the socket is open and a QQ account is logged in."""
        return bool(self._connected_transport and self._login_confirmed)

    @property
    def runtime_error(self) -> str | None:
        return self._runtime_error

    def _parse_keywords(self, raw: Any) -> tuple[str, ...]:
        if isinstance(raw, (list, tuple, set)):
            items = [str(item).strip() for item in raw]
        else:
            text = str(raw or "").replace("，", ",").replace("\n", ",")
            items = [item.strip() for item in text.split(",")]
        if self._config.keyword_case_sensitive:
            return tuple(item for item in items if item)
        return tuple(item.casefold() for item in items if item)

    def _keyword_hit(self, text: str) -> str | None:
        if not self._keywords or not text:
            return None
        haystack = text if self._config.keyword_case_sensitive else text.casefold()
        for keyword in self._keywords:
            if keyword in haystack:
                return keyword
        return None

    def _raw_text(self, payload: dict[str, Any]) -> str:
        message = payload.get("message")
        if isinstance(message, str):
            return "".join(str(seg.get("data", {}).get("text") or "") for seg in self._parse_cq_string(message) if seg.get("type") == "text")
        if isinstance(message, list):
            parts = []
            for seg in message:
                if isinstance(seg, dict) and seg.get("type") == "text":
                    data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
                    parts.append(str(data.get("text") or ""))
            if parts:
                return "".join(parts)
        return str(payload.get("raw_message") or "")

    def _default_constraints(self) -> ChannelConstraints:
        # Defensive: may be invoked by BaseChannel.__init__ before _config lands.
        config = getattr(self, "_config", None)
        if config is not None:
            count = max(0, _to_int(config.send_rate_limit_count, 20))
            window = max(1.0, _to_float(config.send_rate_limit_window, 60.0))
        else:
            count, window = 20, 60.0
        return ChannelConstraints(
            reply_timeout=0.0,
            send_rate_limit=(count, window) if count > 0 else None,
            typing_keepalive_interval=0.0,
            show_thinking=False,
            show_tool_hints=False,
        )

    async def push_text(self, subject: ChannelSubject, text: str) -> None:
        """Proactive text send; strict mode queues or raises on real failures.

        Octop's internal push paths (cron / proactive care / team) keep the
        base best-effort semantics.  Tool-driven sends mark the subject with
        ``strict_push=True``: delivery failures then either land in the retry
        outbox (while disconnected, or on transient action errors) or raise so
        the agent knows the message was not accepted.
        """
        if not (subject.metadata or {}).get("strict_push"):
            return await super().push_text(subject, text)
        await self.push_segments(subject, [{"type": "text", "data": {"text": text}}])

    async def push_segments(self, subject: ChannelSubject, segments: list[dict[str, Any]]) -> None:
        """Strict proactive send of mixed OneBot segments (at / image / text).

        Long text is split into multiple messages; non-text segments ride on
        the first one.  Local file paths in media segments are inlined as
        base64.  When the transport is down (or fails mid-batch) the remaining
        messages are parked in the outbox instead of raising, unless the
        outbox is disabled or full.
        """
        denial = self._send_denial(subject)
        if denial:
            raise PermissionError(denial)
        account = str((subject.metadata or {}).get("self_id") or "").strip()
        if account and self._select_session(account) is None:
            raise ConnectionError(f"OneBot 11 账号 {account} 未连接")
        cleaned = [seg for seg in segments if isinstance(seg, dict)]
        cleaned = [self._materialize_media_segment(seg) for seg in cleaned]
        cleaned = self._reply_segments_first(cleaned)
        chunks = self._compose_push_chunks(cleaned)
        if not chunks:
            return
        resolved = self.resolve_push_subject(subject)
        prepared: list[tuple[str, dict[str, Any], str]] = []
        for chunk in chunks:
            action, params = self._build_send_action(resolved, chunk)
            prepared.append((action, {**params, "message": chunk}, account))

        if self._connected_transport or self._http_available():
            await self._acquire_rate_slot()
            for index, (action, params, item_account) in enumerate(prepared):
                try:
                    await self._call_api(action, params, account=item_account)
                except Exception as exc:  # noqa: BLE001 - queue the rest, re-raise only if parking fails
                    if self._enqueue_outbox(prepared[index:], cause=exc):
                        return
                    raise
            return
        if self._enqueue_outbox(prepared):
            return
        raise ConnectionError("OneBot 11 未连接（NapCat 未登录或 WebSocket 未就绪），且待发队列不可用")

    def _http_available(self) -> bool:
        return bool(str(self._config.http_api_url or "").strip())

    @staticmethod
    def _reply_segments_first(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """OneBot expects a ``reply`` segment to lead the message."""
        replies = [seg for seg in segments if str(seg.get("type") or "") == "reply"]
        if not replies:
            return segments
        rest = [seg for seg in segments if str(seg.get("type") or "") != "reply"]
        return [*replies, *rest]

    async def push_forward(self, subject: ChannelSubject, nodes: list[dict[str, Any]]) -> None:
        """Strict proactive merged-forward send (groups only)."""
        if not isinstance(nodes, list) or not nodes or not all(isinstance(node, dict) for node in nodes):
            raise ValueError("合并转发内容必须是非空节点数组")
        denial = self._send_denial(subject)
        if denial:
            raise PermissionError(denial)
        resolved = self.resolve_push_subject(subject)
        action, params = self._build_send_action(resolved, [])
        if action != "send_group_msg":
            raise ValueError("合并转发仅支持群聊")
        account = str((subject.metadata or {}).get("self_id") or "").strip()
        if account and self._select_session(account) is None:
            raise ConnectionError(f"OneBot 11 账号 {account} 未连接")
        if not self._connected_transport and not self._http_available():
            raise ConnectionError("OneBot 11 未连接（NapCat 未登录或 WebSocket 未就绪）")
        await self._acquire_rate_slot()
        await self._call_api("send_group_forward_msg", {**params, "messages": nodes}, account=account)

    def _compose_push_chunks(self, segments: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        """Split mixed segments into sendable messages (long text chunked)."""
        if not segments:
            return []
        media = [seg for seg in segments if str(seg.get("type") or "") != "text"]
        texts = [
            str((seg.get("data") or {}).get("text") or "")
            for seg in segments
            if str(seg.get("type") or "") == "text"
        ]
        text = "\n".join(part for part in texts if part)
        text_pieces = self._split_text(text) if text.strip() else []
        chunks: list[list[dict[str, Any]]] = []
        for piece in text_pieces:
            chunks.append([*media, {"type": "text", "data": {"text": piece}}])
            media = []
        if not chunks and media:
            chunks.append(list(media))
        return chunks

    def _materialize_media_segment(self, seg: dict[str, Any]) -> dict[str, Any]:
        """Inline a local file path in a media segment as base64 payload."""
        if str(seg.get("type") or "") not in ("image", "record", "video", "file"):
            return seg
        data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
        file_value = str(data.get("file") or "")
        if not file_value or file_value.startswith(("http://", "https://", "base64://")):
            return seg
        if not os.path.isfile(file_value):
            # Unknown scheme: pass through and let the OneBot server judge it.
            return seg
        size = os.path.getsize(file_value)
        if size > _MAX_MEDIA_BYTES:
            raise ValueError(f"媒体文件超过 {_MAX_MEDIA_BYTES} 字节上限: {file_value}")
        with open(file_value, "rb") as handle:
            encoded = base64.b64encode(handle.read()).decode("ascii")
        return {**seg, "data": {**data, "file": f"base64://{encoded}"}}

    def _send_denial(self, subject: ChannelSubject) -> str | None:
        """Return a human-readable denial reason, or None when allowed."""
        if not self._allowed_groups and not self._allowed_users:
            return None
        action, params = self._build_send_action(subject, [])
        target = str(params.get("group_id") or params.get("user_id") or "")
        if action == "send_group_msg" and self._allowed_groups and target not in self._allowed_groups:
            return f"目标群 {target} 不在 allowed_groups 白名单"
        if action == "send_private_msg" and self._allowed_users and target not in self._allowed_users:
            return f"目标用户 {target} 不在 allowed_users 白名单"
        return None

    # ------------------------------------------------------------------
    # Proactive outbox (in-memory; survives NapCat restarts, not Octop restarts)
    # ------------------------------------------------------------------

    def _enqueue_outbox(
        self,
        prepared: list[tuple[str, dict[str, Any], str]],
        cause: BaseException | None = None,
    ) -> bool:
        """Park prepared messages for retry; False means the caller must raise."""
        if not prepared or not self._config.outbox_enabled:
            return False
        limit = int(self._config.outbox_max_items)
        if len(self._outbox) + len(prepared) > limit:
            logger.warning(
                "OneBot11 outbox full (%d/%d), strict push rejected",
                len(self._outbox),
                limit,
            )
            return False
        now = time.monotonic()
        for action, params, account in prepared:
            self._outbox.append(
                _OutboxItem(
                    action=action,
                    params=params,
                    queued_at=now,
                    attempts=0,
                    next_try=now,
                    account=account,
                )
            )
        logger.info(
            "OneBot11 queued %d message(s) for retry (outbox=%d, cause=%s)",
            len(prepared),
            len(self._outbox),
            cause.__class__.__name__ if cause is not None else "disconnected",
        )
        self._persist_outbox()
        return True

    def _outbox_backoff(self, attempts: int) -> float:
        low = max(0.5, float(self._config.outbox_retry_min_seconds))
        high = max(low, float(self._config.outbox_retry_max_seconds))
        return min(high, low * (2 ** max(0, attempts - 1)))

    def _ensure_outbox_task(self) -> None:
        if self._outbox_task is None or self._outbox_task.done():
            self._outbox_task = asyncio.create_task(
                self._outbox_flush_loop(),
                name=f"onebot11-outbox-{self.channel_id}",
            )

    async def _outbox_flush_loop(self) -> None:
        while not self._stopping:
            try:
                if (self.is_connected or self._http_available()) and self._outbox:
                    await self._flush_outbox()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the loop must survive
                logger.exception("OneBot11 outbox flush failed")
            await asyncio.sleep(_OUTBOX_FLUSH_INTERVAL)

    async def _flush_outbox(self) -> None:
        """Deliver due items in order; drop expired / exhausted ones."""
        now = time.monotonic()
        ttl = max(0.0, float(self._config.outbox_ttl_seconds))
        changed = False
        while self._outbox:
            head = self._outbox[0]
            if ttl and now - head.queued_at > ttl:
                expired = self._outbox.popleft()
                changed = True
                logger.warning(
                    "OneBot11 outbox item expired (ttl=%ss): %s", ttl, expired.action
                )
                continue
            break
        sent = 0
        while self._outbox:
            item = self._outbox[0]
            if now < item.next_try:
                break
            if item.attempts >= int(self._config.outbox_max_attempts):
                exhausted = self._outbox.popleft()
                changed = True
                logger.warning(
                    "OneBot11 outbox item exceeded max attempts (%d): %s",
                    int(self._config.outbox_max_attempts),
                    exhausted.action,
                )
                continue
            try:
                await self._call_api(item.action, item.params, account=item.account)
            except OneBotAPIError as exc:
                item.attempts += 1
                item.next_try = now + self._outbox_backoff(item.attempts)
                self._outbox.popleft()
                self._outbox.append(item)
                changed = True
                logger.info(
                    "OneBot11 outbox retry #%d scheduled for %s: %s",
                    item.attempts,
                    item.action,
                    exc,
                )
                break
            except (ConnectionError, ConnectionClosed, OSError, TimeoutError, asyncio.TimeoutError):
                break
            self._outbox.popleft()
            sent += 1
            changed = True
        if sent:
            logger.info("OneBot11 outbox delivered %d message(s), %d remaining", sent, len(self._outbox))
        if changed:
            self._persist_outbox()

    # ------------------------------------------------------------------
    # Outbox persistence (file-based, per channel; survives Octop restarts)
    # ------------------------------------------------------------------

    def _persist_path(self) -> str:
        if not self._config.outbox_persist or self.channel_id == "__probe__":
            return ""
        base = self._config.outbox_persist_dir or os.path.join(
            os.path.expanduser("~"), ".octop", "onebot11", "state"
        )
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(self.channel_id)) or "channel"
        return os.path.join(base, f"{safe}.json")

    def _persist_outbox(self) -> None:
        """Atomically dump the outbox so a restart can drain it later."""
        path = self._persist_path()
        if not path:
            return
        if not self._outbox and not os.path.exists(path):
            return
        now_mono = time.monotonic()
        now_epoch = time.time()
        payload = {
            "version": 1,
            "items": [
                {
                    "action": item.action,
                    "params": item.params,
                    "queued_at": now_epoch - max(0.0, now_mono - item.queued_at),
                    "attempts": item.attempts,
                    "account": item.account,
                }
                for item in self._outbox
            ],
        }
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            os.replace(tmp, path)
        except (OSError, ValueError) as exc:
            logger.warning("OneBot11 outbox persist failed: %s", exc)

    def _load_outbox(self) -> None:
        """Restore queued messages written by a previous process."""
        path = self._persist_path()
        if not path or not self._config.outbox_enabled:
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            return
        except (OSError, ValueError):
            logger.warning("OneBot11 outbox state corrupt, ignoring: %s", path)
            return
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return
        now_mono = time.monotonic()
        now_epoch = time.time()
        limit = max(0, int(self._config.outbox_max_items))
        for raw in items[:limit]:
            if not isinstance(raw, dict):
                continue
            action = str(raw.get("action") or "")
            params = raw.get("params")
            if not action or not isinstance(params, dict):
                continue
            try:
                queued_epoch = float(raw.get("queued_at") or now_epoch)
            except (TypeError, ValueError):
                queued_epoch = now_epoch
            age = max(0.0, now_epoch - queued_epoch)
            self._outbox.append(
                _OutboxItem(
                    action=action,
                    params=params,
                    queued_at=now_mono - age,
                    attempts=max(0, _to_int(raw.get("attempts"), 0)),
                    next_try=now_mono,
                    account=str(raw.get("account") or ""),
                )
            )
        if self._outbox:
            logger.info(
                "OneBot11 restored %d outbox item(s) from %s", len(self._outbox), path
            )

    # ------------------------------------------------------------------
    # Group monitor (periodic digest of non-triggering messages)
    # ------------------------------------------------------------------

    def _ensure_monitor_task(self) -> None:
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(
                self._monitor_loop(),
                name=f"onebot11-monitor-{self.channel_id}",
            )

    async def _monitor_loop(self) -> None:
        interval = max(30.0, float(self._config.monitor_interval_seconds))
        while not self._stopping:
            await asyncio.sleep(interval)
            if self._stopping:
                return
            try:
                self._monitor_sweep()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the loop must survive
                logger.exception("OneBot11 monitor sweep failed")

    def _monitor_keyword_hit(self, text: str) -> bool:
        """True when the monitor keyword filter allows this text."""
        if not self._monitor_keywords:
            return True
        haystack = text if self._config.keyword_case_sensitive else text.casefold()
        return any(keyword in haystack for keyword in self._monitor_keywords)

    def _monitor_record(self, group_id: str, user_id: str, name: str, text: str) -> None:
        if not group_id or not text.strip():
            return
        buffer = self._monitor_buffer.setdefault(group_id, deque(maxlen=_MONITOR_BUFFER_CAP))
        buffer.append((user_id, name, text[:160], time.monotonic()))

    def _monitor_sweep(self) -> None:
        """Check buffers and synthesize one digest trigger per eligible group."""
        if self._stopping or not self._config.monitor_enabled:
            return
        if self._config.group_reply_mode == "off":
            return
        now = time.monotonic()
        while self._monitor_triggers and now - self._monitor_triggers[0] > 3600.0:
            self._monitor_triggers.popleft()
        if len(self._monitor_triggers) >= max(1, int(self._config.monitor_max_per_hour)):
            return
        horizon = max(60.0, 2.0 * float(self._config.monitor_interval_seconds))
        for group_id in list(self._monitor_buffer.keys()):
            buffer = self._monitor_buffer.get(group_id)
            if not buffer:
                continue
            while buffer and now - buffer[0][3] > horizon:
                buffer.popleft()
            if len(buffer) < max(1, int(self._config.monitor_min_messages)):
                continue
            if now - self._monitor_last.get(group_id, 0.0) < max(
                0.0, float(self._config.monitor_min_trigger_gap_seconds)
            ):
                continue
            if not any(self._monitor_keyword_hit(entry[2]) for entry in buffer):
                continue
            count = len(buffer)
            lines = [
                f"{name or user_id}（{user_id}）: {text}"
                for user_id, name, text, _ in list(buffer)
            ]
            buffer.clear()
            self._monitor_last[group_id] = now
            self._monitor_triggers.append(now)
            digest = (
                f"【群巡检】群 {group_id} 自上次检查以来有 {count} 条未触发回复的消息：\n"
                + "\n".join(lines[:20])
                + "\n请判断是否需要回应群友。若认为无需介入，请只回复：〔巡检跳过〕"
            )
            self.enqueue(self._synthetic_message("group", group_id, digest, "_octop_monitor"))
            logger.info(
                "OneBot11 monitor triggered for group %s (%d messages)", group_id, count
            )

    def _synthetic_message(
        self,
        message_type: str,
        key: str,
        text: str,
        marker: str,
        *,
        sender_name: str = "群巡检",
    ) -> dict[str, Any]:
        event: dict[str, Any] = {
            "post_type": "message",
            "message_type": message_type,
            "sub_type": "normal",
            "message_id": f"{marker}-{uuid.uuid4().hex}",
            "user_id": 0,
            "self_id": self._self_id or 0,
            "time": int(time.time()),
            "message": [{"type": "text", "data": {"text": text}}],
            "raw_message": "",
            "sender": {"user_id": 0, "nickname": sender_name, "card": sender_name},
            marker: True,
        }
        if message_type == "group":
            event["group_id"] = int(key) if str(key).isdigit() else key
        else:
            event["user_id"] = int(key) if str(key).isdigit() else key
        return event

    def _enrich_push_metadata(
        self,
        subject: ChannelSubject,
        meta: dict[str, Any],
    ) -> dict[str, Any]:
        """Backfill group routing for sparse proactive subjects.

        A caller may construct ``ChannelSubject(subject_id="group:123",
        chat_type="group")`` without metadata.  Persisted sessions may also
        carry a stale chat_type.  Normalize both before the base class builds
        the push subject.
        """
        subject_id = str(subject.subject_id or "")
        if subject_id.startswith("group:"):
            meta.setdefault("group_id", subject_id[6:])
            meta.setdefault("chat_type", "group")
            meta.setdefault("message_type", "group")
        elif subject.chat_type == "group":
            meta.setdefault("chat_type", "group")
            meta.setdefault("message_type", "group")
        return meta

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the supervisor without failing when NapCat is not ready."""
        if self._task is not None and not self._task.done():
            return

        reverse = self._config.is_reverse
        if reverse:
            port = _to_int(self._config.listen_port, 0)
            if not 0 < port < 65536:
                raise ValueError(f"OneBot11Channel: invalid listen_port: {port!r}")
        else:
            url = str(self._config.ws_url or "").strip()
            if not url:
                raise ValueError("OneBot11Channel: ws_url is required in forward mode")
            if not url.startswith(("ws://", "wss://")):
                raise ValueError(f"OneBot11Channel: unsupported ws_url scheme: {url!r}")

        self._stopping = False
        self._runtime_error = "启动反向服务" if reverse else "连接中"
        self._first_attempt = asyncio.Event()
        runner = self._serve_forever if reverse else self._run_forever
        self._task = asyncio.create_task(runner(), name=f"onebot11-{self.channel_id}")
        self._ensure_outbox_task()
        if self._config.monitor_enabled:
            self._ensure_monitor_task()

        # Give the first attempt a short window so the dashboard shows a useful
        # state right after creation.  Never propagate connection failures.
        try:
            await asyncio.wait_for(
                self._first_attempt.wait(),
                timeout=max(0.2, float(self._config.startup_wait_seconds)),
            )
        except (TimeoutError, asyncio.TimeoutError):
            pass

    async def stop(self) -> None:
        """Cancel the supervisor and close every session."""
        self._stopping = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        for probe_task in (self._receiver_task, self._outbox_task, self._monitor_task):
            if probe_task is not None and not probe_task.done():
                probe_task.cancel()
        self._receiver_task = None
        self._outbox_task = None
        self._monitor_task = None

        # Persist queued proactive messages so a restart can drain them later.
        self._persist_outbox()

        for session in list(self._sessions.values()):
            with contextlib.suppress(Exception):
                await self._remove_session(session, reason="channel stopped", close_code=1001)
        self._sessions.clear()
        self._refresh_state()

        server, self._server = self._server, None
        if server is not None:
            with contextlib.suppress(Exception):
                server.close()
            wait_closed = getattr(server, "wait_closed", None)
            if wait_closed is not None:
                with contextlib.suppress(Exception):
                    await wait_closed()

        self._connected_transport = False
        self._login_confirmed = False
        self._self_id = str(self._config.self_id or "").strip()
        self._runtime_error = "已停止"
        _LIVE_CHANNELS.pop(self.channel_id, None)
        logger.info("OneBot11Channel stopped (channel_id=%s)", self.channel_id)

    # ------------------------------------------------------------------
    # Connection supervisor
    # ------------------------------------------------------------------

    async def _run_forever(self) -> None:
        first = True
        try:
            while not self._stopping:
                try:
                    await self._connect_once()
                except asyncio.CancelledError:
                    raise
                except InvalidStatus as exc:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    self._runtime_error = f"websocket_http_{status or 'error'}"
                    logger.warning("OneBot11 WS rejected (HTTP %s): %s", status, self._config.ws_url)
                except (OSError, ConnectionClosed, TimeoutError, asyncio.TimeoutError) as exc:
                    self._runtime_error = f"connect_failed: {exc.__class__.__name__}: {exc}"
                    logger.info("OneBot11 WS unavailable: %s", exc)
                except Exception as exc:  # noqa: BLE001 - supervisor must survive
                    self._runtime_error = f"connect_failed: {exc}"
                    logger.exception("OneBot11 WS unexpected error")
                finally:
                    for stale in list(self._sessions.values()):
                        with contextlib.suppress(Exception):
                            await self._remove_session(stale, reason="supervisor cycle")
                    self._refresh_state()
                    if not self._stopping and self._runtime_error is None:
                        self._runtime_error = "连接已断开"
                    if first:
                        first = False
                        if self._first_attempt is not None:
                            self._first_attempt.set()

                if self._stopping:
                    break
                delay = min(
                    max(0.5, float(self._config.reconnect_max_seconds)),
                    self._backoff,
                ) + random.uniform(0, 0.5)
                self._backoff = min(
                    max(0.5, float(self._config.reconnect_max_seconds)),
                    max(1.0, self._backoff * 2),
                )
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.debug("OneBot11 supervisor cancelled")
            raise

    async def _connect_once(self) -> None:
        url = str(self._config.ws_url).strip()
        headers: dict[str, str] = {}
        token = str(self._config.token or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        kwargs: dict[str, Any] = {
            "open_timeout": max(1.0, float(self._config.connect_timeout_seconds)),
            "close_timeout": 5,
            "ping_interval": 20,
            "ping_timeout": 20,
            "max_size": 8 * 1024 * 1024,
        }
        if headers:
            kwargs["additional_headers"] = headers

        try:
            ws = await websockets.connect(url, **kwargs)
        except TypeError:
            # websockets < 14 used extra_headers instead of additional_headers.
            if headers:
                kwargs.pop("additional_headers", None)
                kwargs["extra_headers"] = headers
            ws = await websockets.connect(url, **kwargs)

        session = _ConnSession(
            key=uuid.uuid4().hex,
            ws=ws,
            remote=url,
            self_id=str(self._config.self_id or "").strip(),
        )
        self._sessions[session.key] = session
        self._runtime_error = "等待 NapCat 就绪"
        self._backoff = max(0.5, float(self._config.reconnect_min_seconds))
        self._refresh_state()
        logger.info("OneBot11 WS connected: %s (channel_id=%s)", url, self.channel_id)

        self._start_probe(session)
        self._receiver_task = asyncio.create_task(self._receiver(session), name="onebot11-recv")
        try:
            await self._receiver_task
        finally:
            if self._receiver_task is not None and not self._receiver_task.done():
                self._receiver_task.cancel()
            self._receiver_task = None
            with contextlib.suppress(Exception):
                await self._remove_session(session, reason="forward closed")
            with contextlib.suppress(Exception):
                await ws.close()

    # ------------------------------------------------------------------
    # Reverse mode: Octop listens, NapCat dials in
    # ------------------------------------------------------------------

    async def _serve_forever(self) -> None:
        """Bind the reverse-WS endpoint and keep it alive across errors."""
        host = str(self._config.listen_host or "0.0.0.0").strip() or "0.0.0.0"
        port = _to_int(self._config.listen_port, 3002)
        if self.channel_id == "__probe__":
            # A credential probe must not fight the live listener for the port.
            port = 0
        first = True
        try:
            while not self._stopping:
                try:
                    async with self._server_factory(host, port) as server:
                        self._server = server
                        self._backoff = max(0.5, float(self._config.reconnect_min_seconds))
                        bound = self._bound_port(server, port)
                        self._runtime_error = (
                            f"等待 NapCat 反向连接（监听 {host}:{bound}{self._config.listen_path}）"
                        )
                        logger.info(
                            "OneBot11 reverse WS listening on ws://%s:%s%s (channel_id=%s)",
                            host,
                            bound,
                            self._config.listen_path,
                            self.channel_id,
                        )
                        if not str(self._config.token or "").strip():
                            logger.warning(
                                "OneBot11 reverse WS accepts any client: token is empty "
                                "(channel_id=%s)",
                                self.channel_id,
                            )
                        if first:
                            first = False
                            if self._first_attempt is not None:
                                self._first_attempt.set()
                        while not self._stopping:
                            await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    raise
                except OSError as exc:
                    self._runtime_error = f"反向 WS 端口监听失败: {exc}"
                    logger.warning(
                        "OneBot11 reverse WS bind failed on %s:%s: %s",
                        host,
                        port,
                        exc,
                    )
                except Exception as exc:  # noqa: BLE001 - supervisor must survive
                    self._runtime_error = f"反向 WS 服务异常: {exc}"
                    logger.exception("OneBot11 reverse WS server failed")
                finally:
                    self._server = None
                    if first:
                        first = False
                        if self._first_attempt is not None:
                            self._first_attempt.set()

                if self._stopping:
                    break
                delay = min(
                    max(0.5, float(self._config.reconnect_max_seconds)),
                    max(1.0, self._backoff * 2),
                )
                self._backoff = delay
                await asyncio.sleep(delay + random.uniform(0, 0.5))
        except asyncio.CancelledError:
            logger.debug("OneBot11 reverse server cancelled")
            raise

    def _server_factory(self, host: str, port: int) -> Any:
        """Return a websockets server usable as an async context manager."""
        serve: Any = None
        try:  # websockets >= 13
            from websockets.asyncio.server import serve as _asyncio_serve

            serve = _asyncio_serve
        except Exception:  # noqa: BLE001 - older websockets exposes it at top level
            serve = websockets.serve
        return serve(
            self._handle_reverse_client,
            host,
            port,
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        )

    @staticmethod
    def _bound_port(server: Any, fallback: int) -> int:
        sockets = getattr(server, "sockets", None)
        if sockets:
            try:
                return int(sockets[0].getsockname()[1])
            except Exception:  # noqa: BLE001
                pass
        return fallback

    async def _handle_reverse_client(self, ws: Any, path: str | None = None) -> None:
        """Validate and serve one inbound NapCat connection."""
        remote = self._remote_label(ws)
        target = self._request_target(ws, path)
        path_only = target.split("?", 1)[0] or "/"
        expected = str(self._config.listen_path or "/")
        if expected not in ("", "/") and path_only.rstrip("/") != expected.rstrip("/"):
            logger.warning(
                "OneBot11 reverse WS rejected path %s from %s (expected %s)",
                path_only,
                remote,
                expected,
            )
            with contextlib.suppress(Exception):
                await ws.close(code=1008, reason="unexpected path")
            return
        if not self._reverse_authorized(ws, target):
            logger.warning("OneBot11 reverse WS rejected token from %s", remote)
            with contextlib.suppress(Exception):
                await ws.close(code=1008, reason="unauthorized")
            return

        header_self_id = self._header_value(ws, "X-Self-ID")
        if header_self_id and not self._self_id:
            self._self_id = header_self_id

        session = await self._add_session(ws, remote, self_id_hint=header_self_id)
        try:
            async for raw in ws:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "replace")
                try:
                    payload = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    logger.debug("OneBot11 ignored non-JSON frame: %.200r", raw)
                    continue
                if isinstance(payload, dict):
                    await self._dispatch_payload(payload, session)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed:
            logger.info("OneBot11 reverse WS closed by peer: %s", remote)
        except Exception as exc:  # noqa: BLE001
            self._runtime_error = f"receive_failed: {exc}"
            logger.exception("OneBot11 reverse receive loop failed")
        finally:
            await self._remove_session(session, reason="peer closed")

    # ------------------------------------------------------------------
    # Connection sessions (reverse multi-account + forward single)
    # ------------------------------------------------------------------

    def _primary_session(self) -> _ConnSession | None:
        """The session aggregate state is derived from (config self_id, else first)."""
        if not self._sessions:
            return None
        wanted = str(self._config.self_id or "").strip()
        if wanted:
            for session in self._sessions.values():
                if session.self_id == wanted:
                    return session
        return next(iter(self._sessions.values()))

    def _select_session(self, account: str = "") -> _ConnSession | None:
        """Strictly match a session by account (self_id); empty means primary."""
        account = str(account or "").strip()
        if not account:
            return self._primary_session()
        for session in self._sessions.values():
            if session.self_id == account:
                return session
        return None

    def _refresh_state(self) -> None:
        """Recompute the aggregate (legacy) channel view from the session map."""
        primary = self._primary_session()
        self._ws = primary.ws if primary is not None else None
        self._connected_transport = bool(self._sessions)
        self._login_confirmed = any(s.login_confirmed for s in self._sessions.values())
        if primary is not None and primary.self_id:
            self._self_id = primary.self_id

    def _register_session_identity(self, session: _ConnSession, self_id: Any) -> None:
        """Bind a discovered self_id to a session; retire duplicates per account."""
        self_id = str(self_id or "").strip()
        if not self_id or session.self_id == self_id:
            return
        session.self_id = self_id
        for other in list(self._sessions.values()):
            if other is not session and other.self_id == self_id:
                logger.info(
                    "OneBot11 replacing stale connection for self_id=%s", self_id
                )
                try:
                    task = asyncio.get_running_loop().create_task(
                        self._remove_session(other, reason="replaced", close_code=1000)
                    )
                except RuntimeError:  # no loop (offline tests): drop bookkeeping only
                    self._sessions.pop(other.key, None)
                else:
                    self._session_tasks.add(task)
                    task.add_done_callback(self._session_tasks.discard)
                break
        self._refresh_state()

    async def _add_session(
        self,
        ws: Any,
        remote: str,
        self_id_hint: str = "",
    ) -> _ConnSession:
        session = _ConnSession(
            key=uuid.uuid4().hex,
            ws=ws,
            remote=remote,
            self_id=str(self_id_hint or "").strip(),
        )
        if not self._config.multi_account:
            # Single-connection semantics: a new dial retires every previous one.
            for other in list(self._sessions.values()):
                await self._remove_session(
                    other, reason="replaced by a newer connection", close_code=1000
                )
        self._sessions[session.key] = session
        self._backoff = max(0.5, float(self._config.reconnect_min_seconds))
        self._runtime_error = "等待 NapCat 就绪"
        logger.info(
            "OneBot11 reverse WS client connected: %s (sessions=%d, self_id=%s, channel_id=%s)",
            remote,
            len(self._sessions),
            session.self_id or "?",
            self.channel_id,
        )
        self._start_probe(session)
        if session.self_id:
            # In multi mode a same-account redial must replace, not duplicate.
            for other in list(self._sessions.values()):
                if other is not session and other.self_id == session.self_id:
                    await self._remove_session(other, reason="replaced", close_code=1000)
                    break
        self._refresh_state()
        return session

    async def _remove_session(
        self,
        session: _ConnSession,
        *,
        reason: str = "closed",
        close_code: int | None = None,
    ) -> None:
        self._sessions.pop(session.key, None)
        if session.probe_task is not None and not session.probe_task.done():
            session.probe_task.cancel()
        session.probe_task = None
        for future in session.pending.values():
            if not future.done():
                future.set_exception(
                    ConnectionError(f"OneBot 11 socket closed ({reason})")
                )
        session.pending.clear()
        if close_code is not None:
            with contextlib.suppress(Exception):
                await session.ws.close(code=close_code, reason=reason)
        self._refresh_state()
        if not self._sessions and not self._stopping:
            self._runtime_error = "连接已断开，等待 NapCat 重连"
        logger.info(
            "OneBot11 session closed: %s (%s, sessions=%d)",
            session.remote,
            reason,
            len(self._sessions),
        )

    def _start_probe(self, session: _ConnSession) -> None:
        if session.probe_task is not None and not session.probe_task.done():
            session.probe_task.cancel()
        session.probe_task = asyncio.create_task(
            self._login_probe_loop(session),
            name="onebot11-login",
        )

    async def _attach_client(self, ws: Any, remote: str) -> _ConnSession:
        """Backward-compatible wrapper around ``_add_session`` (no hint)."""
        return await self._add_session(ws, remote)

    async def _detach_client(self, ws: Any, remote: str) -> None:
        """Backward-compatible wrapper: remove the session owning ``ws``."""
        for session in list(self._sessions.values()):
            if session.ws is ws:
                await self._remove_session(session, reason="peer closed")
                return
        logger.info("OneBot11 reverse WS stale client closed: %s", remote)

    def _reverse_authorized(self, ws: Any, target: str) -> bool:
        token = str(self._config.token or "").strip()
        if not token:
            return True
        header = self._header_value(ws, "Authorization")
        if header.lower().startswith("bearer "):
            if hmac.compare_digest(header[7:].strip(), token):
                return True
        if "?" in target:
            values = parse_qs(urlsplit(target).query).get("access_token") or []
            if values and hmac.compare_digest(str(values[0]), token):
                return True
        return False

    @staticmethod
    def _header_value(ws: Any, name: str) -> str:
        headers = getattr(ws, "request_headers", None)
        if headers is None:
            headers = getattr(getattr(ws, "request", None), "headers", None)
        if headers is None:
            return ""
        try:
            return str(headers.get(name) or "")
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _request_target(ws: Any, path: str | None) -> str:
        if path:
            return str(path)
        request = getattr(ws, "request", None)
        candidate = getattr(request, "path", None)
        if candidate:
            return str(candidate)
        return str(getattr(ws, "path", "") or "")

    @staticmethod
    def _remote_label(ws: Any) -> str:
        remote = getattr(ws, "remote_address", None)
        if isinstance(remote, tuple) and remote:
            return f"{remote[0]}:{remote[1]}" if len(remote) > 1 else str(remote[0])
        return str(remote or "unknown")

    # ------------------------------------------------------------------
    # Receive path
    # ------------------------------------------------------------------

    async def _receiver(self, session: _ConnSession) -> None:
        ws = session.ws
        try:
            async for raw in ws:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "replace")
                try:
                    payload = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    logger.debug("OneBot11 ignored non-JSON frame: %.200r", raw)
                    continue
                if isinstance(payload, dict):
                    await self._dispatch_payload(payload, session)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed:
            logger.info("OneBot11 WS closed by peer")
        except Exception as exc:  # noqa: BLE001
            self._runtime_error = f"receive_failed: {exc}"
            logger.exception("OneBot11 receive loop failed")

    async def _dispatch_payload(
        self,
        payload: dict[str, Any],
        session: _ConnSession | None = None,
    ) -> None:
        echo = payload.get("echo")
        if echo is not None:
            owner = session if session is not None else self._primary_session()
            pending = owner.pending if owner is not None else None
            if pending is not None:
                future = pending.pop(str(echo), None)
                if future is not None and not future.done():
                    future.set_result(payload)
            return

        post_type = payload.get("post_type")
        if post_type == "meta_event":
            self._handle_meta_event(payload, session)
            return
        if post_type == "message":
            await self._handle_message_event(payload, session)
            return
        if post_type in ("notice", "request"):
            await self._handle_notice_event(payload, session)
            return
        logger.debug("OneBot11 unhandled event: %s", post_type)

    def _handle_meta_event(
        self,
        payload: dict[str, Any],
        session: _ConnSession | None = None,
    ) -> None:
        meta_type = payload.get("meta_event_type")
        self_id = payload.get("self_id")
        target = session if session is not None else self._primary_session()
        if self_id:
            if target is not None:
                self._register_session_identity(target, self_id)
            elif not self._self_id:
                self._self_id = str(self_id)
        if meta_type == "heartbeat":
            status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
            online = status.get("online")
            if online is True and (self._self_id or (target and target.self_id)):
                if target is not None:
                    target.login_confirmed = True
                    self._refresh_state()
                else:
                    self._login_confirmed = True
                self._runtime_error = None
            elif online is False:
                if target is not None:
                    target.login_confirmed = False
                    target.self_id = ""
                    self._refresh_state()
                else:
                    self._login_confirmed = False
                    self._self_id = ""
                self._runtime_error = "NapCat 未登录，等待登录"
        elif meta_type == "lifecycle":
            sub_type = payload.get("sub_type")
            if sub_type in ("connect", "enable"):
                self._runtime_error = None if self._login_confirmed else "等待 NapCat 就绪"

    async def _handle_message_event(
        self,
        payload: dict[str, Any],
        session: _ConnSession | None = None,
    ) -> None:
        message_type = payload.get("message_type")
        if message_type not in ("private", "group"):
            return
        if message_type == "group" and self._config.group_reply_mode == "off":
            return
        if message_type == "private":
            if self._config.private_reply_mode == "off":
                return
            if self._config.private_reply_mode == "keyword" and not self._keyword_hit(self._raw_text(payload)):
                return

        user_id = str(payload.get("user_id") or "")
        self_id = str(payload.get("self_id") or self._self_id or "")
        target = session if session is not None else self._primary_session()
        if self_id and target is not None:
            self._register_session_identity(target, self_id)
        elif self_id and not self._self_id:
            self._self_id = self_id
        if self._config.ignore_self_message and self_id and user_id == self_id:
            return
        # Inbound sender policy: blacklist wins over the whitelist.
        if self._blocked_senders and user_id in self._blocked_senders:
            return
        if self._allowed_senders and user_id not in self._allowed_senders:
            return

        message_id = str(payload.get("message_id") or "")
        if message_id:
            now = time.monotonic()
            if message_id in self._dedupe:
                return
            self._dedupe[message_id] = now
            while len(self._dedupe) > _MAX_DEDUPE:
                self._dedupe.popitem(last=False)

        if self._config.resolve_forward and self._has_forward_segment(payload):
            await self._resolve_forward_payload(payload)
        if (
            self._config.monitor_enabled
            and message_type == "group"
            and self._config.group_reply_mode == "mention"
            and not self._event_triggers(payload)
        ):
            sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
            self._monitor_record(
                str(payload.get("group_id") or ""),
                user_id,
                str(sender.get("card") or sender.get("nickname") or user_id),
                self._raw_text(payload),
            )
        self._runtime_error = None if self._self_id else self._runtime_error
        if self._self_id:
            self._login_confirmed = True
        self.enqueue(payload)

    def _has_forward_segment(self, payload: dict[str, Any]) -> bool:
        message = payload.get("message")
        segments = message if isinstance(message, list) else []
        return any(isinstance(seg, dict) and seg.get("type") == "forward" for seg in segments)

    async def _resolve_forward_payload(self, payload: dict[str, Any]) -> None:
        """Fetch merged-forward content and attach it for parse_inbound."""
        message = payload.get("message")
        segments = message if isinstance(message, list) else []
        for seg in segments:
            if not (isinstance(seg, dict) and seg.get("type") == "forward"):
                continue
            data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
            forward_id = str(data.get("id") or "")
            if not forward_id:
                continue
            try:
                resolved = await self._call_api("get_forward_msg", {"id": forward_id}, timeout=10.0)
            except Exception as exc:  # noqa: BLE001 - resolution is best-effort
                logger.warning("OneBot11 forward resolve failed (id=%s): %s", forward_id, exc)
                return
            if isinstance(resolved, list):
                payload["_forward_messages"] = resolved
            return

    def _event_triggers(self, payload: dict[str, Any]) -> bool:
        """Cheap trigger check used by the monitor buffer (no full parse)."""
        message = payload.get("message")
        if isinstance(message, str):
            segments = self._parse_cq_string(message)
        elif isinstance(message, list):
            segments = [seg for seg in message if isinstance(seg, dict)]
        else:
            segments = []
        for seg in segments:
            if seg.get("type") == "at":
                data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
                qq = str(data.get("qq") or "")
                if self._self_id and qq == self._self_id:
                    return True
        return self._keyword_hit(self._raw_text(payload)) is not None

    async def _handle_notice_event(
        self,
        payload: dict[str, Any],
        session: _ConnSession | None = None,
    ) -> None:
        """Best-effort handling of notice / request events."""
        try:
            self_id = str(payload.get("self_id") or "")
            target = session if session is not None else self._primary_session()
            if self_id and target is not None:
                self._register_session_identity(target, self_id)
            elif self_id and not self._self_id:
                self._self_id = self_id
            if payload.get("post_type") == "request":
                await self._handle_request_event(payload)
                return
            notice_type = str(payload.get("notice_type") or "")
            sub_type = str(payload.get("sub_type") or "")
            if notice_type == "notify" and sub_type == "poke":
                target = str(payload.get("target_id") or "")
                if target and target == self._self_id and self._config.poke_triggers_reply:
                    self._enqueue_poke(payload)
                else:
                    logger.debug("OneBot11 poke not directed at bot: target=%s", target)
            elif notice_type in ("friend_recall", "group_recall"):
                logger.info(
                    "OneBot11 message recalled: %s message_id=%s",
                    notice_type,
                    payload.get("message_id"),
                )
            elif notice_type == "group_increase" and str(payload.get("user_id") or "") == self._self_id:
                logger.info("OneBot11 bot joined group %s", payload.get("group_id"))
            else:
                logger.debug(
                    "OneBot11 %s event: %s/%s",
                    payload.get("post_type"),
                    notice_type,
                    sub_type,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never kill the receiver loop
            logger.warning("OneBot11 notice handling failed: %s", exc)

    async def _handle_request_event(self, payload: dict[str, Any]) -> None:
        request_type = str(payload.get("request_type") or "")
        flag = str(payload.get("flag") or "")
        if request_type == "friend" and self._config.auto_accept_friend_request and flag:
            await self._call_api("set_friend_add_request", {"flag": flag, "approve": True})
            logger.info("OneBot11 auto-accepted friend request (flag=%s)", flag)
        elif request_type == "group" and self._config.auto_accept_group_invite and flag:
            sub_type = str(payload.get("sub_type") or "invite")
            await self._call_api(
                "set_group_add_request",
                {"flag": flag, "sub_type": sub_type, "approve": True},
            )
            logger.info("OneBot11 auto-accepted group request (flag=%s)", flag)
        else:
            logger.info("OneBot11 request event: %s flag=%s", request_type, flag)

    def _enqueue_poke(self, payload: dict[str, Any]) -> None:
        """Turn a poke-at-bot notice into a triggering synthetic message."""
        user_id = str(payload.get("user_id") or "")
        if not user_id or user_id == self._self_id:
            return
        group_id = str(payload.get("group_id") or "")
        if group_id:
            if self._config.group_reply_mode == "off":
                return
            message_type = "group"
        else:
            if self._config.private_reply_mode == "off":
                return
            message_type = "private"
        sender = payload.get("sender") if isinstance(payload.get("sender"), dict) else {}
        event: dict[str, Any] = {
            "post_type": "message",
            "message_type": message_type,
            "sub_type": "poke",
            "message_id": f"poke-{uuid.uuid4().hex}",
            "user_id": int(user_id) if user_id.isdigit() else user_id,
            "self_id": self._self_id or payload.get("self_id") or 0,
            "time": int(time.time()),
            "message": [{"type": "text", "data": {"text": "（戳了戳机器人）"}}],
            "raw_message": "",
            "sender": {"user_id": user_id, "nickname": str(sender.get("nickname") or "群友")},
            "_octop_poke": True,
        }
        if group_id:
            event["group_id"] = int(group_id) if group_id.isdigit() else group_id
        self.enqueue(event)
        logger.info("OneBot11 poke trigger enqueued (%s)", message_type)

    async def _login_probe_loop(self, session: _ConnSession) -> None:
        """Detect NapCat login state; do not require it for channel startup."""
        while not self._stopping and session.key in self._sessions:
            if not session.login_confirmed:
                try:
                    info = await self._api_via_session(session, "get_login_info", {}, timeout=5.0)
                    uid = str((info or {}).get("user_id") or "")
                    if uid and uid != "0":
                        session.login_confirmed = True
                        self._register_session_identity(session, uid)
                        self._refresh_state()
                        self._runtime_error = None
                        logger.info("OneBot11 login detected: self_id=%s", uid)
                    else:
                        self._runtime_error = "NapCat 未登录，等待登录"
                except OneBotAPIError as exc:
                    if exc.retcode in (1403, 1404):
                        self._runtime_error = "NapCat 未登录，等待登录"
                    else:
                        self._runtime_error = f"NapCat 登录查询失败: {exc.retcode}"
                except (TimeoutError, asyncio.TimeoutError):
                    self._runtime_error = "NapCat 未响应登录查询"
                except (ConnectionClosed, OSError) as exc:
                    self._runtime_error = f"NapCat 连接断开: {exc.__class__.__name__}"
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    self._runtime_error = f"NapCat 登录查询失败: {exc}"
            await asyncio.sleep(15.0)

    # ------------------------------------------------------------------
    # Outbound path
    # ------------------------------------------------------------------

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        if not self._connected_transport and not self._http_available():
            logger.warning("OneBot11 text send skipped: WebSocket is not connected")
            return
        if not text.strip():
            return
        if (subject.metadata or {}).get("monitor") and text.strip() in _MONITOR_SKIP_TEXTS:
            logger.info("OneBot11 monitor skip reply dropped")
            return
        segments: list[dict[str, Any]] = [{"type": "text", "data": {"text": chunk}} for chunk in self._split_text(text)]
        await self._send_segments(subject, segments, allow_quote=True)

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        for part in parts:
            if isinstance(part, TextContent):
                await self._send_text(subject, part.text)
            else:
                await self._send_media(subject, part)

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        try:
            data, mime = await self.load_media_bytes(media)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OneBot11 cannot load media: %s", exc)
            await self._send_text(subject, f"[{self._get_media_label(media)}]")
            return

        if not data:
            await self._send_text(subject, f"[{self._get_media_label(media)}]")
            return
        if len(data) > _MAX_MEDIA_BYTES:
            logger.warning("OneBot11 media too large (%d bytes), sending label only", len(data))
            await self._send_text(subject, f"[{self._get_media_label(media)}]")
            return

        b64 = base64.b64encode(data).decode("ascii")
        file_value = f"base64://{b64}"
        if isinstance(media, ImageContent):
            segment = {"type": "image", "data": {"file": file_value, "sub_type": 0}}
        elif isinstance(media, VideoContent):
            segment = {"type": "video", "data": {"file": file_value}}
        elif isinstance(media, AudioContent):
            segment = {"type": "record", "data": {"file": file_value}}
        elif isinstance(media, FileContent):
            name = str(media.filename or "file")
            if media.url and str(media.url).startswith(("http://", "https://")):
                segment = {"type": "file", "data": {"file": media.url, "name": name}}
            else:
                await self._send_text(subject, f"[文件: {name}]")
                return
        else:
            await self._send_text(subject, f"[{self._get_media_label(media)}]")
            return

        await self._send_segments(subject, [segment], allow_quote=False)

    async def _send_segments(
        self,
        subject: ChannelSubject,
        segments: list[dict[str, Any]],
        *,
        allow_quote: bool,
    ) -> None:
        if not segments:
            return
        denial = self._send_denial(subject)
        if denial:
            # Passive replies to whitelisted-out targets are dropped silently:
            # the agent already produced its answer, raising here has no reader.
            logger.warning("OneBot11 passive send blocked by policy: %s", denial)
            return
        action, params = self._build_send_action(subject, segments)
        account = str((subject.metadata or {}).get("self_id") or "").strip()
        if account and self._select_session(account) is None:
            # The inbound account's session is gone; fall back to the primary
            # connection (single-account legacy behaviour).
            account = ""
        quote_id = str(subject.metadata.get("message_id") or "") if allow_quote else ""
        if self._config.reply_with_quote and quote_id:
            quoted = [{"type": "reply", "data": {"id": quote_id}}, *segments]
            try:
                await self._call_api(action, {**params, "message": quoted}, account=account)
                return
            except OneBotAPIError as exc:
                logger.info("OneBot11 quote reply failed, retrying without quote: %s", exc)
        try:
            await self._call_api(action, {**params, "message": segments}, account=account)
        except OneBotAPIError as exc:
            logger.warning("OneBot11 send failed: %s", exc)
        except (ConnectionError, ConnectionClosed, OSError, TimeoutError, asyncio.TimeoutError) as exc:
            logger.warning("OneBot11 send skipped, socket unavailable: %s", exc)

    def _build_send_action(
        self,
        subject: ChannelSubject,
        segments: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        meta = subject.metadata or {}
        subject_id = str(subject.subject_id or "")
        # A proactive ChannelSubject may come from a persisted session whose
        # chat_type is stale ("dm"), while group_id / subject_id still identify
        # a group.  Check every signal before falling back to a private send.
        group_marker = meta.get("group_id")
        has_group_id = group_marker not in (None, "", 0, "0")
        is_group = bool(
            subject.chat_type == "group"
            or meta.get("chat_type") == "group"
            or meta.get("message_type") == "group"
            or meta.get("is_group") is True
            or has_group_id
            or subject_id.startswith("group:")
        )
        if is_group:
            group_id = meta.get("group_id")
            if not group_id:
                group_id = subject_id[6:] if subject_id.startswith("group:") else subject_id
            return "send_group_msg", {"group_id": self._normalize_id(group_id)}
        user_id = meta.get("user_id")
        if not user_id:
            user_id = subject_id[5:] if subject_id.startswith("user:") else subject_id
        return "send_private_msg", {"user_id": self._normalize_id(user_id)}

    async def _call_api(
        self,
        action: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,
        account: str = "",
    ) -> Any:
        """Send an action over the account's session (or the HTTP fallback)."""
        account = str(account or "").strip()
        session = self._select_session(account)
        if account and session is None:
            raise ConnectionError(f"OneBot 11 账号 {account} 未连接")
        if session is None:
            if self._http_available():
                return await self._call_api_http(action, params, timeout)
            raise ConnectionError("OneBot 11 WebSocket is not connected")
        return await self._api_via_session(session, action, params, timeout)

    async def _api_via_session(
        self,
        session: _ConnSession,
        action: str,
        params: dict[str, Any],
        timeout: float | None = None,
    ) -> Any:
        if session.key not in self._sessions:
            raise ConnectionError("OneBot 11 connection is closed")
        echo = f"octop-{uuid.uuid4().hex}"
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        session.pending[echo] = future
        try:
            await session.ws.send(
                json.dumps({"action": action, "params": params, "echo": echo}, ensure_ascii=False)
            )
            response = await asyncio.wait_for(
                future,
                timeout=float(timeout or self._config.request_timeout_seconds),
            )
        except (TimeoutError, asyncio.TimeoutError):
            session.pending.pop(echo, None)
            raise TimeoutError(f"OneBot action timed out: {action}") from None
        except Exception:
            session.pending.pop(echo, None)
            raise

        status = str(response.get("status") or "")
        retcode = response.get("retcode", 0)
        if status == "failed" or (retcode not in (0, None, "0")):
            message = str(response.get("wording") or response.get("message") or "unknown error")
            raise OneBotAPIError(action, retcode, message)
        return response.get("data")

    async def _call_api_http(
        self,
        action: str,
        params: dict[str, Any],
        timeout: float | None = None,
    ) -> Any:
        """OneBot 11 HTTP API fallback (``POST {http_api_url}/{action}``)."""
        base = str(self._config.http_api_url or "").strip().rstrip("/")
        token = str(self._config.http_api_token or "").strip() or str(self._config.token or "").strip()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = json.dumps(params, ensure_ascii=False).encode("utf-8")
        url = f"{base}/{action}"

        def _post() -> dict[str, Any]:
            request = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(  # noqa: S310 - operator-configured endpoint
                request,
                timeout=float(timeout or self._config.http_api_timeout_seconds),
            ) as response:
                return json.loads(response.read().decode("utf-8"))

        try:
            response = await asyncio.to_thread(_post)
        except (urllib.error.URLError, OSError, TimeoutError, asyncio.TimeoutError, ValueError) as exc:
            raise ConnectionError(
                f"OneBot HTTP API unavailable: {exc.__class__.__name__}: {exc}"
            ) from exc
        status = str(response.get("status") or "")
        retcode = response.get("retcode", 0)
        if status == "failed" or retcode not in (0, None, "0"):
            message = str(response.get("wording") or response.get("message") or "unknown error")
            raise OneBotAPIError(action, retcode, message)
        return response.get("data")

    def _split_text(self, text: str) -> list[str]:
        limit = max(200, int(self._config.max_text_chars))
        text = text.strip()
        if not text:
            return []
        chunks: list[str] = []
        while len(text) > limit:
            cut = text.rfind("\n", 0, limit)
            if cut < limit // 2:
                cut = limit
            chunks.append(text[:cut].strip())
            text = text[cut:].strip()
        if text:
            chunks.append(text)
        return chunks

    # ------------------------------------------------------------------
    # Inbound normalization
    # ------------------------------------------------------------------

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        if isinstance(raw_payload, InboundMessage):
            return raw_payload
        event: dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}

        message_type = str(event.get("message_type") or "private")
        is_group = message_type == "group"
        message_id = str(event.get("message_id") or "")
        user_id = str(event.get("user_id") or "")
        group_id = str(event.get("group_id") or "")
        self_id = str(event.get("self_id") or self._self_id or self._config.self_id or "")
        sender = event.get("sender") if isinstance(event.get("sender"), dict) else {}
        sender_name = str(sender.get("card") or sender.get("nickname") or user_id)

        content, bot_mentioned, reply_to, mentions = self._parse_message_content(event.get("message"), self_id)
        keyword_triggered = False
        trigger_reason = "at" if bot_mentioned else ""
        if event.get("_octop_monitor") or event.get("_octop_poke"):
            bot_mentioned = True
            trigger_reason = "monitor" if event.get("_octop_monitor") else "poke"
        elif not bot_mentioned and self._keywords:
            plain = "\n".join(part.text for part in content if isinstance(part, TextContent) and part.text)
            hit = self._keyword_hit(plain)
            if hit:
                bot_mentioned = True
                keyword_triggered = True
                trigger_reason = f"keyword:{hit}"
        forward_msgs = event.get("_forward_messages")
        if isinstance(forward_msgs, list) and forward_msgs:
            lines = []
            for item in forward_msgs:
                if not isinstance(item, dict):
                    continue
                fwd_sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}
                name = str(fwd_sender.get("card") or fwd_sender.get("nickname") or fwd_sender.get("user_id") or "?")
                segs = item.get("message") if isinstance(item.get("message"), list) else []
                fwd_text = "".join(
                    str((seg.get("data") or {}).get("text") or "")
                    for seg in segs
                    if isinstance(seg, dict) and seg.get("type") == "text"
                )
                if fwd_text:
                    lines.append(f"{name}: {fwd_text[:120]}")
            if lines:
                content.append(TextContent(text="[合并转发]\n" + "\n".join(lines[:20])))
        if not content:
            content = [TextContent(text="")]

        subject_id = f"group:{group_id}" if is_group else f"user:{user_id}"
        metadata = {
            "message_id": message_id,
            "message_type": message_type,
            # Octop's message_keys.chat_type_from_message() reads metadata, not
            # channel_subject.chat_type.  Without this key a group session is
            # persisted as "dm" and proactive pushes would go to send_private_msg.
            "chat_type": "group" if is_group else "dm",
            "is_group": is_group,
            "user_id": user_id,
            "group_id": group_id,
            "self_id": self_id,
            "sender_name": sender_name,
            "bot_mentioned": bot_mentioned,
            "keyword_triggered": keyword_triggered,
            "trigger_reason": trigger_reason,
            "reply_to": reply_to,
            "mentions": mentions,
            "monitor": bool(event.get("_octop_monitor")),
            "poke": bool(event.get("_octop_poke")),
            "raw_message": str(event.get("raw_message") or ""),
        }
        timestamp = event.get("time")
        try:
            ts = float(timestamp) if timestamp is not None else time.time()
        except (TypeError, ValueError):
            ts = time.time()

        return InboundMessage(
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            tenant_id=self.tenant_id,
            channel_subject=ChannelSubject(
                subject_id=subject_id,
                chat_type="group" if is_group else "direct",
                display_name=sender_name if not is_group else group_id,
                metadata=metadata,
                first_seen=ts,
                last_seen=ts,
            ),
            channel_session_id=f"onebot11:{self_id or 'unknown'}:{message_type}:{group_id or user_id}",
            content=content,
            metadata=metadata,
            timestamp=ts,
        )

    def _parse_message_content(
        self,
        message: Any,
        self_id: str,
    ) -> tuple[list[ContentPart], bool, str, list[dict[str, str]]]:
        parts: list[ContentPart] = []
        bot_mentioned = False
        reply_to = ""
        mentions: list[dict[str, str]] = []

        segments: list[dict[str, Any]]
        if isinstance(message, str):
            segments = self._parse_cq_string(message)
        elif isinstance(message, list):
            segments = [seg for seg in message if isinstance(seg, dict)]
        else:
            segments = []

        for seg in segments:
            seg_type = str(seg.get("type") or "")
            data = seg.get("data") if isinstance(seg.get("data"), dict) else {}
            if seg_type == "text":
                text = str(data.get("text") or "")
                if text:
                    parts.append(TextContent(text=text))
            elif seg_type == "at":
                qq = str(data.get("qq") or "")
                if self_id and qq == self_id:
                    bot_mentioned = True
                else:
                    name = str(data.get("name") or qq)
                    parts.append(TextContent(text=f"@{name} "))
                    mentions.append({"qq": qq, "name": name})
            elif seg_type == "reply":
                reply_to = str(data.get("id") or "")
            elif seg_type == "image":
                parts.append(self._image_part(data))
            elif seg_type == "record":
                url = self._http_url(data)
                if url:
                    parts.append(AudioContent(url=url))
                else:
                    parts.append(TextContent(text="[语音]"))
            elif seg_type == "video":
                url = self._http_url(data)
                if url:
                    parts.append(VideoContent(url=url))
                else:
                    parts.append(TextContent(text="[视频]"))
            elif seg_type == "file":
                name = str(data.get("name") or "file")
                url = self._http_url(data)
                if url:
                    parts.append(FileContent(url=url, filename=name))
                else:
                    parts.append(TextContent(text=f"[文件: {name}]"))
            elif seg_type == "face":
                parts.append(TextContent(text="[表情]"))
            elif seg_type == "forward":
                parts.append(TextContent(text="[合并转发]"))
            elif seg_type == "json":
                parts.append(TextContent(text="[卡片消息]"))
            elif seg_type == "xml":
                parts.append(TextContent(text="[XML 消息]"))
            else:
                logger.debug("OneBot11 unsupported segment type: %s", seg_type)

        return parts, bot_mentioned, reply_to, mentions

    def _image_part(self, data: dict[str, Any]) -> ContentPart:
        url = self._http_url(data)
        summary = str(data.get("summary") or "")
        if url:
            return ImageContent(url=url, alt_text=summary)
        file_value = str(data.get("file") or "")
        if file_value.startswith("base64://"):
            return ImageContent(data=file_value[len("base64://") :], alt_text=summary)
        return TextContent(text="[图片]")

    @staticmethod
    def _http_url(data: dict[str, Any]) -> str:
        for key in ("url", "file"):
            value = str(data.get(key) or "")
            if value.startswith(("http://", "https://")):
                return value
        return ""

    def _parse_cq_string(self, message: str) -> list[dict[str, Any]]:
        """Best-effort CQ-code parser for ``message_format=string`` setups."""
        segments: list[dict[str, Any]] = []
        cursor = 0
        for match in _CQ_RE.finditer(message):
            if match.start() > cursor:
                segments.append({"type": "text", "data": {"text": message[cursor : match.start()]}})
            seg_type = match.group(1)
            raw_params = match.group(2).lstrip(",")
            data: dict[str, str] = {}
            for item in raw_params.split(","):
                if not item or "=" not in item:
                    continue
                key, value = item.split("=", 1)
                data[key] = value.replace("&#44;", ",").replace("&#91;", "[").replace("&#93;", "]")
            segments.append({"type": seg_type, "data": data})
            cursor = match.end()
        if cursor < len(message):
            segments.append({"type": "text", "data": {"text": message[cursor:]}})
        return segments

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_id(value: Any) -> Any:
        text = str(value or "").strip()
        return int(text) if text.isdigit() else text


# QQ penguin icon (same asset the built-in "qq" channel kind uses in the
# dashboard): an inline SVG data URI, rendered by the dashboard as <img src>.
_QQ_ICON = "data:image/svg+xml,%3c?xml%20version='1.0'%20standalone='no'?%3e%3c!DOCTYPE%20svg%20PUBLIC%20'-//W3C//DTD%20SVG%201.1//EN'%20'http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd'%3e%3csvg%20t='1772609702986'%20class='icon'%20viewBox='0%200%201024%201024'%20version='1.1'%20xmlns='http://www.w3.org/2000/svg'%20p-id='5150'%20xmlns:xlink='http://www.w3.org/1999/xlink'%20width='64'%20height='64'%3e%3cpath%20d='M511.09761%20957.257c-80.159%200-153.737-25.019-201.11-62.386-24.057%206.702-54.831%2017.489-74.252%2030.864-16.617%2011.439-14.546%2023.106-11.55%2027.816%2013.15%2020.689%20225.583%2013.211%20286.912%206.767v-3.061z'%20fill='%23FAAD08'%20p-id='5151'%3e%3c/path%3e%3cpath%20d='M496.65061%20957.257c80.157%200%20153.737-25.019%20201.11-62.386%2024.057%206.702%2054.83%2017.489%2074.253%2030.864%2016.616%2011.439%2014.543%2023.106%2011.55%2027.816-13.15%2020.689-225.584%2013.211-286.914%206.767v-3.061z'%20fill='%23FAAD08'%20p-id='5152'%3e%3c/path%3e%3cpath%20d='M497.12861%20474.524c131.934-0.876%20237.669-25.783%20273.497-35.34%208.541-2.28%2013.11-6.364%2013.11-6.364%200.03-1.172%200.542-20.952%200.542-31.155C784.27761%20229.833%20701.12561%2057.173%20496.64061%2057.162%20292.15661%2057.173%20209.00061%20229.832%20209.00061%20401.665c0%2010.203%200.516%2029.983%200.547%2031.155%200%200%203.717%203.821%2010.529%205.67%2033.078%208.98%20140.803%2035.139%20276.08%2036.034h0.972z'%20fill='%23000000'%20p-id='5153'%3e%3c/path%3e%3cpath%20d='M860.28261%20619.782c-8.12-26.086-19.204-56.506-30.427-85.72%200%200-6.456-0.795-9.718%200.148-100.71%2029.205-222.773%2047.818-315.792%2046.695h-0.962C410.88561%20582.017%20289.65061%20563.617%20189.27961%20534.698%20185.44461%20533.595%20177.87261%20534.063%20177.87261%20534.063%20166.64961%20563.276%20155.56661%20593.696%20147.44761%20619.782%20108.72961%20744.168%20121.27261%20795.644%20130.82461%20796.798c20.496%202.474%2079.78-93.637%2079.78-93.637%200%2097.66%2088.324%20247.617%20290.576%20248.996a718.01%20718.01%200%200%201%205.367%200C708.80161%20950.778%20797.12261%20800.822%20797.12261%20703.162c0%200%2059.284%2096.111%2079.783%2093.637%209.55-1.154%2022.093-52.63-16.623-177.017'%20fill='%23000000'%20p-id='5154'%3e%3c/path%3e%3cpath%20d='M434.38261%20316.917c-27.9%201.24-51.745-30.106-53.24-69.956-1.518-39.877%2019.858-73.207%2047.764-74.454%2027.875-1.224%2051.703%2030.109%2053.218%2069.974%201.527%2039.877-19.853%2073.2-47.742%2074.436m206.67-69.956c-1.494%2039.85-25.34%2071.194-53.24%2069.956-27.888-1.238-49.269-34.559-47.742-74.435%201.513-39.868%2025.341-71.201%2053.216-69.974%2027.909%201.247%2049.285%2034.576%2047.767%2074.453'%20fill='%23FFFFFF'%20p-id='5155'%3e%3c/path%3e%3cpath%20d='M683.94261%20368.627c-7.323-17.609-81.062-37.227-172.353-37.227h-0.98c-91.29%200-165.031%2019.618-172.352%2037.227a6.244%206.244%200%200%200-0.535%202.505c0%201.269%200.393%202.414%201.006%203.386%206.168%209.765%2088.054%2058.018%20171.882%2058.018h0.98c83.827%200%20165.71-48.25%20171.881-58.016a6.352%206.352%200%200%200%201.002-3.395c0-0.897-0.2-1.736-0.531-2.498'%20fill='%23FAAD08'%20p-id='5156'%3e%3c/path%3e%3cpath%20d='M467.63161%20256.377c1.26%2015.886-7.377%2030-19.266%2031.542-11.907%201.544-22.569-10.083-23.836-25.978-1.243-15.895%207.381-30.008%2019.25-31.538%2011.927-1.549%2022.607%2010.088%2023.852%2025.974m73.097%207.935c2.533-4.118%2019.827-25.77%2055.62-17.886%209.401%202.07%2013.75%205.116%2014.668%206.316%201.355%201.77%201.726%204.29%200.352%207.684-2.722%206.725-8.338%206.542-11.454%205.226-2.01-0.85-26.94-15.889-49.905%206.553-1.579%201.545-4.405%202.074-7.085%200.242-2.678-1.834-3.786-5.553-2.196-8.135'%20fill='%23000000'%20p-id='5157'%3e%3c/path%3e%3cpath%20d='M504.33261%20584.495h-0.967c-63.568%200.752-140.646-7.504-215.286-21.92-6.391%2036.262-10.25%2081.838-6.936%20136.196%208.37%20137.384%2091.62%20223.736%20220.118%20224.996H506.48461c128.498-1.26%20211.748-87.612%20220.12-224.996%203.314-54.362-0.547-99.938-6.94-136.203-74.654%2014.423-151.745%2022.684-215.332%2021.927'%20fill='%23FFFFFF'%20p-id='5158'%3e%3c/path%3e%3cpath%20d='M323.27461%20577.016v137.468s64.957%2012.705%20130.031%203.91V591.59c-41.225-2.262-85.688-7.304-130.031-14.574'%20fill='%23EB1C26'%20p-id='5159'%3e%3c/path%3e%3cpath%20d='M788.09761%20432.536s-121.98%2040.387-283.743%2041.539h-0.962c-161.497-1.147-283.328-41.401-283.744-41.539l-40.854%20106.952c102.186%2032.31%20228.837%2053.135%20324.598%2051.926l0.96-0.002c95.768%201.216%20222.4-19.61%20324.6-51.924l-40.855-106.952z'%20fill='%23EB1C26'%20p-id='5160'%3e%3c/path%3e%3c/svg%3e"


# ----------------------------------------------------------------------
# Plugin entry point
# ----------------------------------------------------------------------

_FIELDS: list[dict[str, Any]] = [
    {
        "name": "connection_mode",
        "label": "连接方式",
        "type": "select",
        "placeholder": "forward",
        "required": False,
        "helpKey": "onebot11.connectionMode",
        "options": [
            {"label": "正向：Octop 连接 NapCat", "value": "forward"},
            {"label": "反向：NapCat 连接 Octop", "value": "reverse"},
        ],
    },
    {
        "name": "ws_url",
        "label": "NapCat WebSocket 地址（正向模式）",
        "type": "text",
        "placeholder": "ws://maim-bot-napcat:3001（反向模式可留空）",
        "required": False,
        "helpKey": "onebot11.wsUrl",
    },
    {
        "name": "token",
        "label": "Access Token",
        "type": "password",
        "placeholder": "正向=发送给 NapCat；反向=校验 NapCat 携带（可留空）",
        "required": False,
        "helpKey": "onebot11.token",
    },
    {
        "name": "listen_host",
        "label": "反向监听地址",
        "type": "text",
        "placeholder": "0.0.0.0（反向模式）",
        "required": False,
        "helpKey": "onebot11.listenHost",
    },
    {
        "name": "listen_port",
        "label": "反向监听端口",
        "type": "number",
        "placeholder": "3002（反向模式）",
        "required": False,
        "helpKey": "onebot11.listenPort",
    },
    {
        "name": "listen_path",
        "label": "反向监听路径",
        "type": "text",
        "placeholder": "/onebot/v11/ws（反向模式）",
        "required": False,
        "helpKey": "onebot11.listenPath",
    },
    {
        "name": "self_id",
        "label": "机器人 QQ 号",
        "type": "text",
        "placeholder": "留空则自动从 get_login_info 获取",
        "required": False,
        "helpKey": "onebot11.selfId",
    },
    {
        "name": "group_reply_mode",
        "label": "群聊回复策略",
        "type": "select",
        "placeholder": "mention",
        "required": False,
        "helpKey": "onebot11.groupReplyMode",
        "options": [
            {"label": "仅 @ 或关键词时回复", "value": "mention"},
            {"label": "回复所有消息", "value": "always"},
            {"label": "不回复群聊", "value": "off"},
        ],
    },
    {
        "name": "trigger_keywords",
        "label": "触发关键词",
        "type": "text",
        "placeholder": "逗号分隔，例如：小助手,日报,提醒",
        "required": False,
        "helpKey": "onebot11.triggerKeywords",
    },
    {
        "name": "private_reply_mode",
        "label": "私聊回复策略",
        "type": "select",
        "placeholder": "always",
        "required": False,
        "helpKey": "onebot11.privateReplyMode",
        "options": [
            {"label": "回复所有私聊", "value": "always"},
            {"label": "仅关键词触发", "value": "keyword"},
            {"label": "不回复私聊", "value": "off"},
        ],
    },
    {
        "name": "group_history_limit",
        "label": "群上下文条数",
        "type": "number",
        "placeholder": "10",
        "required": False,
        "helpKey": "onebot11.groupHistoryLimit",
    },
    {
        "name": "reply_with_quote",
        "label": "引用回复",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.replyWithQuote",
    },
    {
        "name": "allowed_groups",
        "label": "群白名单",
        "type": "text",
        "placeholder": "逗号分隔群号，留空不限制，例如：123456789,987654321",
        "required": False,
        "helpKey": "onebot11.allowedGroups",
    },
    {
        "name": "allowed_users",
        "label": "私聊白名单",
        "type": "text",
        "placeholder": "逗号分隔 QQ 号，留空不限制",
        "required": False,
        "helpKey": "onebot11.allowedUsers",
    },
    {
        "name": "outbox_enabled",
        "label": "断线排队发送",
        "type": "switch",
        "placeholder": "true",
        "required": False,
        "helpKey": "onebot11.outboxEnabled",
    },
    {
        "name": "allowed_senders",
        "label": "发送人白名单",
        "type": "text",
        "placeholder": "逗号分隔 QQ 号，留空不限制（入站）",
        "required": False,
        "helpKey": "onebot11.allowedSenders",
    },
    {
        "name": "blocked_senders",
        "label": "发送人黑名单",
        "type": "text",
        "placeholder": "逗号分隔 QQ 号，优先于白名单",
        "required": False,
        "helpKey": "onebot11.blockedSenders",
    },
    {
        "name": "poke_triggers_reply",
        "label": "戳一戳触发回复",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.pokeTriggersReply",
    },
    {
        "name": "auto_accept_friend_request",
        "label": "自动通过好友请求",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.autoAcceptFriendRequest",
    },
    {
        "name": "auto_accept_group_invite",
        "label": "自动通过群邀请",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.autoAcceptGroupInvite",
    },
    {
        "name": "monitor_enabled",
        "label": "群巡检",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.monitorEnabled",
    },
    {
        "name": "multi_account",
        "label": "多账号（反向）",
        "type": "switch",
        "placeholder": "false",
        "required": False,
        "helpKey": "onebot11.multiAccount",
    },
]


def setup(ctx: Any) -> None:
    """Register the channel kind with Octop's plugin registry."""
    ctx.channel(
        "onebot11",
        OneBot11Channel,
        label="OneBot 11 (NapCat)",
        icon=_QQ_ICON,
        intro_url="https://napcat.napneko.icu/",
        fields=_FIELDS,
    )
