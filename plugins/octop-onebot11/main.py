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
import random
import re
import time
import uuid
from collections import OrderedDict
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


def build_group_subject(group_id: str) -> "ChannelSubject":
    """Build a proactive group subject from a bare group id."""
    gid = str(group_id or "").strip()
    return ChannelSubject(
        subject_id=f"group:{gid}",
        chat_type="group",
        metadata={
            "group_id": gid,
            "chat_type": "group",
            "message_type": "group",
            "strict_push": True,
        },
    )


def build_private_subject(user_id: str) -> "ChannelSubject":
    """Build a proactive private subject from a bare QQ user id."""
    uid = str(user_id or "").strip()
    return ChannelSubject(
        subject_id=f"user:{uid}",
        chat_type="dm",
        metadata={
            "user_id": uid,
            "chat_type": "dm",
            "message_type": "private",
            "strict_push": True,
        },
    )

_CQ_RE = re.compile(r"\[CQ:([a-zA-Z0-9_]+)((?:,[^\]]*)?)\]")
_MAX_DEDUPE = 512
_MAX_MEDIA_BYTES = 10 * 1024 * 1024


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
        self._ws: Any = None
        self._task: asyncio.Task[None] | None = None
        self._probe_task: asyncio.Task[None] | None = None
        self._receiver_task: asyncio.Task[None] | None = None
        self._server: Any = None
        self._clients: set[Any] = set()
        self._stopping = False
        self._connected_transport = False
        self._login_confirmed = False
        self._self_id: str = str(config.self_id or "").strip()
        self._runtime_error: str | None = "未启动"
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._dedupe: OrderedDict[str, float] = OrderedDict()
        self._first_attempt: asyncio.Event | None = None
        self._backoff = max(0.5, float(config.reconnect_min_seconds))
        if self.channel_id != "__probe__":
            _LIVE_CHANNELS[self.channel_id] = self

        if config.group_reply_mode == "always":
            config.group_context.enabled = True
            config.group_context.activation = "always"
        elif config.group_reply_mode == "off":
            config.group_context.enabled = False

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
        return ChannelConstraints(
            reply_timeout=0.0,
            send_rate_limit=(20, 60.0),
            typing_keepalive_interval=0.0,
            show_thinking=False,
            show_tool_hints=False,
        )

    async def push_text(self, subject: ChannelSubject, text: str) -> None:
        """Proactive text send; strict mode raises on real failures.

        Octop's internal push paths (cron / proactive care / team) keep the
        base best-effort semantics.  Tool-driven sends mark the subject with
        ``strict_push=True`` so the agent gets a real error instead of a silent
        drop.
        """
        if not (subject.metadata or {}).get("strict_push"):
            return await super().push_text(subject, text)
        if not self._connected_transport:
            raise ConnectionError("OneBot 11 未连接（NapCat 未登录或 WebSocket 未就绪）")
        chunks = self._split_text(text)
        if not chunks:
            return
        await self._acquire_rate_slot()
        resolved = self.resolve_push_subject(subject)
        for chunk in chunks:
            segments = [{"type": "text", "data": {"text": chunk}}]
            action, params = self._build_send_action(resolved, segments)
            await self._call_api(action, {**params, "message": segments})

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
        """Cancel the supervisor and close the socket."""
        self._stopping = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

        for probe_task in (self._probe_task, self._receiver_task):
            if probe_task is not None and not probe_task.done():
                probe_task.cancel()
        self._probe_task = None
        self._receiver_task = None

        clients, self._clients = list(self._clients), set()
        for client in clients:
            with contextlib.suppress(Exception):
                await client.close()

        server, self._server = self._server, None
        if server is not None:
            with contextlib.suppress(Exception):
                server.close()
            wait_closed = getattr(server, "wait_closed", None)
            if wait_closed is not None:
                with contextlib.suppress(Exception):
                    await wait_closed()

        ws, self._ws = self._ws, None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

        self._fail_pending(ConnectionError("OneBot 11 channel stopped"))
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
                    self._connected_transport = False
                    self._login_confirmed = False
                    self._ws = None
                    if not self._stopping and self._runtime_error is None:
                        self._runtime_error = "连接已断开"
                    self._fail_pending(ConnectionError("OneBot 11 socket closed"))
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

        self._ws = ws
        self._connected_transport = True
        self._login_confirmed = False
        self._runtime_error = "等待 NapCat 就绪"
        self._backoff = max(0.5, float(self._config.reconnect_min_seconds))
        logger.info("OneBot11 WS connected: %s (channel_id=%s)", url, self.channel_id)

        self._receiver_task = asyncio.create_task(self._receiver(ws), name="onebot11-recv")
        self._probe_task = asyncio.create_task(self._login_probe_loop(ws), name="onebot11-login")
        try:
            await self._receiver_task
        finally:
            for task in (self._probe_task, self._receiver_task):
                if task is not None and not task.done():
                    task.cancel()
            self._probe_task = None
            self._receiver_task = None
            self._connected_transport = False
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

        await self._attach_client(ws, remote)
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
                    await self._dispatch_payload(payload)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed:
            logger.info("OneBot11 reverse WS closed by peer: %s", remote)
        except Exception as exc:  # noqa: BLE001
            self._runtime_error = f"receive_failed: {exc}"
            logger.exception("OneBot11 reverse receive loop failed")
        finally:
            await self._detach_client(ws, remote)

    async def _attach_client(self, ws: Any, remote: str) -> None:
        previous = self._ws
        self._clients.add(ws)
        self._ws = ws
        self._connected_transport = True
        self._login_confirmed = False
        self._backoff = max(0.5, float(self._config.reconnect_min_seconds))
        self._runtime_error = "等待 NapCat 就绪"
        logger.info(
            "OneBot11 reverse WS client connected: %s (clients=%d, channel_id=%s)",
            remote,
            len(self._clients),
            self.channel_id,
        )
        if self._probe_task is not None and not self._probe_task.done():
            self._probe_task.cancel()
        self._probe_task = asyncio.create_task(
            self._login_probe_loop(ws),
            name="onebot11-login",
        )
        if previous is not None and previous is not ws:
            # One QQ account normally opens a single reverse connection.  A new
            # one usually means NapCat reconnected before the old socket timed
            # out, so keep the newest and retire the stale one.
            logger.info("OneBot11 reverse WS replacing previous client: %s", remote)
            with contextlib.suppress(Exception):
                await previous.close(code=1000, reason="replaced by a newer connection")

    async def _detach_client(self, ws: Any, remote: str) -> None:
        self._clients.discard(ws)
        if self._ws is not ws:
            logger.info("OneBot11 reverse WS stale client closed: %s", remote)
            return
        self._ws = next(iter(self._clients), None)
        if self._ws is not None:
            logger.info("OneBot11 reverse WS primary switched after %s", remote)
            return
        if self._probe_task is not None and not self._probe_task.done():
            self._probe_task.cancel()
        self._probe_task = None
        self._connected_transport = False
        self._login_confirmed = False
        self._fail_pending(ConnectionError("OneBot 11 reverse socket closed"))
        if not self._stopping:
            self._runtime_error = "连接已断开，等待 NapCat 重连"
        logger.info("OneBot11 reverse WS client disconnected: %s", remote)

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

    async def _receiver(self, ws: Any) -> None:
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
                    await self._dispatch_payload(payload)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed:
            logger.info("OneBot11 WS closed by peer")
        except Exception as exc:  # noqa: BLE001
            self._runtime_error = f"receive_failed: {exc}"
            logger.exception("OneBot11 receive loop failed")

    async def _dispatch_payload(self, payload: dict[str, Any]) -> None:
        echo = payload.get("echo")
        if echo is not None:
            key = str(echo)
            future = self._pending.pop(key, None)
            if future is not None and not future.done():
                future.set_result(payload)
            return

        post_type = payload.get("post_type")
        if post_type == "meta_event":
            self._handle_meta_event(payload)
            return
        if post_type == "message":
            self._handle_message_event(payload)
            return
        if post_type in ("notice", "request"):
            logger.debug("OneBot11 %s event: %s", post_type, payload.get("notice_type") or payload.get("request_type"))
            return
        logger.debug("OneBot11 unhandled event: %s", post_type)

    def _handle_meta_event(self, payload: dict[str, Any]) -> None:
        meta_type = payload.get("meta_event_type")
        self_id = payload.get("self_id")
        if self_id and not self._self_id:
            self._self_id = str(self_id)
        if meta_type == "heartbeat":
            status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
            online = status.get("online")
            if online is True and self._self_id:
                self._login_confirmed = True
                self._runtime_error = None
            elif online is False:
                self._login_confirmed = False
                self._self_id = ""
                self._runtime_error = "NapCat 未登录，等待登录"
        elif meta_type == "lifecycle":
            sub_type = payload.get("sub_type")
            if sub_type in ("connect", "enable"):
                self._runtime_error = None if self._login_confirmed else "等待 NapCat 就绪"

    def _handle_message_event(self, payload: dict[str, Any]) -> None:
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
        if self_id and not self._self_id:
            self._self_id = self_id
        if self._config.ignore_self_message and self_id and user_id == self_id:
            return

        message_id = str(payload.get("message_id") or "")
        if message_id:
            now = time.monotonic()
            if message_id in self._dedupe:
                return
            self._dedupe[message_id] = now
            while len(self._dedupe) > _MAX_DEDUPE:
                self._dedupe.popitem(last=False)

        self._runtime_error = None if self._self_id else self._runtime_error
        if self._self_id:
            self._login_confirmed = True
        self.enqueue(payload)

    async def _login_probe_loop(self, ws: Any) -> None:
        """Detect NapCat login state; do not require it for channel startup."""
        while not self._stopping and self._ws is ws:
            if not self._login_confirmed:
                try:
                    info = await self._call_api("get_login_info", {}, timeout=5.0)
                    uid = str((info or {}).get("user_id") or "")
                    if uid and uid != "0":
                        self._self_id = uid
                        self._login_confirmed = True
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
        if not self._connected_transport:
            logger.warning("OneBot11 text send skipped: WebSocket is not connected")
            return
        if not text.strip():
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
        action, params = self._build_send_action(subject, segments)
        quote_id = str(subject.metadata.get("message_id") or "") if allow_quote else ""
        if self._config.reply_with_quote and quote_id:
            quoted = [{"type": "reply", "data": {"id": quote_id}}, *segments]
            try:
                await self._call_api(action, {**params, "message": quoted})
                return
            except OneBotAPIError as exc:
                logger.info("OneBot11 quote reply failed, retrying without quote: %s", exc)
        try:
            await self._call_api(action, {**params, "message": segments})
        except OneBotAPIError as exc:
            logger.warning("OneBot11 send failed: %s", exc)
        except (ConnectionClosed, OSError, TimeoutError, asyncio.TimeoutError) as exc:
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

    async def _call_api(self, action: str, params: dict[str, Any], *, timeout: float | None = None) -> Any:
        ws = self._ws
        if ws is None or not self._connected_transport:
            raise ConnectionError("OneBot 11 WebSocket is not connected")
        echo = f"octop-{uuid.uuid4().hex}"
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[echo] = future
        try:
            await ws.send(json.dumps({"action": action, "params": params, "echo": echo}, ensure_ascii=False))
            response = await asyncio.wait_for(
                future,
                timeout=float(timeout or self._config.request_timeout_seconds),
            )
        except (TimeoutError, asyncio.TimeoutError):
            self._pending.pop(echo, None)
            raise TimeoutError(f"OneBot action timed out: {action}") from None
        except Exception:
            self._pending.pop(echo, None)
            raise

        status = str(response.get("status") or "")
        retcode = response.get("retcode", 0)
        if status == "failed" or (retcode not in (0, None, "0")):
            message = str(response.get("wording") or response.get("message") or "unknown error")
            raise OneBotAPIError(action, retcode, message)
        return response.get("data")

    def _fail_pending(self, exc: BaseException) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

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

        content, bot_mentioned, reply_to = self._parse_message_content(event.get("message"), self_id)
        keyword_triggered = False
        trigger_reason = "at" if bot_mentioned else ""
        if not bot_mentioned and self._keywords:
            plain = "\n".join(part.text for part in content if isinstance(part, TextContent) and part.text)
            hit = self._keyword_hit(plain)
            if hit:
                bot_mentioned = True
                keyword_triggered = True
                trigger_reason = f"keyword:{hit}"
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
    ) -> tuple[list[ContentPart], bool, str]:
        parts: list[ContentPart] = []
        bot_mentioned = False
        reply_to = ""

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

        return parts, bot_mentioned, reply_to

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
]


def setup(ctx: Any) -> None:
    """Register the channel kind with Octop's plugin registry."""
    ctx.channel(
        "onebot11",
        OneBot11Channel,
        label="OneBot 11 (NapCat)",
        intro_url="https://napcat.napneko.icu/",
        fields=_FIELDS,
    )
